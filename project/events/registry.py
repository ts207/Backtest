from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import numpy as np
import pandas as pd
import yaml

from pipelines._lib.io_utils import (
    choose_partition_dir,
    ensure_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
    write_parquet,
)
from pipelines._lib.validation import ts_ns_utc
from pipelines._lib.sanity import assert_monotonic_utc_timestamp

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))


@dataclass(frozen=True)
class EventRegistrySpec:
    event_type: str
    reports_dir: str
    events_file: str
    signal_column: str


def _load_event_specs() -> Dict[str, EventRegistrySpec]:
    spec_dir = PROJECT_ROOT.parent / "spec" / "events"
    if not spec_dir.exists():
        return {}

    import logging as _logging
    _log = _logging.getLogger(__name__)
    required = {"event_type", "reports_dir", "events_file", "signal_column"}
    specs = {}
    for yaml_file in sorted(spec_dir.glob("*.yaml")):
        with open(yaml_file, "r") as f:
            data = yaml.safe_load(f)
        if not data:
            continue
        if bool(data.get("deprecated", False)) or not bool(data.get("active", True)):
            continue
        if data.get("kind") == "canonical_event_registry":
            continue
        missing = required - set(data.keys())
        if missing:
            raise ValueError(f"Malformed spec {yaml_file.name} — missing registry fields: {missing}")
        spec = EventRegistrySpec(
            event_type=data["event_type"],
            reports_dir=data["reports_dir"],
            events_file=data["events_file"],
            signal_column=data["signal_column"],
        )
        specs[spec.event_type] = spec
    return specs


EVENT_REGISTRY_SPECS: Dict[str, EventRegistrySpec] = _load_event_specs()

SIGNAL_TO_EVENT_TYPE: Dict[str, str] = {
    spec.signal_column: event_type for event_type, spec in EVENT_REGISTRY_SPECS.items()
}
REGISTRY_BACKED_SIGNALS = set(SIGNAL_TO_EVENT_TYPE.keys())

REGISTRY_EVENT_COLUMNS = [
    "run_id",
    "event_type",
    "signal_column",
    "timestamp",
    "phenom_enter_ts",
    "enter_ts",
    "detected_ts",
    "signal_ts",
    "exit_ts",
    "symbol",
    "event_id",
    "direction",
    "sign",
    "split_label",
    "features_at_event",
]


def registry_contract_check(events: pd.DataFrame, flags: pd.DataFrame, symbol: str) -> None:
    """
    Assert event registry integrity: timestamps, grid alignment, and signal consistency.
    """
    if events.empty:
        return
    
    # Check signal_ts >= enter_ts
    ev = events[events["symbol"] == symbol]
    if not ev.empty:
        invalid = ev[ev["signal_ts"] < ev["enter_ts"]]
        if not invalid.empty:
            raise ValueError(f"Events for {symbol} have signal_ts < enter_ts")
            
    # Check flags alignment to feature grid
    if not flags.empty:
        f_sym = flags[flags["symbol"] == symbol]
        if not f_sym.empty:
            ts = ts_ns_utc(f_sym["timestamp"])
            if len(ts) > 1:
                diffs = ts.diff().dropna()
                expected_ns = 300 * 10**9
                if not (diffs.view("int64") == expected_ns).all():
                    if not (diffs.view("int64")[1:] == expected_ns).all():
                        raise ValueError(f"Event flags for {symbol} do not follow 5m grid")

# Some phase-1 analyzers emit multiple canonical event types into one shared CSV.
# Filtering remains event-type exact unless an explicit canonical union is declared.
AGGREGATE_EVENT_TYPE_UNIONS: Dict[str, Sequence[str]] = {}


def expected_event_types_for_spec(event_type: str) -> Sequence[str]:
    normalized = str(event_type).strip()
    if not normalized:
        return ()
    return AGGREGATE_EVENT_TYPE_UNIONS.get(normalized, (normalized,))


def filter_phase1_rows_for_event_type(events: pd.DataFrame, event_type: str) -> pd.DataFrame:
    if events.empty or "event_type" not in events.columns:
        return events
    allowed = set(expected_event_types_for_spec(event_type))
    if not allowed:
        return events.iloc[0:0].copy()
    return events[events["event_type"].astype(str).isin(allowed)].copy()


def _empty_registry_events() -> pd.DataFrame:
    return pd.DataFrame(columns=REGISTRY_EVENT_COLUMNS)


def _active_signal_column(signal_column: str) -> str:
    signal = str(signal_column).strip()
    if signal.endswith("_event"):
        return f"{signal[:-6]}_active"
    return f"{signal}_active"


def _signal_ts_column(signal_column: str) -> str:
    signal = str(signal_column).strip()
    if signal.endswith("_event"):
        return f"{signal[:-6]}_signal"
    return f"{signal}_signal"


def _registry_root(data_root: Path, run_id: str) -> Path:
    return Path(data_root) / "events" / str(run_id)


def _registry_file(root: Path, stem: str) -> Path:
    return root / f"{stem}.parquet"


def _first_existing_column(df: pd.DataFrame, names: Sequence[str]) -> str | None:
    for name in names:
        if name in df.columns:
            return name
    return None


def _feature_payload(row: pd.Series) -> str:
    keys = [
        "adverse_proxy_excess",
        "opportunity_value_excess",
        "forward_abs_return_h",
        "quote_volume",
        "spread_bps",
        "funding_rate_scaled",
        "range_pct_96",
        "rv_decay_half_life",
        "time_to_secondary_shock",
        "severity",
        "stress_score",
        "depth_drop_pct",
        "shock_return",
        "auc_excess_range",
    ]
    payload = {}
    for key in keys:
        if key in row.index:
            value = row.get(key)
            if pd.notna(value):
                try:
                    payload[key] = float(value)
                except (TypeError, ValueError):
                    payload[key] = str(value)
    return json.dumps(payload, sort_keys=True)


def normalize_phase1_events(events: pd.DataFrame, spec: EventRegistrySpec, run_id: str) -> pd.DataFrame:
    if events.empty:
        return _empty_registry_events()

    out = filter_phase1_rows_for_event_type(events.copy(), spec.event_type)
    if out.empty:
        return _empty_registry_events()
    
    timestamp_col = _first_existing_column(out, ["enter_ts", "anchor_ts", "timestamp", "event_ts", "start_ts"])
    if timestamp_col is None:
        return _empty_registry_events()

    out["enter_ts"] = ts_ns_utc(out[timestamp_col])
    
    exit_col = _first_existing_column(out, ["exit_ts", "end_ts", "event_end_ts", "relax_ts", "norm_ts", "end_time", "exit_time"])
    if exit_col is not None:
        out["exit_ts"] = ts_ns_utc(out[exit_col])
    else:
        out["exit_ts"] = out["enter_ts"]

    det_col = _first_existing_column(out, ["detected_ts", "detection_ts", "signal_ts", "trigger_ts"])
    if det_col is not None:
        out["detected_ts"] = ts_ns_utc(out[det_col])
    else:
        out["detected_ts"] = out["enter_ts"]

    # signal_ts defaults to detected_ts, used for impulse snapping
    out["signal_ts"] = out["detected_ts"]

    # phenom_enter_ts preserves the original phenomenological entry
    out["phenom_enter_ts"] = out["enter_ts"]

    out["timestamp"] = out["signal_ts"]
    out = out.dropna(subset=["timestamp", "enter_ts", "detected_ts", "signal_ts"]).copy()
    if out.empty:
        return _empty_registry_events()
    out["exit_ts"] = out["exit_ts"].where(out["exit_ts"].notna(), out["enter_ts"])
    out["exit_ts"] = out["exit_ts"].where(out["exit_ts"] >= out["enter_ts"], out["enter_ts"])

    if "symbol" not in out.columns:
        out["symbol"] = "ALL"
    out["symbol"] = out["symbol"].fillna("ALL").astype(str).str.upper().replace("", "ALL")

    if "event_id" not in out.columns:
        if "parent_event_id" in out.columns:
            out["event_id"] = out["parent_event_id"].astype(str)
        else:
            out["event_id"] = [f"{spec.event_type}_{idx:08d}" for idx in range(len(out))]
    out["event_id"] = out["event_id"].fillna("").astype(str)
    missing_ids = out["event_id"].str.len() == 0
    if missing_ids.any():
        out.loc[missing_ids, "event_id"] = [f"{spec.event_type}_{idx:08d}" for idx in range(int(missing_ids.sum()))]

    # Preserve direction/sign/split_label if present; enforce stable dtypes
    for col in ["direction", "sign"]:
        if col not in out.columns:
            out[col] = np.nan
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")
    if "split_label" not in out.columns:
        out["split_label"] = np.nan

    out = out.sort_values(["timestamp", "symbol", "event_id"]).reset_index(drop=True)
    out["features_at_event"] = out.apply(_feature_payload, axis=1)

    result = pd.DataFrame(
        {
            "run_id": str(run_id),
            "event_type": spec.event_type,
            "signal_column": spec.signal_column,
            "timestamp": out["timestamp"],
            "phenom_enter_ts": out["phenom_enter_ts"],
            "enter_ts": out["enter_ts"],
            "detected_ts": out["detected_ts"],
            "signal_ts": out["signal_ts"],
            "exit_ts": out["exit_ts"],
            "symbol": out["symbol"],
            "event_id": out["event_id"],
            "direction": out["direction"],
            "sign": out["sign"],
            "split_label": out["split_label"],
            "features_at_event": out["features_at_event"],
        }
    )
    result = result.drop_duplicates(subset=["event_type", "timestamp", "symbol", "event_id"]).reset_index(drop=True)
    return result


def _read_phase1_events(data_root: Path, run_id: str, spec: EventRegistrySpec) -> pd.DataFrame:
    """
    Read phase-1 event artifacts for a given spec.

    Contract:
      - Machine artifacts are preferred as Parquet.
      - CSV is allowed as a human-facing export / legacy format.
      - If spec.events_file is missing, try the alternate suffix (.csv <-> .parquet)
        to tolerate partial migrations.
    """
    primary = Path(data_root) / "reports" / spec.reports_dir / str(run_id) / spec.events_file
    candidates: List[Path] = [primary]

    # Tolerate spec/analyzer suffix mismatches during migrations.
    if primary.suffix.lower() == ".csv":
        candidates.append(primary.with_suffix(".parquet"))
    elif primary.suffix.lower() == ".parquet":
        candidates.append(primary.with_suffix(".csv"))

    for path in candidates:
        if not path.exists():
            continue
        try:
            if path.suffix.lower() == ".parquet":
                return pd.read_parquet(path)
            # CSV path: try CSV first, but fall back to Parquet if the content is misnamed.
            try:
                return pd.read_csv(path)
            except Exception:
                return pd.read_parquet(path)
        except Exception:
            continue

    return pd.DataFrame()


def collect_registry_events(data_root: Path, run_id: str, event_types: Iterable[str] | None = None) -> pd.DataFrame:
    selected = list(event_types) if event_types is not None else sorted(EVENT_REGISTRY_SPECS.keys())
    rows: List[pd.DataFrame] = []
    for event_type in selected:
        spec = EVENT_REGISTRY_SPECS.get(str(event_type))
        if spec is None:
            continue
        events = _read_phase1_events(data_root=data_root, run_id=run_id, spec=spec)
        normalized = normalize_phase1_events(events=events, spec=spec, run_id=run_id)
        if not normalized.empty:
            rows.append(normalized)

    if not rows:
        return _empty_registry_events()
    out = pd.concat(rows, ignore_index=True)
    out = out.sort_values(["timestamp", "symbol", "event_type", "event_id"]).reset_index(drop=True)
    return out[REGISTRY_EVENT_COLUMNS]


def _normalize_registry_events_frame(events: pd.DataFrame) -> pd.DataFrame:
    if events is None or events.empty:
        return _empty_registry_events()

    out = events.copy()
    for column in REGISTRY_EVENT_COLUMNS:
        if column not in out.columns:
            out[column] = None
    out = out[REGISTRY_EVENT_COLUMNS].copy()
    out["run_id"] = out["run_id"].fillna("").astype(str)
    out["event_type"] = out["event_type"].fillna("").astype(str)
    out["signal_column"] = out["signal_column"].fillna("").astype(str)
    
    out["timestamp"] = ts_ns_utc(out["timestamp"])
    out["enter_ts"] = ts_ns_utc(out["enter_ts"])
    out["phenom_enter_ts"] = ts_ns_utc(out.get("phenom_enter_ts", out["enter_ts"]))
    out["detected_ts"] = ts_ns_utc(out.get("detected_ts", out["enter_ts"]))
    out["signal_ts"] = ts_ns_utc(out.get("signal_ts", out["detected_ts"]))
    out["exit_ts"] = ts_ns_utc(out["exit_ts"])

    out["enter_ts"] = out["enter_ts"].where(out["enter_ts"].notna(), out["timestamp"])
    out["exit_ts"] = out["exit_ts"].where(out["exit_ts"].notna(), out["enter_ts"])
    out["exit_ts"] = out["exit_ts"].where(out["exit_ts"] >= out["enter_ts"], out["enter_ts"])
    out = out.dropna(subset=["timestamp"]).copy()
    out["symbol"] = out["symbol"].fillna("ALL").astype(str).str.upper().replace("", "ALL")
    out["event_id"] = out["event_id"].fillna("").astype(str)
    out["features_at_event"] = out["features_at_event"].fillna("{}").astype(str)
    for col in ("direction", "sign"):
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")
    out = out.drop_duplicates(subset=["event_type", "timestamp", "symbol", "event_id"]).copy()
    out = out.sort_values(["timestamp", "symbol", "event_type", "event_id"]).reset_index(drop=True)
    return out[REGISTRY_EVENT_COLUMNS]


def merge_registry_events(
    *,
    existing: pd.DataFrame,
    incoming: pd.DataFrame,
    selected_event_types: Iterable[str] | None,
) -> pd.DataFrame:
    selected = {str(event_type).strip() for event_type in (selected_event_types or []) if str(event_type).strip()}
    existing_norm = _normalize_registry_events_frame(existing)
    incoming_norm = _normalize_registry_events_frame(incoming)
    if selected:
        existing_kept = existing_norm[~existing_norm["event_type"].isin(selected)].copy()
        incoming_replacement = incoming_norm[incoming_norm["event_type"].isin(selected)].copy()
    else:
        existing_kept = _empty_registry_events()
        incoming_replacement = incoming_norm
    merged = pd.concat([existing_kept, incoming_replacement], ignore_index=True)
    return _normalize_registry_events_frame(merged)


def _load_symbol_timestamps(data_root: Path, run_id: str, symbol: str, timeframe: str = "5m") -> pd.Series:
    candidates = [
        run_scoped_lake_path(data_root, run_id, "features", "perp", symbol, timeframe, "features_v1"),
        Path(data_root) / "lake" / "features" / "perp" / symbol / timeframe / "features_v1",
    ]
    src = choose_partition_dir(candidates)
    files = list_parquet_files(src) if src else []
    if not files:
        return pd.Series(dtype="datetime64[ns, UTC]")
    frame = read_parquet(files)
    if frame.empty or "timestamp" not in frame.columns:
        return pd.Series(dtype="datetime64[ns, UTC]")
    ts = ts_ns_utc(frame["timestamp"])
    if ts.empty:
        return pd.Series(dtype="datetime64[ns, UTC]")
    return pd.Series(sorted(pd.DatetimeIndex(ts).unique()))


def build_event_flags(
    *,
    events: pd.DataFrame,
    symbols: Sequence[str],
    data_root: Path,
    run_id: str,
    timeframe: str = "5m",
) -> pd.DataFrame:
    symbols_clean = [str(s).strip().upper() for s in symbols if str(s).strip()]
    symbols_clean = list(dict.fromkeys(symbols_clean))
    if not symbols_clean and not events.empty:
        symbols_clean = sorted(set(events["symbol"].dropna().astype(str).str.upper().tolist()) - {"ALL"})

    # 1. Load grids per symbol
    grids = []
    for symbol in symbols_clean:
        ts = _load_symbol_timestamps(data_root=data_root, run_id=run_id, symbol=symbol, timeframe=timeframe)
        if ts.empty:
            event_ts = pd.Series(dtype="datetime64[ns, UTC]")
            if not events.empty:
                local = events[(events["symbol"] == symbol) | (events["symbol"] == "ALL")]
                event_ts = pd.to_datetime(local.get("timestamp", pd.Series(dtype=object)), utc=True, errors="coerce").dropna()
            ts = pd.Series(sorted(pd.DatetimeIndex(event_ts).unique())) if not event_ts.empty else ts
        
        if not ts.empty:
            grids.append(pd.DataFrame({"timestamp": ts_ns_utc(ts), "symbol": symbol}))

    all_signals = []
    for sig in sorted(REGISTRY_BACKED_SIGNALS):
        all_signals.append(sig)
        all_signals.append(_active_signal_column(sig))
        all_signals.append(_signal_ts_column(sig))

    if not grids:
        return pd.DataFrame(columns=["timestamp", "symbol"] + all_signals)

    full_grid = pd.concat(grids, ignore_index=True)
    
    # Fast initialization of signal columns
    zero_data = np.zeros((len(full_grid), len(all_signals)), dtype=bool)
    flags_part = pd.DataFrame(zero_data, columns=all_signals, index=full_grid.index)
    full_grid = pd.concat([full_grid, flags_part], axis=1)

    if events.empty:
        return full_grid

    # 2. Prepare events
    ev = events.copy()
    ev["timestamp"] = ts_ns_utc(ev["timestamp"])
    ev["enter_ts"] = ts_ns_utc(ev["enter_ts"])
    ev["exit_ts"] = ts_ns_utc(ev["exit_ts"])
    ev["signal_ts"] = ts_ns_utc(ev.get("signal_ts", ev["timestamp"]))
    # detected_ts: the bar where condition first confirmed; fall back to enter_ts
    if "detected_ts" in ev.columns:
        ev["_detected_ts"] = ts_ns_utc(ev["detected_ts"])
    else:
        ev["_detected_ts"] = ev["enter_ts"]
    
    ev = ev[ev["signal_column"].isin(REGISTRY_BACKED_SIGNALS)].copy()
    if ev.empty:
        return full_grid

    # 3. Handle "ALL" broadcast
    all_events = ev[ev["symbol"] == "ALL"]
    specific_events = ev[ev["symbol"] != "ALL"]
    
    expanded_rows = []
    if not all_events.empty:
        for symbol in symbols_clean:
            temp = all_events.copy()
            temp["symbol"] = symbol
            expanded_rows.append(temp)
    
    if expanded_rows:
        ev = pd.concat([specific_events] + expanded_rows, ignore_index=True)
    else:
        ev = specific_events

    # 4. Vectorized Snapping per symbol
    for symbol, group in full_grid.groupby("symbol"):
        sym_events = ev[ev["symbol"] == symbol]
        if sym_events.empty:
            continue
            
        grid_ts_naive = group["timestamp"].dt.tz_localize(None).values
        grid_indices = group.index
        
        # Impulse Snapping (signal_ts)
        sig_ts_naive = sym_events["signal_ts"].dt.tz_localize(None).values
        idx = np.searchsorted(grid_ts_naive, sig_ts_naive)
        
        valid_mask = idx < len(grid_ts_naive)
        if valid_mask.any():
            valid_idx = idx[valid_mask]
            target_indices = grid_indices[valid_idx]
            target_signals = sym_events["signal_column"].values[valid_mask]
            
            for sig_col in np.unique(target_signals):
                hits = target_indices[target_signals == sig_col]
                full_grid.loc[hits, sig_col] = True

        # Range Snapping (active flags)
        enter_ts_naive = sym_events["enter_ts"].dt.tz_localize(None).values
        exit_ts_naive = sym_events["exit_ts"].dt.tz_localize(None).values
        
        idx_enter = np.searchsorted(grid_ts_naive, enter_ts_naive)
        idx_exit = np.searchsorted(grid_ts_naive, exit_ts_naive, side="right") - 1
        
        for sig_col in np.unique(sym_events["signal_column"]):
            active_col = _active_signal_column(sig_col)
            sig_mask = sym_events["signal_column"] == sig_col
            
            diff = np.zeros(len(grid_ts_naive) + 1, dtype=int)
            enters = idx_enter[sig_mask]
            exits = idx_exit[sig_mask]
            
            valid_ranges = (enters < len(grid_ts_naive)) & (exits >= 0) & (enters <= exits)
            if not valid_ranges.any():
                continue
                
            enters_v = np.maximum(0, enters[valid_ranges])
            exits_v = np.minimum(len(grid_ts_naive) - 1, exits[valid_ranges])
            
            np.add.at(diff, enters_v, 1)
            np.add.at(diff, exits_v + 1, -1)
            
            active_mask = (np.cumsum(diff)[:-1] > 0)
            full_grid.loc[grid_indices[active_mask], active_col] = True

        # _signal flag: first bar strictly after detected_ts (PIT-safe tradable impulse)
        detected_naive = sym_events["_detected_ts"].dt.tz_localize(None).values
        # searchsorted with side="right" gives first index where grid > detected_ts
        sig_idx = np.searchsorted(grid_ts_naive, detected_naive, side="right")

        valid_sig = sig_idx < len(grid_ts_naive)
        if valid_sig.any():
            valid_positions = sig_idx[valid_sig]
            target_grid_indices = grid_indices[valid_positions]
            target_sig_cols = sym_events["signal_column"].values[valid_sig]

            for sig_col in np.unique(target_sig_cols):
                tradable_col = _signal_ts_column(sig_col)
                hits = target_grid_indices[target_sig_cols == sig_col]
                full_grid.loc[hits, tradable_col] = True

    return full_grid.sort_values(["timestamp", "symbol"]).reset_index(drop=True)


def write_event_registry_artifacts(data_root: Path, run_id: str, events: pd.DataFrame, event_flags: pd.DataFrame) -> Dict[str, str]:
    root = _registry_root(data_root=data_root, run_id=run_id)
    ensure_dir(root)

    events_path, _ = write_parquet(events, _registry_file(root, "events"))
    flags_path, _ = write_parquet(event_flags, _registry_file(root, "event_flags"))
    return {
        "events_path": str(events_path),
        "event_flags_path": str(flags_path),
        "registry_root": str(root),
    }


def _read_registry_stem(data_root: Path, run_id: str, stem: str) -> pd.DataFrame:
    root = _registry_root(data_root=data_root, run_id=run_id)
    parquet_path = root / f"{stem}.parquet"
    csv_path = root / f"{stem}.csv"

    if parquet_path.exists():
        return read_parquet([parquet_path])
    if csv_path.exists():
        return pd.read_csv(csv_path)
    return pd.DataFrame()


def load_registry_events(
    *,
    data_root: Path,
    run_id: str,
    event_type: str | None = None,
    symbols: Sequence[str] | None = None,
) -> pd.DataFrame:
    events = _read_registry_stem(data_root=data_root, run_id=run_id, stem="events")
    if not events.empty:
        events["enter_ts"] = pd.to_datetime(events["enter_ts"], unit="ms", utc=True, errors="coerce")
        events["exit_ts"] = pd.to_datetime(events["exit_ts"], unit="ms", utc=True, errors="coerce")
        for col in ("phenom_enter_ts", "detected_ts", "signal_ts"):
            if col in events.columns:
                events[col] = pd.to_datetime(events[col], unit="ms", utc=True, errors="coerce")
    events = _normalize_registry_events_frame(events)
    if events.empty:
        return _empty_registry_events()
    if event_type is not None:
        events = events[events["event_type"].astype(str) == str(event_type)].copy()
    if symbols is not None:
        symbol_set = {str(s).strip().upper() for s in symbols if str(s).strip()}
        if symbol_set:
            events = events[events["symbol"].astype(str).str.upper().isin(symbol_set)].copy()
    return events.sort_values(["timestamp", "symbol", "event_type", "event_id"]).reset_index(drop=True)


def load_registry_flags(data_root: Path, run_id: str, symbol: str | None = None) -> pd.DataFrame:
    flags = _read_registry_stem(data_root=data_root, run_id=run_id, stem="event_flags")
    if flags.empty:
        cols = ["timestamp", "symbol"]
        for signal in sorted(REGISTRY_BACKED_SIGNALS):
            cols.extend([signal, _active_signal_column(signal), _signal_ts_column(signal)])
        return pd.DataFrame(columns=cols)

    flags["timestamp"] = pd.to_datetime(flags.get("timestamp"), utc=True, errors="coerce")
    flags = flags.dropna(subset=["timestamp"]).copy()
    if symbol is not None:
        symbol_norm = str(symbol).strip().upper()
        flags = flags[flags["symbol"].astype(str).str.upper() == symbol_norm].copy()

    # Build column set and ensure all exist
    cols = ["timestamp", "symbol"]
    for signal in sorted(REGISTRY_BACKED_SIGNALS):
        cols.extend([signal, _active_signal_column(signal), _signal_ts_column(signal)])

    missing = [c for c in cols if c not in flags.columns]
    if missing:
        fill = pd.DataFrame({c: False for c in missing}, index=flags.index)
        flags = pd.concat([flags, fill], axis=1)
    flags = flags[cols].copy()  # defragment

    for signal in sorted(REGISTRY_BACKED_SIGNALS):
        flags[signal] = flags[signal].fillna(False).astype(bool)
        flags[_active_signal_column(signal)] = flags[_active_signal_column(signal)].fillna(False).astype(bool)
        flags[_signal_ts_column(signal)] = flags[_signal_ts_column(signal)].fillna(False).astype(bool)

    return flags.sort_values(["timestamp", "symbol"]).reset_index(drop=True)


def merge_event_flags_for_selected_event_types(
    *,
    existing_flags: pd.DataFrame,
    recomputed_flags: pd.DataFrame,
    selected_event_types: Sequence[str],
) -> pd.DataFrame:
    """
    Merge fresh flag computation for selected event types into an existing flag frame.

    This is used by per-event registry updates to avoid rebuilding every signal column
    from all historical events on each stage.
    """
    selected = [str(event_type).strip() for event_type in selected_event_types if str(event_type).strip()]
    selected_signal_cols: List[str] = []
    for event_type in selected:
        spec = EVENT_REGISTRY_SPECS.get(event_type)
        if spec is None:
            continue
        selected_signal_cols.append(spec.signal_column)
        selected_signal_cols.append(_active_signal_column(spec.signal_column))
        selected_signal_cols.append(_signal_ts_column(spec.signal_column))
    selected_signal_cols = list(dict.fromkeys(selected_signal_cols))

    keys = ["timestamp", "symbol"]
    left = existing_flags.copy() if existing_flags is not None else pd.DataFrame(columns=keys)
    right = recomputed_flags.copy() if recomputed_flags is not None else pd.DataFrame(columns=keys)

    if "timestamp" in left.columns:
        left["timestamp"] = pd.to_datetime(left["timestamp"], utc=True, errors="coerce")
    if "timestamp" in right.columns:
        right["timestamp"] = pd.to_datetime(right["timestamp"], utc=True, errors="coerce")
    left = left.dropna(subset=["timestamp"]) if "timestamp" in left.columns else pd.DataFrame(columns=keys)
    right = right.dropna(subset=["timestamp"]) if "timestamp" in right.columns else pd.DataFrame(columns=keys)

    if left.empty:
        merged = right.copy()
    else:
        if right.empty:
            merged = left.copy()
        else:
            keep_right_cols = [c for c in [*keys, *selected_signal_cols] if c in right.columns]
            merged = left.merge(right[keep_right_cols], on=keys, how="outer", suffixes=("", "__recomputed"))
            for col in selected_signal_cols:
                new_col = f"{col}__recomputed"
                if new_col in merged.columns:
                    merged[col] = merged[new_col]
                    merged.drop(columns=[new_col], inplace=True)

    if "symbol" not in merged.columns:
        merged["symbol"] = "ALL"
    merged["symbol"] = merged["symbol"].fillna("").astype(str).str.upper()
    merged = merged[merged["symbol"].str.len() > 0].copy()

    # Build all signal columns at once to avoid DataFrame fragmentation
    out_cols = ["timestamp", "symbol"]
    for signal in sorted(REGISTRY_BACKED_SIGNALS):
        out_cols.extend([signal, _active_signal_column(signal), _signal_ts_column(signal)])

    missing = [c for c in out_cols if c not in merged.columns]
    if missing:
        fill = pd.DataFrame({c: False for c in missing}, index=merged.index)
        merged = pd.concat([merged, fill], axis=1)
    merged = merged[out_cols].copy()  # defragment

    for signal in sorted(REGISTRY_BACKED_SIGNALS):
        merged[signal] = merged[signal].fillna(False).astype(bool)
        merged[_active_signal_column(signal)] = merged[_active_signal_column(signal)].fillna(False).astype(bool)
        merged[_signal_ts_column(signal)] = merged[_signal_ts_column(signal)].fillna(False).astype(bool)

    return merged.sort_values(["timestamp", "symbol"]).reset_index(drop=True)


def build_event_feature_frame(
    data_root: Path,
    run_id: str,
    symbol: str,
) -> pd.DataFrame:
    """
    Pivot registry event features into a sparse time-series frame for DSL strategies.
    Unpacks JSON 'features_at_event' and prefixes them with event_type.
    """
    events = load_registry_events(data_root=data_root, run_id=run_id, symbols=[symbol])
    if events.empty:
        return pd.DataFrame()

    rows = []
    for _, row in events.iterrows():
        try:
            payload = json.loads(row["features_at_event"])
        except (ValueError, TypeError):
            payload = {}

        prefix = str(row["event_type"]).lower()
        flattened = {"timestamp": row["timestamp"]}
        for k, v in payload.items():
            flattened[f"{prefix}_{k}"] = v
        rows.append(flattened)

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last")
    return df
