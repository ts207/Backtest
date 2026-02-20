from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import pandas as pd

from pipelines._lib.io_utils import (
    choose_partition_dir,
    ensure_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
    write_parquet,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))


@dataclass(frozen=True)
class EventRegistrySpec:
    event_type: str
    reports_dir: str
    events_file: str
    signal_column: str


EVENT_REGISTRY_SPECS: Dict[str, EventRegistrySpec] = {
    "vol_shock_relaxation": EventRegistrySpec(
        event_type="vol_shock_relaxation",
        reports_dir="vol_shock_relaxation",
        events_file="vol_shock_relaxation_events.csv",
        signal_column="vol_shock_relaxation_event",
    ),
    "liquidity_refill_lag_window": EventRegistrySpec(
        event_type="liquidity_refill_lag_window",
        reports_dir="liquidity_refill_lag_window",
        events_file="liquidity_refill_lag_window_events.csv",
        signal_column="liquidity_refill_lag_event",
    ),
    "liquidity_absence_window": EventRegistrySpec(
        event_type="liquidity_absence_window",
        reports_dir="liquidity_absence_window",
        events_file="liquidity_absence_window_events.csv",
        signal_column="liquidity_absence_event",
    ),
    "vol_aftershock_window": EventRegistrySpec(
        event_type="vol_aftershock_window",
        reports_dir="vol_aftershock_window",
        events_file="vol_aftershock_window_events.csv",
        signal_column="vol_aftershock_event",
    ),
    "directional_exhaustion_after_forced_flow": EventRegistrySpec(
        event_type="directional_exhaustion_after_forced_flow",
        reports_dir="directional_exhaustion_after_forced_flow",
        events_file="directional_exhaustion_after_forced_flow_events.csv",
        signal_column="forced_flow_exhaustion_event",
    ),
    "cross_venue_desync": EventRegistrySpec(
        event_type="cross_venue_desync",
        reports_dir="cross_venue_desync",
        events_file="cross_venue_desync_events.csv",
        signal_column="cross_venue_desync_event",
    ),
    "liquidity_vacuum": EventRegistrySpec(
        event_type="liquidity_vacuum",
        reports_dir="liquidity_vacuum",
        events_file="liquidity_vacuum_events.csv",
        signal_column="liquidity_vacuum_event",
    ),
    "funding_extreme_reversal_window": EventRegistrySpec(
        event_type="funding_extreme_reversal_window",
        reports_dir="funding_extreme_reversal_window",
        events_file="funding_extreme_reversal_window_events.csv",
        signal_column="funding_extreme_event",
    ),
    "range_compression_breakout_window": EventRegistrySpec(
        event_type="range_compression_breakout_window",
        reports_dir="range_compression_breakout_window",
        events_file="range_compression_breakout_window_events.csv",
        signal_column="range_compression_breakout_event",
    ),
    "LIQUIDATION_CASCADE": EventRegistrySpec(
        event_type="LIQUIDATION_CASCADE",
        reports_dir="LIQUIDATION_CASCADE",
        events_file="LIQUIDATION_CASCADE_events.csv",
        signal_column="liquidation_cascade_event",
    ),
    "liquidity_vacuum": EventRegistrySpec(
        event_type="liquidity_vacuum",
        reports_dir="liquidity_vacuum",
        events_file="liquidity_vacuum_events.csv",
        signal_column="liquidity_vacuum_event",
    ),
}

SIGNAL_TO_EVENT_TYPE: Dict[str, str] = {
    spec.signal_column: event_type for event_type, spec in EVENT_REGISTRY_SPECS.items()
}
REGISTRY_BACKED_SIGNALS = set(SIGNAL_TO_EVENT_TYPE.keys())

REGISTRY_EVENT_COLUMNS = [
    "run_id",
    "event_type",
    "signal_column",
    "timestamp",
    "symbol",
    "event_id",
    "features_at_event",
]


def _empty_registry_events() -> pd.DataFrame:
    return pd.DataFrame(columns=REGISTRY_EVENT_COLUMNS)


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

    out = events.copy()
    timestamp_col = _first_existing_column(out, ["enter_ts", "anchor_ts", "timestamp", "event_ts"])
    if timestamp_col is None:
        return _empty_registry_events()

    out["timestamp"] = pd.to_datetime(out[timestamp_col], utc=True, errors="coerce")
    out = out.dropna(subset=["timestamp"]).copy()
    if out.empty:
        return _empty_registry_events()

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

    out = out.sort_values(["timestamp", "symbol", "event_id"]).reset_index(drop=True)
    out["features_at_event"] = out.apply(_feature_payload, axis=1)

    result = pd.DataFrame(
        {
            "run_id": str(run_id),
            "event_type": spec.event_type,
            "signal_column": spec.signal_column,
            "timestamp": out["timestamp"],
            "symbol": out["symbol"],
            "event_id": out["event_id"],
            "features_at_event": out["features_at_event"],
        }
    )
    result = result.drop_duplicates(subset=["event_type", "timestamp", "symbol", "event_id"]).reset_index(drop=True)
    return result


def _read_phase1_events(data_root: Path, run_id: str, spec: EventRegistrySpec) -> pd.DataFrame:
    path = Path(data_root) / "reports" / spec.reports_dir / str(run_id) / spec.events_file
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
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
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    out = out.dropna(subset=["timestamp"]).copy()
    out["symbol"] = out["symbol"].fillna("ALL").astype(str).str.upper().replace("", "ALL")
    out["event_id"] = out["event_id"].fillna("").astype(str)
    out["features_at_event"] = out["features_at_event"].fillna("{}").astype(str)
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


def _load_symbol_timestamps(data_root: Path, run_id: str, symbol: str, timeframe: str = "15m") -> pd.Series:
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
    ts = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce").dropna()
    if ts.empty:
        return pd.Series(dtype="datetime64[ns, UTC]")
    return pd.Series(sorted(pd.DatetimeIndex(ts).unique()))


def build_event_flags(
    *,
    events: pd.DataFrame,
    symbols: Sequence[str],
    data_root: Path,
    run_id: str,
    timeframe: str = "15m",
) -> pd.DataFrame:
    symbols_clean = [str(s).strip().upper() for s in symbols if str(s).strip()]
    symbols_clean = list(dict.fromkeys(symbols_clean))
    if not symbols_clean and not events.empty:
        symbols_clean = sorted(set(events["symbol"].dropna().astype(str).str.upper().tolist()) - {"ALL"})

    frames: List[pd.DataFrame] = []
    for symbol in symbols_clean:
        ts = _load_symbol_timestamps(data_root=data_root, run_id=run_id, symbol=symbol, timeframe=timeframe)
        if ts.empty:
            event_ts = pd.Series(dtype="datetime64[ns, UTC]")
            if not events.empty:
                local = events[(events["symbol"] == symbol) | (events["symbol"] == "ALL")]
                event_ts = pd.to_datetime(local.get("timestamp", pd.Series(dtype=object)), utc=True, errors="coerce").dropna()
            ts = pd.Series(sorted(pd.DatetimeIndex(event_ts).unique())) if not event_ts.empty else ts
        if ts.empty:
            continue
        frame = pd.DataFrame({"timestamp": pd.to_datetime(ts, utc=True), "symbol": symbol})
        for signal in sorted(REGISTRY_BACKED_SIGNALS):
            frame[signal] = False
        frames.append(frame)

    if not frames:
        return pd.DataFrame(columns=["timestamp", "symbol", *sorted(REGISTRY_BACKED_SIGNALS)])

    flags = pd.concat(frames, ignore_index=True)
    flags["timestamp"] = pd.to_datetime(flags["timestamp"], utc=True, errors="coerce")
    flags = flags.dropna(subset=["timestamp"]).copy()

    if not events.empty:
        events_copy = events.copy()
        events_copy["timestamp"] = pd.to_datetime(events_copy["timestamp"], utc=True, errors="coerce")
        for row in events_copy.itertuples(index=False):
            signal = str(getattr(row, "signal_column", "")).strip()
            if signal not in REGISTRY_BACKED_SIGNALS:
                continue
            symbol = str(getattr(row, "symbol", "ALL")).strip().upper() or "ALL"
            ts = getattr(row, "timestamp", pd.NaT)
            if pd.isna(ts):
                continue
            if symbol == "ALL":
                mask = flags["timestamp"] == ts
            else:
                mask = (flags["symbol"] == symbol) & (flags["timestamp"] == ts)
            if mask.any():
                flags.loc[mask, signal] = True

    for signal in sorted(REGISTRY_BACKED_SIGNALS):
        flags[signal] = flags[signal].fillna(False).astype(bool)
    flags = flags.sort_values(["timestamp", "symbol"]).reset_index(drop=True)
    return flags


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
        cols = ["timestamp", "symbol", *sorted(REGISTRY_BACKED_SIGNALS)]
        return pd.DataFrame(columns=cols)

    flags["timestamp"] = pd.to_datetime(flags.get("timestamp"), utc=True, errors="coerce")
    flags = flags.dropna(subset=["timestamp"]).copy()
    if symbol is not None:
        symbol_norm = str(symbol).strip().upper()
        flags = flags[flags["symbol"].astype(str).str.upper() == symbol_norm].copy()
    for signal in sorted(REGISTRY_BACKED_SIGNALS):
        if signal not in flags.columns:
            flags[signal] = False
        flags[signal] = flags[signal].fillna(False).astype(bool)
    cols = ["timestamp", "symbol", *sorted(REGISTRY_BACKED_SIGNALS)]
    return flags[cols].sort_values(["timestamp", "symbol"]).reset_index(drop=True)
