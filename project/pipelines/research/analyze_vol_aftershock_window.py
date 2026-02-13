from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from features.vol_shock_relaxation import DEFAULT_VSR_CONFIG, detect_vol_shock_relaxation_events
from pipelines._lib.io_utils import choose_partition_dir, ensure_dir, list_parquet_files, read_parquet, run_scoped_lake_path

WINDOW_SWEEP: List[Tuple[int, int]] = [(0, 48), (24, 96), (48, 144)]


def _table_text(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False)
    except ImportError:
        return df.to_string(index=False)


def _load_feature_frame(run_id: str, symbol: str, timeframe: str = "15m") -> pd.DataFrame:
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "features", "perp", symbol, timeframe, "features_v1"),
        DATA_ROOT / "lake" / "features" / "perp" / symbol / timeframe / "features_v1",
    ]
    features_dir = choose_partition_dir(candidates)
    if features_dir is None:
        return pd.DataFrame()
    frame = read_parquet(list_parquet_files(features_dir))
    if frame.empty:
        return frame
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    return frame.sort_values("timestamp").reset_index(drop=True)


def _occupied(events: pd.DataFrame, n_rows: int, cooldown: int) -> np.ndarray:
    occ = np.zeros(n_rows, dtype=bool)
    if events.empty:
        return occ
    for _, ev in events.iterrows():
        s = int(ev["enter_idx"])
        e = min(n_rows - 1, int(ev["exit_idx"]) + cooldown)
        occ[s : e + 1] = True
    return occ


def _session_from_hour(hour: int) -> str:
    if 0 <= hour <= 7:
        return "Asia"
    if 8 <= hour <= 15:
        return "EU"
    return "US"


def _hazard(times: pd.Series, horizon: int) -> pd.DataFrame:
    rows = []
    t = times.dropna().astype(int)
    for age in range(1, horizon + 1):
        at_risk = int((times.isna() | (times >= age)).sum())
        hits = int((t == age).sum())
        rows.append({"age": age, "at_risk": at_risk, "hits": hits, "hazard": float(hits / at_risk) if at_risk else np.nan})
    return pd.DataFrame(rows)


def _event_metrics_from_window(core: pd.DataFrame, start: int, end: int, t_shock: float) -> Dict[str, float]:
    if end <= start:
        return {}
    win = core.iloc[start : end + 1]
    if win.empty:
        return {}

    sec_time = np.nan
    for k in range(start, end + 1):
        sr = float(core["shock_ratio"].iat[k]) if pd.notna(core["shock_ratio"].iat[k]) else np.nan
        if np.isfinite(sr) and sr >= t_shock:
            sec_time = float(k - start)
            break

    rv = win["rv"].astype(float)
    close0 = float(core["close"].iat[start]) if pd.notna(core["close"].iat[start]) else np.nan
    range_expansion = np.nan
    if np.isfinite(close0) and close0 > 0:
        range_expansion = float((win["high"].max() - win["low"].min()) / close0)

    return {
        "secondary_shock_within": int(np.isfinite(sec_time)),
        "time_to_secondary_shock": sec_time,
        "realized_vol_mean": float(rv.mean()) if not rv.empty else np.nan,
        "range_expansion": range_expansion,
    }


def _build_event_rows(
    symbol: str,
    core: pd.DataFrame,
    parent_events: pd.DataFrame,
    t_shock: float,
    windows: Iterable[Tuple[int, int]],
) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    rv_peak_quantiles = pd.qcut(parent_events["rv_peak"].rank(method="first"), 4, labels=False, duplicates="drop") if "rv_peak" in parent_events.columns and not parent_events.empty else pd.Series([0] * len(parent_events), index=parent_events.index)
    for idx, parent in parent_events.reset_index(drop=True).iterrows():
        exit_idx = int(parent["exit_idx"])
        for x, y in windows:
            start = min(len(core) - 1, exit_idx + x)
            end = min(len(core) - 1, exit_idx + y)
            metrics = _event_metrics_from_window(core, start, end, t_shock)
            if not metrics:
                continue
            hour = int(pd.Timestamp(core["timestamp"].iat[start]).hour)
            rows.append(
                {
                    "event_type": "vol_aftershock_window",
                    "parent_event_id": parent.get("event_id", f"{symbol}_{idx}"),
                    "symbol": symbol,
                    "window_x": x,
                    "window_y": y,
                    "enter_ts": core["timestamp"].iat[start],
                    "start_idx": start,
                    "end_idx": end,
                    "year": int(pd.Timestamp(core["timestamp"].iat[start]).year),
                    "session": _session_from_hour(hour),
                    "prior_shock_severity_q": int(rv_peak_quantiles.iloc[idx]) if len(rv_peak_quantiles) > idx and pd.notna(rv_peak_quantiles.iloc[idx]) else 0,
                    "parent_relax_duration": float(parent.get("duration_bars", np.nan)),
                    "parent_time_to_relax": float(parent.get("time_to_relax", np.nan)),
                    "parent_rv_peak": float(parent.get("rv_peak", np.nan)),
                    "rv_rank": float(core["rv_base"].rank(pct=True).iat[start]) if "rv_base" in core.columns else np.nan,
                    **metrics,
                }
            )
    return pd.DataFrame(rows)


def _matched_controls(
    core: pd.DataFrame,
    parent_events: pd.DataFrame,
    events_df_sym: pd.DataFrame,
    t_shock: float,
    seed: int,
) -> pd.DataFrame:
    occ = _occupied(parent_events, n_rows=len(core), cooldown=DEFAULT_VSR_CONFIG.cooldown_bars)
    pool = core[(~occ) & (core["shock_ratio"] < t_shock)].copy().reset_index().rename(columns={"index": "bar_idx"})
    if pool.empty or events_df_sym.empty:
        return pd.DataFrame()

    pool["rv_rank"] = pool["rv_base"].rank(pct=True)
    pool_ranks = pool["rv_rank"].to_numpy(dtype=float)
    rng = np.random.default_rng(seed)
    rows: List[Dict[str, object]] = []

    for ev in events_df_sym.itertuples(index=False):
        x = int(ev.window_x)
        y = int(ev.window_y)
        target_rank = float(getattr(ev, "rv_rank", np.nan))
        if np.isfinite(target_rank):
            top_k = min(200, len(pool_ranks))
            nearest_idx = np.argpartition(np.abs(pool_ranks - target_rank), top_k - 1)[:top_k]
            pick_pool = pool.iloc[nearest_idx]
        else:
            pick_pool = pool
        if pick_pool.empty:
            continue
        pick = pick_pool.sample(n=1, random_state=int(rng.integers(0, 2**31 - 1))).iloc[0]
        start = min(len(core) - 1, int(pick["bar_idx"] + x))
        end = min(len(core) - 1, int(pick["bar_idx"] + y))
        metrics = _event_metrics_from_window(core, start, end, t_shock)
        if not metrics:
            continue
        rows.append(
            {
                "event_id": ev.parent_event_id,
                "symbol": ev.symbol,
                "window_x": x,
                "window_y": y,
                "start_idx": start,
                "end_idx": end,
                "rv_rank": float(pick.get("rv_rank", np.nan)),
                **metrics,
            }
        )
    return pd.DataFrame(rows)


def _placebo_windows(core: pd.DataFrame, parent_events: pd.DataFrame, events_df_sym: pd.DataFrame, t_shock: float, seed: int) -> pd.DataFrame:
    occ = _occupied(parent_events, n_rows=len(core), cooldown=DEFAULT_VSR_CONFIG.cooldown_bars)
    pool = core[(~occ) & (core["shock_ratio"] < t_shock)].copy().reset_index().rename(columns={"index": "bar_idx"})
    if pool.empty or events_df_sym.empty:
        return pd.DataFrame()
    pool["rv_rank"] = pool["rv_base"].rank(pct=True)

    pool_ranks = pool["rv_rank"].to_numpy(dtype=float)
    pool_sessions = np.where(
        pool["timestamp"].dt.hour <= 7,
        "Asia",
        np.where(pool["timestamp"].dt.hour <= 15, "EU", "US"),
    )

    rng = np.random.default_rng(seed)
    rows: List[Dict[str, object]] = []
    for ev in events_df_sym.itertuples(index=False):
        x = int(ev.window_x)
        y = int(ev.window_y)
        target_rank = float(getattr(ev, "rv_rank", np.nan))
        target_session = str(getattr(ev, "session", ""))
        if np.isfinite(target_rank):
            top_k = min(300, len(pool_ranks))
            nearest_idx = np.argpartition(np.abs(pool_ranks - target_rank), top_k - 1)[:top_k]
            pick_pool = pool.iloc[nearest_idx]
            pick_sessions = pool_sessions[nearest_idx]
        else:
            pick_pool = pool
            pick_sessions = pool_sessions
        if target_session:
            same_session = pick_pool[pick_sessions == target_session]
            if not same_session.empty:
                pick_pool = same_session
        if pick_pool.empty:
            continue
        pick = pick_pool.sample(n=1, random_state=int(rng.integers(0, 2**31 - 1))).iloc[0]
        start = min(len(core) - 1, int(pick["bar_idx"] + x))
        end = min(len(core) - 1, int(pick["bar_idx"] + y))
        metrics = _event_metrics_from_window(core, start, end, t_shock)
        if not metrics:
            continue
        rows.append(
            {
                "source_parent_event_id": ev.parent_event_id,
                "symbol": ev.symbol,
                "window_x": x,
                "window_y": y,
                "placebo_start_idx": start,
                "placebo_end_idx": end,
                "placebo_rv_rank": float(pick.get("rv_rank", np.nan)),
                **metrics,
            }
        )
    return pd.DataFrame(rows)


def _window_sensitivity(events: pd.DataFrame, controls: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    rows = []
    ctrl_means = controls.groupby(["event_id", "window_x", "window_y"], as_index=False).mean(numeric_only=True) if not controls.empty else pd.DataFrame()
    merged = (
        events.merge(ctrl_means, left_on=["parent_event_id", "window_x", "window_y"], right_on=["event_id", "window_x", "window_y"], how="left", suffixes=("", "_ctrl"))
        if not ctrl_means.empty
        else events.copy()
    )
    for (x, y), g in merged.groupby(["window_x", "window_y"], sort=True):
        p_secondary = float(g["secondary_shock_within"].mean())
        delta_vol = float((g["realized_vol_mean"] - g.get("realized_vol_mean_ctrl", np.nan)).mean()) if "realized_vol_mean_ctrl" in g.columns else np.nan
        rows.append(
            {
                "window_x": int(x),
                "window_y": int(y),
                "n": int(len(g)),
                "p_secondary_shock": p_secondary,
                "delta_realized_vol_mean": delta_vol,
            }
        )
    out = pd.DataFrame(rows).sort_values(["window_x", "window_y"])
    if len(out) >= 2:
        out["p_secondary_monotone_non_decreasing"] = bool(out["p_secondary_shock"].is_monotonic_increasing)
        out["delta_realized_vol_monotone_non_decreasing"] = bool(out["delta_realized_vol_mean"].fillna(0).is_monotonic_increasing)
    else:
        out["p_secondary_monotone_non_decreasing"] = False
        out["delta_realized_vol_monotone_non_decreasing"] = False
    return out


def _conditional_hazards(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    rows = []
    events = events.copy()
    events["relax_duration_bucket"] = pd.cut(
        events["parent_relax_duration"],
        bins=[-np.inf, 16, 48, np.inf],
        labels=["short", "medium", "long"],
    ).astype(str)

    for (x, y), window_df in events.groupby(["window_x", "window_y"], sort=True):
        for col in ["prior_shock_severity_q", "session", "relax_duration_bucket"]:
            for group_val, g in window_df.groupby(col, sort=True):
                h = _hazard(g["time_to_secondary_shock"], horizon=96)
                if h.empty:
                    continue
                h = h.assign(window_x=int(x), window_y=int(y), slice_type=col, slice_value=str(group_val))
                rows.append(h)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def _phase_and_sign(events: pd.DataFrame, controls: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if events.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    ctrl_mean = controls.groupby(["event_id", "window_x", "window_y"], as_index=False).mean(numeric_only=True) if not controls.empty else pd.DataFrame()
    merged = (
        events.merge(ctrl_mean, left_on=["parent_event_id", "window_x", "window_y"], right_on=["event_id", "window_x", "window_y"], how="left", suffixes=("", "_ctrl"))
        if not ctrl_mean.empty
        else events.copy()
    )
    merged["delta_secondary_shock"] = merged["secondary_shock_within"] - merged.get("secondary_shock_within_ctrl", np.nan)
    merged["delta_realized_vol_mean_96"] = merged["realized_vol_mean"] - merged.get("realized_vol_mean_ctrl", np.nan)
    merged["delta_range_expansion_96"] = merged["range_expansion"] - merged.get("range_expansion_ctrl", np.nan)

    deltas = merged[
        [
            "parent_event_id",
            "symbol",
            "window_x",
            "window_y",
            "delta_secondary_shock",
            "delta_realized_vol_mean_96",
            "delta_range_expansion_96",
            "year",
        ]
    ]

    phase = (
        deltas.groupby(["window_x", "window_y"], as_index=False)
        .agg(
            n=("delta_secondary_shock", "size"),
            delta_secondary_shock=("delta_secondary_shock", "mean"),
            delta_realized_vol_mean_96=("delta_realized_vol_mean_96", "mean"),
            delta_range_expansion_96=("delta_range_expansion_96", "mean"),
        )
        .sort_values(["window_x", "window_y"])
    )

    sign = (
        deltas.groupby(["window_x", "window_y", "year"], as_index=False)
        .agg(
            n=("delta_secondary_shock", "size"),
            sign_secondary=("delta_secondary_shock", lambda s: float(np.sign(np.nanmean(s)))),
            sign_realized_vol=("delta_realized_vol_mean_96", lambda s: float(np.sign(np.nanmean(s)))),
            sign_range_expansion=("delta_range_expansion_96", lambda s: float(np.sign(np.nanmean(s)))),
        )
        .sort_values(["window_x", "window_y", "year"])
    )
    return deltas, phase, sign


def _interpretation_note(window_sensitivity: pd.DataFrame, placebo_deltas: pd.DataFrame) -> str:
    if window_sensitivity.empty:
        return "No defensible re-risk window identified (insufficient data)."

    best = window_sensitivity.sort_values("p_secondary_shock").iloc[0]
    placebo_secondary = float(placebo_deltas["secondary_shock_within"].mean()) if not placebo_deltas.empty else np.nan
    best_p = float(best["p_secondary_shock"])
    best_win = f"[{int(best['window_x'])}, {int(best['window_y'])}]"

    if np.isfinite(placebo_secondary) and best_p < placebo_secondary:
        return (
            f"Potentially defensible re-risk pocket: window {best_win} shows the lowest observed secondary-shock rate "
            f"({best_p:.3f}) below placebo baseline ({placebo_secondary:.3f}); treat as Phase-2 candidate only after split stability checks."
        )
    return (
        f"No defensible re-risk window yet: best observed window {best_win} secondary-shock rate ({best_p:.3f}) "
        f"does not improve versus placebo baseline ({placebo_secondary:.3f} if available)."
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase-1 analyzer for vol aftershock / re-risk window")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--window_start", type=int, default=0)
    parser.add_argument("--window_end", type=int, default=96)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "vol_aftershock_window" / args.run_id
    ensure_dir(out_dir)

    all_event_rows: List[pd.DataFrame] = []
    all_control_rows: List[pd.DataFrame] = []
    all_placebo_rows: List[pd.DataFrame] = []

    windows = sorted(set(WINDOW_SWEEP + [(args.window_start, args.window_end)]))

    for symbol in symbols:
        frame = _load_feature_frame(args.run_id, symbol)
        if frame.empty:
            continue
        parent_events, core, meta = detect_vol_shock_relaxation_events(frame, symbol=symbol, config=DEFAULT_VSR_CONFIG)
        if parent_events.empty or core.empty:
            continue
        t_shock = float(meta.get("t_shock", np.nan))

        events_df_sym = _build_event_rows(symbol=symbol, core=core, parent_events=parent_events, t_shock=t_shock, windows=windows)
        controls_df_sym = _matched_controls(core=core, parent_events=parent_events, events_df_sym=events_df_sym, t_shock=t_shock, seed=7)
        placebo_df_sym = _placebo_windows(core=core, parent_events=parent_events, events_df_sym=events_df_sym, t_shock=t_shock, seed=11)

        all_event_rows.append(events_df_sym)
        all_control_rows.append(controls_df_sym)
        all_placebo_rows.append(placebo_df_sym)

    events_df = pd.concat(all_event_rows, ignore_index=True) if all_event_rows else pd.DataFrame()
    controls_df = pd.concat(all_control_rows, ignore_index=True) if all_control_rows else pd.DataFrame()
    placebo_df = pd.concat(all_placebo_rows, ignore_index=True) if all_placebo_rows else pd.DataFrame()

    deltas, phase_stability, sign_stability = _phase_and_sign(events_df, controls_df)
    hazards = _hazard(events_df["time_to_secondary_shock"], horizon=96).assign(cohort="events") if not events_df.empty else pd.DataFrame()
    if not controls_df.empty:
        hazards = pd.concat([hazards, _hazard(controls_df["time_to_secondary_shock"], horizon=96).assign(cohort="controls")], ignore_index=True)
    conditional_hazards = _conditional_hazards(events_df)
    window_sensitivity = _window_sensitivity(events_df, controls_df)

    placebo_deltas = pd.DataFrame()
    if not placebo_df.empty and not events_df.empty:
        p = placebo_df.rename(
            columns={
                "secondary_shock_within": "placebo_secondary_shock_within",
                "realized_vol_mean": "placebo_realized_vol_mean",
                "range_expansion": "placebo_range_expansion",
            }
        )
        placebo_deltas = events_df.merge(
            p[["source_parent_event_id", "symbol", "window_x", "window_y", "placebo_secondary_shock_within", "placebo_realized_vol_mean", "placebo_range_expansion"]],
            left_on=["parent_event_id", "symbol", "window_x", "window_y"],
            right_on=["source_parent_event_id", "symbol", "window_x", "window_y"],
            how="left",
        )
        placebo_deltas["delta_secondary_vs_placebo"] = placebo_deltas["secondary_shock_within"] - placebo_deltas["placebo_secondary_shock_within"]
        placebo_deltas["delta_realized_vol_vs_placebo"] = placebo_deltas["realized_vol_mean"] - placebo_deltas["placebo_realized_vol_mean"]

    events_path = out_dir / "vol_aftershock_window_events.csv"
    controls_path = out_dir / "vol_aftershock_window_controls.csv"
    deltas_path = out_dir / "vol_aftershock_window_matched_deltas.csv"
    hazards_path = out_dir / "vol_aftershock_window_hazards.csv"
    conditional_hazards_path = out_dir / "vol_aftershock_window_conditional_hazards.csv"
    phase_path = out_dir / "vol_aftershock_window_phase_stability.csv"
    sign_path = out_dir / "vol_aftershock_window_sign_stability.csv"
    sweep_path = out_dir / "vol_aftershock_window_window_sensitivity.csv"
    placebo_path = out_dir / "vol_aftershock_window_placebo_deltas.csv"
    note_path = out_dir / "vol_aftershock_window_re_risk_note.md"
    summary_json_path = out_dir / "vol_aftershock_window_summary.json"
    summary_md_path = out_dir / "vol_aftershock_window_summary.md"

    events_df.to_csv(events_path, index=False)
    controls_df.to_csv(controls_path, index=False)
    deltas.to_csv(deltas_path, index=False)
    hazards.to_csv(hazards_path, index=False)
    conditional_hazards.to_csv(conditional_hazards_path, index=False)
    phase_stability.to_csv(phase_path, index=False)
    sign_stability.to_csv(sign_path, index=False)
    window_sensitivity.to_csv(sweep_path, index=False)
    placebo_deltas.to_csv(placebo_path, index=False)

    note = _interpretation_note(window_sensitivity, placebo_df)
    note_path.write_text("# Re-risk Defensibility Note\n\n" + note + "\n", encoding="utf-8")

    summary = {
        "event_type": "vol_aftershock_window",
        "phase": 1,
        "window": {"x": args.window_start, "y": args.window_end},
        "window_sweep": [{"x": x, "y": y} for x, y in windows],
        "actions_generated": 0,
        "events": int(len(events_df)),
        "controls": int(len(controls_df)),
        "outputs": {
            "events": str(events_path),
            "controls": str(controls_path),
            "deltas": str(deltas_path),
            "hazards": str(hazards_path),
            "conditional_hazards": str(conditional_hazards_path),
            "phase_stability": str(phase_path),
            "sign_stability": str(sign_path),
            "window_sensitivity": str(sweep_path),
            "placebo_deltas": str(placebo_path),
            "re_risk_note": str(note_path),
        },
    }
    summary_json_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    lines = [
        "# Vol Aftershock / Re-risk Window (Phase 1)",
        "",
        f"Run ID: `{args.run_id}`",
        f"Primary window arg: [{args.window_start}, {args.window_end}]",
        f"Sweep windows: {windows}",
        "",
        f"- Events: {len(events_df)}",
        f"- Controls: {len(controls_df)}",
        "- Actions generated: 0 (Phase 1 structure only)",
        "",
        "## Window sensitivity",
        _table_text(window_sensitivity) if not window_sensitivity.empty else "No window sensitivity rows",
        "",
        "## Matched deltas (head)",
        _table_text(deltas.head(12)) if not deltas.empty else "No matched deltas",
        "",
        "## Conditional hazards (head)",
        _table_text(conditional_hazards.head(20)) if not conditional_hazards.empty else "No conditional hazards",
        "",
        "## Placebo deltas (head)",
        _table_text(placebo_deltas.head(12)) if not placebo_deltas.empty else "No placebo deltas",
        "",
        "## Re-risk note",
        note,
    ]
    summary_md_path.write_text("\n".join(lines), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
