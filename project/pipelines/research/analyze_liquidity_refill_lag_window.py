from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import choose_partition_dir, ensure_dir, list_parquet_files, read_parquet, run_scoped_lake_path


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
    frame = frame.sort_values("timestamp").reset_index(drop=True)

    close = frame["close"].astype(float)
    frame["ret_1"] = close.pct_change()
    frame["logret_1"] = np.log(close).diff()
    frame["rv"] = frame["rv_96"].astype(float) if "rv_96" in frame.columns else frame["logret_1"].rolling(96, min_periods=32).std()
    frame["range_frac"] = (frame["high"].astype(float) - frame["low"].astype(float)) / close.replace(0.0, np.nan)
    frame["range_base"] = frame["range_frac"].rolling(96, min_periods=32).median()
    frame["tail_move"] = frame["ret_1"].abs()
    return frame


def _hazard(times: pd.Series, horizon: int) -> pd.DataFrame:
    t = times.dropna().astype(int)
    rows = []
    for age in range(1, horizon + 1):
        at_risk = int((times.isna() | (times >= age)).sum())
        hits = int((t == age).sum())
        rows.append({"age": age, "at_risk": at_risk, "hits": hits, "hazard": float(hits / at_risk) if at_risk else np.nan})
    return pd.DataFrame(rows)


def _event_metrics(
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    rv: np.ndarray,
    tail_hits: np.ndarray | None,
    start_idx: int,
    end_idx: int,
) -> Dict[str, float]:
    if start_idx > end_idx:
        return {}

    rv_window = rv[start_idx : end_idx + 1]
    realized_vol_mean = float(np.nanmean(rv_window)) if np.isfinite(rv_window).any() else np.nan

    close0 = float(close[start_idx]) if np.isfinite(close[start_idx]) else np.nan
    range_expansion = np.nan
    if np.isfinite(close0) and close0 > 0:
        high_max = float(np.nanmax(high[start_idx : end_idx + 1]))
        low_min = float(np.nanmin(low[start_idx : end_idx + 1]))
        if np.isfinite(high_max) and np.isfinite(low_min):
            range_expansion = float((high_max - low_min) / close0)

    t_tail = np.nan
    tail_move_probability = np.nan
    if tail_hits is not None:
        tail_slice = tail_hits[start_idx : end_idx + 1]
        tail_move_probability = float(np.mean(tail_slice))
        hit_pos = np.flatnonzero(tail_slice)
        if hit_pos.size:
            t_tail = float(hit_pos[0])

    return {
        "realized_vol_mean": realized_vol_mean,
        "range_expansion": range_expansion,
        "tail_move_probability": tail_move_probability,
        "tail_move_within": int(np.isfinite(t_tail)),
        "time_to_tail_move": t_tail,
    }


def _pick_control_index(
    pool_indices: np.ndarray,
    pool_rv_rank: np.ndarray,
    rv_rank: float,
    seed: int,
    top_k: int = 200,
) -> int | None:
    if pool_indices.size == 0:
        return None

    rs = np.random.RandomState(seed)

    if not np.isfinite(rv_rank):
        pos = int(rs.choice(pool_indices.size, size=1, replace=False)[0])
        return int(pool_indices[pos])

    diffs = np.abs(pool_rv_rank - rv_rank)
    finite = np.isfinite(diffs)
    if not finite.any():
        pos = int(rs.choice(pool_indices.size, size=1, replace=False)[0])
        return int(pool_indices[pos])

    valid_indices = pool_indices[finite]
    valid_diffs = diffs[finite]
    k = min(top_k, valid_indices.size)
    if k <= 0:
        return None

    # Match pandas nsmallest tie handling by stable sorting then truncating.
    nearest_idx = np.argsort(valid_diffs, kind="mergesort")[:k]
    candidates = valid_indices[nearest_idx]
    pos = int(rs.choice(candidates.size, size=1, replace=False)[0])
    return int(candidates[pos])


def _build_event_and_control_rows(
    symbol: str,
    core: pd.DataFrame,
    window_end: int,
    impulse_q: float,
    refill_horizon: int,
    refill_mult: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if core.empty:
        return pd.DataFrame(), pd.DataFrame()

    tail_threshold = float(core["tail_move"].quantile(0.95)) if core["tail_move"].notna().any() else np.nan
    impulse_threshold = float(core["tail_move"].quantile(impulse_q)) if core["tail_move"].notna().any() else np.nan
    tail_hits = (core["tail_move"].to_numpy(dtype=float, copy=False) >= tail_threshold) if np.isfinite(tail_threshold) else None

    rows_event: List[Dict[str, object]] = []
    rows_ctrl: List[Dict[str, object]] = []

    close = core["close"].to_numpy(dtype=float, copy=False)
    high = core["high"].to_numpy(dtype=float, copy=False)
    low = core["low"].to_numpy(dtype=float, copy=False)
    rv = core["rv"].to_numpy(dtype=float, copy=False)
    range_frac = core["range_frac"].to_numpy(dtype=float, copy=False)
    range_base = core["range_base"].to_numpy(dtype=float, copy=False)
    rv_rank = core["rv"].rank(pct=True).to_numpy(dtype=float, copy=False)
    years = core["timestamp"].dt.year.to_numpy(dtype=int, copy=False)
    timestamps = core["timestamp"].to_numpy(copy=False)

    candidate_idx = (
        np.flatnonzero((core["tail_move"].to_numpy(dtype=float, copy=False) >= impulse_threshold) & np.isfinite(range_base))
        if np.isfinite(impulse_threshold)
        else np.array([], dtype=np.int64)
    )
    pool_indices = (
        np.flatnonzero((core["tail_move"].to_numpy(dtype=float, copy=False) < impulse_threshold) & np.isfinite(range_base))
        if np.isfinite(impulse_threshold)
        else np.array([], dtype=np.int64)
    )
    pool_rv_rank = pd.Series(rv[pool_indices]).rank(pct=True).to_numpy(dtype=float, copy=False) if pool_indices.size else np.array([], dtype=float)

    seen_until = -1
    for idx in candidate_idx:
        if idx <= seen_until:
            continue
        refill_end = min(len(core) - 1, int(idx) + refill_horizon)
        threshold = float(range_base[idx]) * refill_mult
        refill_time = np.flatnonzero(range_frac[idx : refill_end + 1] <= threshold)

        # event is a refill failure/lag: not normalized within refill_horizon bars
        if refill_time.size:
            continue

        end_idx = min(len(core) - 1, int(idx) + window_end)
        seen_until = end_idx
        metrics = _event_metrics(
            close=close,
            high=high,
            low=low,
            rv=rv,
            tail_hits=tail_hits,
            start_idx=int(idx),
            end_idx=end_idx,
        )
        if not metrics:
            continue
        event_id = f"lrl_{symbol}_{int(idx):06d}"
        event_rv_rank = float(rv_rank[idx]) if np.isfinite(rv_rank[idx]) else np.nan

        rows_event.append(
            {
                "event_type": "liquidity_refill_lag_window",
                "event_id": event_id,
                "symbol": symbol,
                "anchor_ts": timestamps[idx],
                "start_idx": int(idx),
                "end_idx": end_idx,
                "year": int(years[idx]),
                "impulse_threshold": impulse_threshold,
                "refill_horizon": refill_horizon,
                "refill_mult": refill_mult,
                "rv_rank": event_rv_rank,
                **metrics,
            }
        )

        c_idx = _pick_control_index(
            pool_indices=pool_indices,
            pool_rv_rank=pool_rv_rank,
            rv_rank=event_rv_rank,
            seed=int(idx),
        )
        if c_idx is None:
            continue
        c_end = min(len(core) - 1, c_idx + window_end)
        c_metrics = _event_metrics(
            close=close,
            high=high,
            low=low,
            rv=rv,
            tail_hits=tail_hits,
            start_idx=c_idx,
            end_idx=c_end,
        )
        if not c_metrics:
            continue
        control_rv_rank = np.nan
        if pool_indices.size:
            pool_pos = int(np.searchsorted(pool_indices, c_idx))
            if pool_pos < pool_indices.size and int(pool_indices[pool_pos]) == int(c_idx):
                control_rv_rank = float(pool_rv_rank[pool_pos])
        rows_ctrl.append(
            {
                "event_id": event_id,
                "symbol": symbol,
                "start_idx": c_idx,
                "end_idx": c_end,
                "rv_rank": control_rv_rank,
                **c_metrics,
            }
        )

    return pd.DataFrame(rows_event), pd.DataFrame(rows_ctrl)


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase-1 analyzer for liquidity refill/requote lag windows")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--window_end", type=int, default=48)
    parser.add_argument("--impulse_quantile", type=float, default=0.99)
    parser.add_argument("--refill_horizon", type=int, default=8)
    parser.add_argument("--refill_mult", type=float, default=1.25)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "liquidity_refill_lag_window" / args.run_id
    ensure_dir(out_dir)

    event_parts: List[pd.DataFrame] = []
    ctrl_parts: List[pd.DataFrame] = []
    for symbol in [s.strip() for s in args.symbols.split(",") if s.strip()]:
        core = _load_feature_frame(args.run_id, symbol)
        if core.empty:
            continue
        ev, ct = _build_event_and_control_rows(
            symbol=symbol,
            core=core,
            window_end=args.window_end,
            impulse_q=args.impulse_quantile,
            refill_horizon=args.refill_horizon,
            refill_mult=args.refill_mult,
        )
        event_parts.append(ev)
        ctrl_parts.append(ct)

    events = pd.concat(event_parts, ignore_index=True) if event_parts else pd.DataFrame()
    controls = pd.concat(ctrl_parts, ignore_index=True) if ctrl_parts else pd.DataFrame()

    deltas = pd.DataFrame()
    hazards = pd.DataFrame()
    phase = pd.DataFrame()
    sign = pd.DataFrame()

    if not events.empty:
        merged = events.merge(controls, on=["event_id", "symbol"], how="left", suffixes=("", "_ctrl"))
        deltas = pd.DataFrame(
            {
                "event_id": merged["event_id"],
                "symbol": merged["symbol"],
                "year": merged["year"],
                "delta_realized_vol_mean": merged["realized_vol_mean"] - merged["realized_vol_mean_ctrl"],
                "delta_range_expansion": merged["range_expansion"] - merged["range_expansion_ctrl"],
                "delta_tail_move_probability": merged["tail_move_probability"] - merged["tail_move_probability_ctrl"],
                "delta_tail_move_within": merged["tail_move_within"] - merged["tail_move_within_ctrl"],
            }
        )
        hazards = _hazard(events["time_to_tail_move"], args.window_end).assign(cohort="events")
        if not controls.empty:
            hazards = pd.concat([hazards, _hazard(controls["time_to_tail_move"], args.window_end).assign(cohort="controls")], ignore_index=True)
        phase = (
            deltas.groupby("symbol", as_index=False)
            .agg(
                n=("event_id", "size"),
                delta_realized_vol_mean=("delta_realized_vol_mean", "mean"),
                delta_range_expansion=("delta_range_expansion", "mean"),
                delta_tail_move_probability=("delta_tail_move_probability", "mean"),
            )
            .sort_values("symbol")
        )
        sign = (
            deltas.groupby(["symbol", "year"], as_index=False)
            .agg(
                n=("event_id", "size"),
                sign_tail_prob=("delta_tail_move_probability", lambda s: float(np.sign(np.nanmean(s)))),
                sign_vol=("delta_realized_vol_mean", lambda s: float(np.sign(np.nanmean(s)))),
            )
            .sort_values(["symbol", "year"])
        )

    events_path = out_dir / "liquidity_refill_lag_window_events.csv"
    controls_path = out_dir / "liquidity_refill_lag_window_controls.csv"
    deltas_path = out_dir / "liquidity_refill_lag_window_matched_deltas.csv"
    hazards_path = out_dir / "liquidity_refill_lag_window_hazards.csv"
    phase_path = out_dir / "liquidity_refill_lag_window_phase_stability.csv"
    sign_path = out_dir / "liquidity_refill_lag_window_sign_stability.csv"
    summary_md_path = out_dir / "liquidity_refill_lag_window_summary.md"
    summary_json_path = out_dir / "liquidity_refill_lag_window_summary.json"

    events.to_csv(events_path, index=False)
    controls.to_csv(controls_path, index=False)
    deltas.to_csv(deltas_path, index=False)
    hazards.to_csv(hazards_path, index=False)
    phase.to_csv(phase_path, index=False)
    sign.to_csv(sign_path, index=False)

    summary = {
        "event_type": "liquidity_refill_lag_window",
        "phase": 1,
        "window": {"x": 0, "y": args.window_end},
        "impulse_quantile": args.impulse_quantile,
        "refill_horizon": args.refill_horizon,
        "refill_mult": args.refill_mult,
        "actions_generated": 0,
        "events": int(len(events)),
        "controls": int(len(controls)),
        "outputs": {
            "events": str(events_path),
            "controls": str(controls_path),
            "deltas": str(deltas_path),
            "hazards": str(hazards_path),
            "phase_stability": str(phase_path),
            "sign_stability": str(sign_path),
        },
    }
    summary_json_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    lines = [
        "# Liquidity Refill / Requote Lag Window (Phase 1)",
        "",
        f"Run ID: `{args.run_id}`",
        f"Window: [0, {args.window_end}]",
        f"Impulse quantile: {args.impulse_quantile}",
        f"Refill horizon: {args.refill_horizon}",
        f"Refill threshold multiplier: {args.refill_mult}",
        "",
        f"- Events: {len(events)}",
        f"- Controls: {len(controls)}",
        "- Actions generated: 0 (Phase 1 structure only)",
        "",
        "## Matched deltas (head)",
        _table_text(deltas.head(12)) if not deltas.empty else "No matched deltas",
        "",
        "## Hazards (head)",
        _table_text(hazards.head(20)) if not hazards.empty else "No hazards",
        "",
        "## Phase stability",
        _table_text(phase) if not phase.empty else "No phase stability rows",
        "",
        "## Sign stability",
        _table_text(sign) if not sign.empty else "No sign stability rows",
    ]
    summary_md_path.write_text("\n".join(lines), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
