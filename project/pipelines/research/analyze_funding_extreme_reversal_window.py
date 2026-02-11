from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

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
    frame["ret_1"] = close.pct_change(fill_method=None)
    frame["logret_1"] = np.log(close).diff()
    frame["rv"] = frame["rv_96"].astype(float) if "rv_96" in frame.columns else frame["logret_1"].rolling(96, min_periods=32).std()
    frame["tail_move"] = frame["ret_1"].abs()
    frame["funding_rate_scaled"] = pd.to_numeric(frame.get("funding_rate_scaled"), errors="coerce").fillna(0.0)
    frame["funding_abs"] = frame["funding_rate_scaled"].abs()
    frame["anchor_hour"] = frame["timestamp"].dt.hour.astype(int)
    frame["rv_rank"] = frame["rv"].rank(pct=True)
    return frame


def _hazard(times: pd.Series, horizon: int) -> pd.DataFrame:
    t = times.dropna().astype(int)
    rows = []
    for age in range(1, horizon + 1):
        at_risk = int((times.isna() | (times >= age)).sum())
        hits = int((t == age).sum())
        rows.append({"age": age, "at_risk": at_risk, "hits": hits, "hazard": float(hits / at_risk) if at_risk else np.nan})
    return pd.DataFrame(rows)


def _window_metrics(core: pd.DataFrame, start_idx: int, end_idx: int, tail_threshold: float) -> Dict[str, float]:
    win = core.iloc[start_idx : end_idx + 1]
    if win.empty:
        return {}

    close0 = float(core["close"].iat[start_idx]) if pd.notna(core["close"].iat[start_idx]) else np.nan
    range_expansion = np.nan
    if np.isfinite(close0) and close0 > 0:
        range_expansion = float((win["high"].max() - win["low"].min()) / close0)

    t_tail = np.nan
    if np.isfinite(tail_threshold):
        for k in range(start_idx, end_idx + 1):
            if pd.notna(core["tail_move"].iat[k]) and float(core["tail_move"].iat[k]) >= tail_threshold:
                t_tail = float(k - start_idx)
                break

    return {
        "realized_vol_mean": float(win["rv"].mean()) if win["rv"].notna().any() else np.nan,
        "range_expansion": range_expansion,
        "tail_move_probability": float((win["tail_move"] >= tail_threshold).mean()) if np.isfinite(tail_threshold) else np.nan,
        "tail_move_within": int(np.isfinite(t_tail)),
        "time_to_tail_move": t_tail,
    }


def _sign_label(v: float) -> str:
    if v > 0:
        return "pos_funding"
    if v < 0:
        return "neg_funding"
    return "flat_funding"


def _vol_regime(v: float) -> str:
    if not np.isfinite(v):
        return "unknown"
    if v >= 0.66:
        return "high"
    if v <= 0.33:
        return "low"
    return "mid"


def _build_event_rows(symbol: str, core: pd.DataFrame, funding_abs_quantile: float, horizon: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if core.empty:
        return pd.DataFrame(), pd.DataFrame()

    threshold = float(core["funding_abs"].quantile(funding_abs_quantile)) if core["funding_abs"].notna().any() else np.nan
    if not np.isfinite(threshold):
        return pd.DataFrame(), pd.DataFrame()

    tail_threshold = float(core["tail_move"].quantile(0.95)) if core["tail_move"].notna().any() else np.nan
    event_idx = core.index[core["funding_abs"] >= threshold].tolist()
    control_pool = core[core["funding_abs"] < threshold].copy()

    events: List[Dict[str, object]] = []
    controls: List[Dict[str, object]] = []
    seen_until = -1
    for idx in event_idx:
        if idx <= seen_until:
            continue
        end_idx = min(len(core) - 1, idx + horizon)
        seen_until = end_idx

        funding = float(core["funding_rate_scaled"].iat[idx]) if pd.notna(core["funding_rate_scaled"].iat[idx]) else 0.0
        rv_rank = float(core["rv_rank"].iat[idx]) if pd.notna(core["rv_rank"].iat[idx]) else np.nan
        metrics = _window_metrics(core, idx, end_idx, tail_threshold)
        if not metrics:
            continue

        event_id = f"ferw_{symbol}_{idx:06d}"
        events.append(
            {
                "event_type": "funding_extreme_reversal_window",
                "event_id": event_id,
                "symbol": symbol,
                "anchor_ts": core["timestamp"].iat[idx],
                "anchor_hour": int(core["anchor_hour"].iat[idx]),
                "start_idx": idx,
                "end_idx": end_idx,
                "year": int(pd.Timestamp(core["timestamp"].iat[idx]).year),
                "funding_rate_scaled": funding,
                "funding_abs": abs(funding),
                "funding_abs_threshold": threshold,
                "funding_sign": _sign_label(funding),
                "rv_rank": rv_rank,
                "vol_regime": _vol_regime(rv_rank),
                "bull_bear": "bull" if funding > 0 else "bear",
                **metrics,
            }
        )

        if control_pool.empty:
            continue
        pool = control_pool[control_pool["anchor_hour"] == int(core["anchor_hour"].iat[idx])]
        if pool.empty:
            pool = control_pool
        if np.isfinite(rv_rank):
            pool = pool.assign(match_dist=(pool["rv_rank"] - rv_rank).abs()).nsmallest(200, "match_dist")
        if pool.empty:
            continue

        pick = pool.sample(n=1, random_state=idx).iloc[0]
        c_idx = int(pick.name)
        c_end = min(len(core) - 1, c_idx + horizon)
        c_metrics = _window_metrics(core, c_idx, c_end, tail_threshold)
        if not c_metrics:
            continue

        controls.append(
            {
                "event_id": event_id,
                "symbol": symbol,
                "anchor_hour": int(pick["anchor_hour"]),
                "control_idx": c_idx,
                "start_idx": c_idx,
                "end_idx": c_end,
                "funding_rate_scaled": float(pick["funding_rate_scaled"]) if pd.notna(pick["funding_rate_scaled"]) else 0.0,
                "funding_abs": float(pick["funding_abs"]) if pd.notna(pick["funding_abs"]) else 0.0,
                "rv_rank": float(pick["rv_rank"]) if pd.notna(pick["rv_rank"]) else np.nan,
                **c_metrics,
            }
        )
    return pd.DataFrame(events), pd.DataFrame(controls)


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase-1 analyzer for funding-extreme reversal windows")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--window_end", type=int, default=96)
    parser.add_argument("--funding_abs_quantile", type=float, default=0.98)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "funding_extreme_reversal_window" / args.run_id
    ensure_dir(out_dir)

    event_parts: List[pd.DataFrame] = []
    control_parts: List[pd.DataFrame] = []
    for symbol in [s.strip() for s in args.symbols.split(",") if s.strip()]:
        core = _load_feature_frame(args.run_id, symbol)
        if core.empty:
            continue
        ev, ct = _build_event_rows(symbol, core, funding_abs_quantile=args.funding_abs_quantile, horizon=args.window_end)
        event_parts.append(ev)
        control_parts.append(ct)

    events = pd.concat(event_parts, ignore_index=True) if event_parts else pd.DataFrame()
    controls = pd.concat(control_parts, ignore_index=True) if control_parts else pd.DataFrame()

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
                "funding_sign": merged["funding_sign"],
                "delta_realized_vol_mean": merged["realized_vol_mean"] - merged["realized_vol_mean_ctrl"],
                "delta_range_expansion": merged["range_expansion"] - merged["range_expansion_ctrl"],
                "delta_tail_move_probability": merged["tail_move_probability"] - merged["tail_move_probability_ctrl"],
                "delta_tail_move_within": merged["tail_move_within"] - merged["tail_move_within_ctrl"],
            }
        )
        hazards = pd.concat(
            [
                _hazard(events["time_to_tail_move"], args.window_end).assign(cohort="events"),
                _hazard(controls["time_to_tail_move"], args.window_end).assign(cohort="controls") if not controls.empty else pd.DataFrame(),
            ],
            ignore_index=True,
        )
        phase = (
            deltas.groupby(["funding_sign"], as_index=False)
            .agg(
                n=("event_id", "size"),
                delta_realized_vol_mean=("delta_realized_vol_mean", "mean"),
                delta_range_expansion=("delta_range_expansion", "mean"),
                delta_tail_move_probability=("delta_tail_move_probability", "mean"),
                delta_tail_move_within=("delta_tail_move_within", "mean"),
            )
            .sort_values("funding_sign")
        )
        sign = (
            deltas.groupby(["year", "funding_sign"], as_index=False)
            .agg(
                n=("event_id", "size"),
                sign_tail_prob=("delta_tail_move_probability", lambda s: float(np.sign(np.nanmean(s)))),
                sign_vol=("delta_realized_vol_mean", lambda s: float(np.sign(np.nanmean(s)))),
            )
            .sort_values(["year", "funding_sign"])
        )

    events_path = out_dir / "funding_extreme_reversal_window_events.csv"
    controls_path = out_dir / "funding_extreme_reversal_window_controls.csv"
    deltas_path = out_dir / "funding_extreme_reversal_window_matched_deltas.csv"
    hazards_path = out_dir / "funding_extreme_reversal_window_hazards.csv"
    phase_path = out_dir / "funding_extreme_reversal_window_phase_stability.csv"
    sign_path = out_dir / "funding_extreme_reversal_window_sign_stability.csv"
    summary_md_path = out_dir / "funding_extreme_reversal_window_summary.md"
    summary_json_path = out_dir / "funding_extreme_reversal_window_summary.json"

    events.to_csv(events_path, index=False)
    controls.to_csv(controls_path, index=False)
    deltas.to_csv(deltas_path, index=False)
    hazards.to_csv(hazards_path, index=False)
    phase.to_csv(phase_path, index=False)
    sign.to_csv(sign_path, index=False)

    summary = {
        "event_type": "funding_extreme_reversal_window",
        "phase": 1,
        "window": {"x": 0, "y": args.window_end},
        "funding_abs_quantile": args.funding_abs_quantile,
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
        "# Funding Extreme Reversal Window (Phase 1)",
        "",
        f"Run ID: `{args.run_id}`",
        f"Window: [0, {args.window_end}]",
        f"Funding |rate| quantile: {args.funding_abs_quantile}",
        "",
        f"- Events: {len(events)}",
        f"- Controls: {len(controls)}",
        "- Actions generated: 0 (Phase 1 structure only)",
        "",
        "## Matched deltas (head)",
        _table_text(deltas.head(12)) if not deltas.empty else "No matched deltas",
        "",
        "## Hazards (head)",
        _table_text(hazards.head(24)) if not hazards.empty else "No hazards",
    ]
    summary_md_path.write_text("\n".join(lines), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
