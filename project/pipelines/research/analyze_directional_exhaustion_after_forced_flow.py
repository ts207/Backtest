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
    frame["ret_1"] = close.pct_change(fill_method=None)
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


def _event_metrics(core: pd.DataFrame, start_idx: int, end_idx: int, tail_threshold: float) -> Dict[str, float]:
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
                t_tail = float(k - start_idx + 1)
                break

    return {
        "realized_vol_mean": float(win["rv"].mean()) if win["rv"].notna().any() else np.nan,
        "range_expansion": range_expansion,
        "tail_move_probability": float((win["tail_move"] >= tail_threshold).mean()) if np.isfinite(tail_threshold) else np.nan,
        "tail_move_within": int(np.isfinite(t_tail)),
        "time_to_tail_move": t_tail,
    }


def _anchor_candidates(core: pd.DataFrame, anchor_mode: str, anchor_quantile: float) -> pd.DataFrame:
    if anchor_mode == "taker_imbalance":
        if "taker_base_volume" in core.columns and "volume" in core.columns:
            imbalance = core["taker_base_volume"].astype(float) / core["volume"].replace(0.0, np.nan)
            hi = imbalance.quantile(anchor_quantile)
            lo = imbalance.quantile(1.0 - anchor_quantile)
            anchors = core.assign(anchor_score=imbalance, direction=np.where(imbalance >= hi, 1, np.where(imbalance <= lo, -1, 0)))
            return anchors[anchors["direction"] != 0]

        if "tail_move" in core.columns and "ret_1" in core.columns:
            threshold = core["tail_move"].quantile(anchor_quantile)
            anchors = core.assign(anchor_score=core["tail_move"], direction=np.sign(core["ret_1"].fillna(0.0)))
            anchors = anchors[anchors["anchor_score"] >= threshold]
            return anchors[anchors["direction"] != 0]
        return pd.DataFrame()

    if anchor_mode == "liquidation_cluster":
        for col in ("liquidation_volume", "liq_volume", "liquidation_notional"):
            if col in core.columns:
                metric = core[col].astype(float)
                threshold = metric.quantile(anchor_quantile)
                anchors = core.assign(anchor_score=metric, direction=np.sign(core["ret_1"].fillna(0.0)))
                anchors = anchors[anchors["anchor_score"] >= threshold]
                return anchors[anchors["direction"] != 0]
        return pd.DataFrame()

    return pd.DataFrame()


def _build_event_and_control_rows(
    symbol: str,
    core: pd.DataFrame,
    window_end: int,
    anchor_mode: str,
    anchor_quantile: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if core.empty:
        return pd.DataFrame(), pd.DataFrame()

    tail_threshold = float(core["tail_move"].quantile(0.95)) if core["tail_move"].notna().any() else np.nan
    anchors = _anchor_candidates(core, anchor_mode, anchor_quantile)
    if anchors.empty:
        return pd.DataFrame(), pd.DataFrame()

    rows_event: List[Dict[str, object]] = []
    rows_ctrl: List[Dict[str, object]] = []

    non_event_pool = core.copy()
    non_event_pool = non_event_pool.assign(rv_rank=non_event_pool["rv"].rank(pct=True)) if not non_event_pool.empty else non_event_pool

    seen_until = -1
    for idx, row in anchors.iterrows():
        if idx <= seen_until:
            continue
        end_idx = min(len(core) - 1, idx + window_end)
        seen_until = end_idx

        direction = int(row.get("direction", 0))
        if direction == 0:
            continue
        anchor_high = float(core["high"].iat[idx])
        anchor_low = float(core["low"].iat[idx])
        window = core.iloc[idx : end_idx + 1]
        if direction > 0:
            failed_new_extreme = float(window["high"].max()) <= anchor_high
        else:
            failed_new_extreme = float(window["low"].min()) >= anchor_low
        if not failed_new_extreme:
            continue

        metrics = _event_metrics(core, idx, end_idx, tail_threshold)
        if not metrics:
            continue
        event_id = f"deff_{symbol}_{idx:06d}"
        rv_rank = float(core["rv"].rank(pct=True).iat[idx]) if pd.notna(core["rv"].iat[idx]) else np.nan
        rows_event.append(
            {
                "event_type": "directional_exhaustion_after_forced_flow",
                "event_id": event_id,
                "symbol": symbol,
                "anchor_ts": core["timestamp"].iat[idx],
                "start_idx": idx,
                "end_idx": end_idx,
                "year": int(pd.Timestamp(core["timestamp"].iat[idx]).year),
                "anchor_mode": anchor_mode,
                "anchor_quantile": anchor_quantile,
                "direction": direction,
                "rv_rank": rv_rank,
                **metrics,
            }
        )

        pool = non_event_pool
        if np.isfinite(rv_rank):
            pool = pool.assign(match_dist=(pool["rv_rank"] - rv_rank).abs()).nsmallest(200, "match_dist")
        if pool.empty:
            continue
        pick = pool.sample(n=1, random_state=idx).iloc[0]
        c_idx = int(pick.name)
        c_end = min(len(core) - 1, c_idx + window_end)
        c_metrics = _event_metrics(core, c_idx, c_end, tail_threshold)
        if not c_metrics:
            continue
        rows_ctrl.append(
            {
                "event_id": event_id,
                "symbol": symbol,
                "start_idx": c_idx,
                "end_idx": c_end,
                "rv_rank": float(pick["rv_rank"]) if pd.notna(pick["rv_rank"]) else np.nan,
                **c_metrics,
            }
        )

    return pd.DataFrame(rows_event), pd.DataFrame(rows_ctrl)


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase-1 analyzer for directional exhaustion after forced flow (structure only)")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--window_end", type=int, default=32)
    parser.add_argument("--anchor_mode", default="taker_imbalance", choices=["taker_imbalance", "liquidation_cluster"])
    parser.add_argument("--anchor_quantile", type=float, default=0.99)
    parser.add_argument("--out_dir", default=None)
    args = parser.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "directional_exhaustion_after_forced_flow" / args.run_id
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
            anchor_mode=args.anchor_mode,
            anchor_quantile=args.anchor_quantile,
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

    events_path = out_dir / "directional_exhaustion_after_forced_flow_events.csv"
    controls_path = out_dir / "directional_exhaustion_after_forced_flow_controls.csv"
    deltas_path = out_dir / "directional_exhaustion_after_forced_flow_matched_deltas.csv"
    hazards_path = out_dir / "directional_exhaustion_after_forced_flow_hazards.csv"
    phase_path = out_dir / "directional_exhaustion_after_forced_flow_phase_stability.csv"
    sign_path = out_dir / "directional_exhaustion_after_forced_flow_sign_stability.csv"
    summary_md_path = out_dir / "directional_exhaustion_after_forced_flow_summary.md"
    summary_json_path = out_dir / "directional_exhaustion_after_forced_flow_summary.json"

    events.to_csv(events_path, index=False)
    controls.to_csv(controls_path, index=False)
    deltas.to_csv(deltas_path, index=False)
    hazards.to_csv(hazards_path, index=False)
    phase.to_csv(phase_path, index=False)
    sign.to_csv(sign_path, index=False)

    summary = {
        "event_type": "directional_exhaustion_after_forced_flow",
        "phase": 1,
        "window": {"x": 0, "y": args.window_end},
        "anchor_mode": args.anchor_mode,
        "anchor_quantile": args.anchor_quantile,
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
        "# Directional Exhaustion After Forced Flow (Phase 1)",
        "",
        f"Run ID: `{args.run_id}`",
        f"Window: [0, {args.window_end}]",
        f"Anchor mode: {args.anchor_mode}",
        f"Anchor quantile: {args.anchor_quantile}",
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
