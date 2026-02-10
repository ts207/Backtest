from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_ROOT = PROJECT_ROOT.parent / "data"

import sys

sys.path.insert(0, str(PROJECT_ROOT))

from engine.runner import _load_symbol_data, _strategy_returns
from pipelines._lib.io_utils import ensure_dir
from pipelines.features.build_features_v1 import DEFAULT_WINDOWS, _build_features_frame


def _parse_csv(raw: str) -> List[str]:
    return [x.strip() for x in raw.split(",") if x.strip()]


def _timeframe_factor(timeframe: str) -> int:
    if timeframe == "15m":
        return 1
    if timeframe == "1h":
        return 4
    if timeframe == "4h":
        return 16
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def _aggregate_ohlcv_15m_to_tf(bars: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if timeframe == "15m":
        return bars.sort_values("timestamp").reset_index(drop=True)
    out = bars.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True)
    rule = "1h" if timeframe == "1h" else "4h"
    out = out.set_index("timestamp").sort_index()

    agg = pd.DataFrame(index=out.resample(rule, label="right", closed="right").size().index)
    agg["open"] = out["open"].resample(rule, label="right", closed="right").first()
    agg["high"] = out["high"].resample(rule, label="right", closed="right").max()
    agg["low"] = out["low"].resample(rule, label="right", closed="right").min()
    agg["close"] = out["close"].resample(rule, label="right", closed="right").last()
    agg["volume"] = out["volume"].resample(rule, label="right", closed="right").sum() if "volume" in out.columns else 0.0

    if "funding_rate_scaled" in out.columns:
        agg["funding_rate_scaled"] = out["funding_rate_scaled"].resample(rule, label="right", closed="right").last()
    else:
        agg["funding_rate_scaled"] = pd.NA
    if "funding_event_ts" in out.columns:
        agg["funding_event_ts"] = out["funding_event_ts"].resample(rule, label="right", closed="right").last()
    else:
        agg["funding_event_ts"] = pd.NaT

    agg["is_gap"] = False
    agg["gap_len"] = 0
    agg = agg.dropna(subset=["open", "high", "low", "close"]).reset_index()
    return agg


def _build_features_for_timeframe(bars: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    factor = _timeframe_factor(timeframe)
    windows = {
        "rv": max(2, int(DEFAULT_WINDOWS["rv"] / factor)),
        "rv_pct": max(20, int(DEFAULT_WINDOWS["rv_pct"] / factor)),
        "range": max(2, int(DEFAULT_WINDOWS["range"] / factor)),
        "range_med": max(10, int(DEFAULT_WINDOWS["range_med"] / factor)),
    }
    features, _segment, _nan_rates, col_map = _build_features_frame(bars, windows=windows)
    rv_col = col_map["rv_pct"]
    range_med_col = col_map["range_med"]
    features = features.rename(columns={rv_col: "rv_pct_2880", range_med_col: "range_med_480"})
    return features


def _aggregation_identity_diagnostics(bars_15m: pd.DataFrame, bars_tf: pd.DataFrame, timeframe: str) -> Dict[str, float]:
    if timeframe == "15m":
        return {
            "timeframe": timeframe,
            "checked_bars": int(len(bars_tf)),
            "open_mismatch_ratio": 0.0,
            "close_mismatch_ratio": 0.0,
            "high_mismatch_ratio": 0.0,
            "low_mismatch_ratio": 0.0,
            "volume_mismatch_ratio": 0.0,
        }

    src = bars_15m.copy()
    src["timestamp"] = pd.to_datetime(src["timestamp"], utc=True)
    src = src.set_index("timestamp").sort_index()
    rule = "1h" if timeframe == "1h" else "4h"

    first_open = src["open"].resample(rule, label="right", closed="right").first()
    last_close = src["close"].resample(rule, label="right", closed="right").last()
    max_high = src["high"].resample(rule, label="right", closed="right").max()
    min_low = src["low"].resample(rule, label="right", closed="right").min()
    sum_volume = src["volume"].resample(rule, label="right", closed="right").sum() if "volume" in src.columns else pd.Series(dtype=float)

    tf = bars_tf.copy()
    tf["timestamp"] = pd.to_datetime(tf["timestamp"], utc=True)
    tf = tf.set_index("timestamp").sort_index()

    aligned = pd.DataFrame(index=tf.index)
    aligned["open_expected"] = first_open.reindex(tf.index)
    aligned["close_expected"] = last_close.reindex(tf.index)
    aligned["high_expected"] = max_high.reindex(tf.index)
    aligned["low_expected"] = min_low.reindex(tf.index)
    aligned["volume_expected"] = sum_volume.reindex(tf.index) if not sum_volume.empty else np.nan

    eps = 1e-12
    checked = max(1, len(aligned))
    open_mismatch = float(((tf["open"] - aligned["open_expected"]).abs() > eps).mean())
    close_mismatch = float(((tf["close"] - aligned["close_expected"]).abs() > eps).mean())
    high_mismatch = float(((tf["high"] - aligned["high_expected"]).abs() > eps).mean())
    low_mismatch = float(((tf["low"] - aligned["low_expected"]).abs() > eps).mean())
    if "volume" in tf.columns and not aligned["volume_expected"].isna().all():
        vol_mismatch = float(((tf["volume"] - aligned["volume_expected"]).abs() > eps).mean())
    else:
        vol_mismatch = 0.0

    return {
        "timeframe": timeframe,
        "checked_bars": checked,
        "open_mismatch_ratio": open_mismatch,
        "close_mismatch_ratio": close_mismatch,
        "high_mismatch_ratio": high_mismatch,
        "low_mismatch_ratio": low_mismatch,
        "volume_mismatch_ratio": vol_mismatch,
    }


def _feature_semantic_equivalence_diagnostics(bars_15m: pd.DataFrame, timeframe: str) -> Dict[str, float]:
    if timeframe == "15m":
        return {
            "timeframe": timeframe,
            "rv_pct_2880_corr": 1.0,
            "range_med_480_corr": 1.0,
            "samples": int(len(bars_15m)),
        }

    bars_tf = _aggregate_ohlcv_15m_to_tf(bars_15m, timeframe)
    features_a = _build_features_for_timeframe(bars_tf, timeframe).copy()
    features_b_15m = _build_features_for_timeframe(bars_15m, "15m").copy()

    features_a["timestamp"] = pd.to_datetime(features_a["timestamp"], utc=True)
    features_b_15m["timestamp"] = pd.to_datetime(features_b_15m["timestamp"], utc=True)

    sampled_b = pd.merge_asof(
        features_a[["timestamp"]].sort_values("timestamp"),
        features_b_15m[["timestamp", "rv_pct_2880", "range_med_480"]].sort_values("timestamp"),
        on="timestamp",
        direction="backward",
    )

    merged = features_a[["timestamp", "rv_pct_2880", "range_med_480"]].merge(
        sampled_b,
        on="timestamp",
        suffixes=("_agg", "_sampled"),
        how="inner",
    )

    rv = merged[["rv_pct_2880_agg", "rv_pct_2880_sampled"]].dropna()
    rmed = merged[["range_med_480_agg", "range_med_480_sampled"]].dropna()

    def _corr(df: pd.DataFrame, left: str, right: str) -> float:
        if len(df) < 3:
            return 0.0
        if float(df[left].std()) == 0.0 or float(df[right].std()) == 0.0:
            return 0.0
        val = float(df[left].corr(df[right]))
        return 0.0 if np.isnan(val) else val

    return {
        "timeframe": timeframe,
        "rv_pct_2880_corr": _corr(rv, "rv_pct_2880_agg", "rv_pct_2880_sampled"),
        "range_med_480_corr": _corr(rmed, "range_med_480_agg", "range_med_480_sampled"),
        "samples": int(len(merged)),
    }


def _weighted_mean(symbol_rows: List[Dict[str, float]], key: str) -> float:
    total_bars = float(sum(row["total_bars"] for row in symbol_rows))
    if total_bars <= 0:
        return 0.0
    return float(sum(row[key] * row["total_bars"] for row in symbol_rows) / total_bars)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate gross/cost feasibility report across strategies")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", default="BTCUSDT")
    parser.add_argument("--strategies", default="tsmom_v1")
    parser.add_argument("--timeframe", choices=["15m", "1h", "4h"], default="15m")
    parser.add_argument("--cost_bps", type=float, default=6.0)
    parser.add_argument("--spread_bps", type=float, default=2.0)
    parser.add_argument("--execution_delay_bars", type=int, default=0)
    parser.add_argument("--execution_min_hold_bars", type=int, default=0)
    parser.add_argument("--execution_decision_grid_hours", type=int, default=0)
    parser.add_argument("--execution_decision_grid_bars", type=int, default=0)
    parser.add_argument("--execution_decision_grid_offset_bars", type=int, default=0)
    parser.add_argument("--data_root", default=str(DEFAULT_DATA_ROOT))
    args = parser.parse_args()

    symbols = _parse_csv(args.symbols)
    strategies = _parse_csv(args.strategies)
    data_root = Path(args.data_root)

    params = {
        "execution_delay_bars": int(args.execution_delay_bars),
        "execution_min_hold_bars": int(args.execution_min_hold_bars),
        "execution_spread_bps": float(args.spread_bps),
        "execution_decision_grid_hours": int(args.execution_decision_grid_hours),
        "execution_decision_grid_bars": int(args.execution_decision_grid_bars),
        "execution_decision_grid_offset_bars": int(args.execution_decision_grid_offset_bars),
    }

    rows = []
    diagnostics_rows: List[Dict[str, object]] = []
    for strategy_name in strategies:
        per_symbol: List[Dict[str, float]] = []
        for symbol in symbols:
            bars_15m, _features = _load_symbol_data(data_root, symbol, run_id=args.run_id)
            bars_tf = _aggregate_ohlcv_15m_to_tf(bars_15m, args.timeframe)
            features_tf = _build_features_for_timeframe(bars_tf, args.timeframe)
            result = _strategy_returns(symbol, bars_tf, features_tf, strategy_name, params, float(args.cost_bps))
            d = result.diagnostics
            per_symbol.append(
                {
                    "total_bars": float(d.get("total_bars", 0.0)),
                    "gross_pnl": float(d.get("gross_pnl", 0.0)),
                    "base_cost_paid": float(d.get("base_cost_paid", 0.0)),
                    "spread_cost_paid": float(d.get("spread_cost_paid", 0.0)),
                    "cost_paid": float(d.get("cost_paid", 0.0)),
                    "net_pnl": float(d.get("net_pnl", 0.0)),
                    "avg_turnover_per_bar": float(d.get("avg_turnover_per_bar", 0.0)),
                    "avg_abs_position": float(d.get("avg_abs_position", 0.0)),
                    "nonzero_position_pct": float(d.get("nonzero_position_pct", 0.0)),
                    "num_trades": float(d.get("num_trades", 0.0)),
                    "avg_holding_bars": float(d.get("avg_holding_bars", 0.0)),
                }
            )

            diagnostics_rows.append(
                {
                    "strategy": strategy_name,
                    "symbol": symbol,
                    "timeframe": args.timeframe,
                    **_aggregation_identity_diagnostics(bars_15m, bars_tf, args.timeframe),
                    **_feature_semantic_equivalence_diagnostics(bars_15m, args.timeframe),
                }
            )

        gross_pnl = float(sum(x["gross_pnl"] for x in per_symbol))
        base_cost_paid = float(sum(x["base_cost_paid"] for x in per_symbol))
        spread_cost_paid = float(sum(x["spread_cost_paid"] for x in per_symbol))
        cost_paid = float(sum(x["cost_paid"] for x in per_symbol))
        net_total_return = float(sum(x["net_pnl"] for x in per_symbol))
        effective_bps = float(args.cost_bps) + max(0.0, float(args.spread_bps))
        turnover_units = cost_paid / (effective_bps / 10000.0) if effective_bps > 0 else 0.0
        breakeven_bps = 10000.0 * gross_pnl / turnover_units if turnover_units > 0 else 0.0

        rows.append(
            {
                "strategy": strategy_name,
                "timeframe": args.timeframe,
                "execution_decision_grid_hours": int(args.execution_decision_grid_hours),
                "execution_decision_grid_bars": int(args.execution_decision_grid_bars),
                "execution_decision_grid_offset_bars": int(args.execution_decision_grid_offset_bars),
                "gross_pnl": gross_pnl,
                "base_cost_paid": base_cost_paid,
                "spread_cost_paid": spread_cost_paid,
                "cost_paid": cost_paid,
                "net_total_return": net_total_return,
                "cost_bps_effective": effective_bps,
                "turnover_units": turnover_units,
                "breakeven_bps": breakeven_bps,
                "avg_turnover_per_bar": _weighted_mean(per_symbol, "avg_turnover_per_bar"),
                "avg_abs_position": _weighted_mean(per_symbol, "avg_abs_position"),
                "nonzero_position_pct": _weighted_mean(per_symbol, "nonzero_position_pct"),
                "num_trades": _weighted_mean(per_symbol, "num_trades"),
                "avg_holding_bars": _weighted_mean(per_symbol, "avg_holding_bars"),
            }
        )

    out_dir = data_root / "runs" / args.run_id / "research"
    ensure_dir(out_dir)
    frame = pd.DataFrame(rows).sort_values("strategy").reset_index(drop=True)

    grid_bars = int(args.execution_decision_grid_bars)
    grid_hours = int(args.execution_decision_grid_hours)
    if grid_bars > 0:
        suffix = f"{args.timeframe}_grid{grid_bars}bars_off{int(args.execution_decision_grid_offset_bars)}"
    elif grid_hours > 0:
        suffix = f"{args.timeframe}_grid{grid_hours}h_off{int(args.execution_decision_grid_offset_bars)}"
    else:
        suffix = args.timeframe
    csv_path = out_dir / f"strategies_cost_report_{suffix}.csv"
    json_path = out_dir / f"strategies_cost_report_{suffix}.json"
    diagnostics_path = out_dir / f"strategies_cost_report_diagnostics_{suffix}.json"
    frame.to_csv(csv_path, index=False)
    json_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    diagnostics_path.write_text(json.dumps(diagnostics_rows, indent=2), encoding="utf-8")

    print(frame.to_string(index=False))
    print(f"\nWrote: {csv_path}")
    print(f"Wrote: {json_path}")
    print(f"Wrote: {diagnostics_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
