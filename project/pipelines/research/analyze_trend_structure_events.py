from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from events.registry import EVENT_REGISTRY_SPECS
from pipelines.research._family_event_utils import (
    EVENT_COLUMNS,
    load_features,
    merge_event_csv,
    past_quantile,
    rows_for_event,
    safe_series,
)

EVENT_TYPES = {
    "RANGE_BREAKOUT",
    "FALSE_BREAKOUT",
    "TREND_ACCELERATION",
    "TREND_DECELERATION",
    "PULLBACK_PIVOT",
    "SUPPORT_RESISTANCE_BREAK",
}

_load_features = load_features


def _event_mask(df: pd.DataFrame, event_type: str) -> pd.Series:
    close = safe_series(df, "close")
    high = safe_series(df, "high")
    low = safe_series(df, "low")
    rv_96 = safe_series(df, "rv_96")

    ret_1 = close.pct_change(1, fill_method=None)
    ret_abs = ret_1.abs()
    trend_96 = close.pct_change(96, fill_method=None)
    trend_abs = trend_96.abs()
    trend_delta = trend_abs.diff(3)
    bar_range = (high - low) / close.replace(0.0, np.nan)

    high_96 = safe_series(df, "high_96")
    low_96 = safe_series(df, "low_96")
    if high_96.isna().all() or low_96.isna().all():
        high_96 = high.rolling(96, min_periods=24).max()
        low_96 = low.rolling(96, min_periods=24).min()

    prior_high_96 = high_96.shift(1)
    prior_low_96 = low_96.shift(1)
    breakout_up = (close > prior_high_96).fillna(False)
    breakout_down = (close < prior_low_96).fillna(False)
    breakout = (breakout_up | breakout_down).fillna(False)

    rv_q60 = past_quantile(rv_96, 0.60)
    rv_q50 = past_quantile(rv_96, 0.50)
    ret_q40 = past_quantile(ret_abs, 0.40)
    ret_q75 = past_quantile(ret_abs, 0.75)
    trend_q70 = past_quantile(trend_abs, 0.70)
    accel_q90 = past_quantile(trend_delta, 0.90)
    decel_q10 = past_quantile(trend_delta, 0.10)
    range_q80 = past_quantile(bar_range, 0.80)

    if event_type == "RANGE_BREAKOUT":
        return (breakout & (rv_96 >= rv_q50).fillna(False)).fillna(False)

    if event_type == "FALSE_BREAKOUT":
        recent_break = breakout.rolling(window=3, min_periods=1).max().shift(1).fillna(0).astype(bool)
        inside_range = (close <= prior_high_96).fillna(False) & (close >= prior_low_96).fillna(False)
        pullback_strength = (ret_abs >= ret_q40).fillna(False)
        return (recent_break & inside_range & pullback_strength).fillna(False)

    if event_type == "TREND_ACCELERATION":
        direction_consistent = (np.sign(ret_1.rolling(window=3, min_periods=1).mean()) == np.sign(trend_96)).fillna(False)
        return (
            (trend_abs >= trend_q70).fillna(False)
            & (trend_delta >= accel_q90).fillna(False)
            & direction_consistent
        ).fillna(False)

    if event_type == "TREND_DECELERATION":
        return (
            (trend_abs.shift(1) >= trend_q70.shift(1)).fillna(False)
            & (trend_delta <= decel_q10).fillna(False)
            & ((ret_1 * trend_96.shift(1)) <= 0).fillna(False)
        ).fillna(False)

    if event_type == "PULLBACK_PIVOT":
        pullback = ((ret_1 * trend_96.shift(1)) < 0).fillna(False)
        ret_window = (ret_abs >= ret_q40).fillna(False) & (ret_abs <= ret_q75).fillna(False)
        controlled_bar = (bar_range <= range_q80).fillna(False)
        return (
            (trend_abs.shift(1) >= trend_q70.shift(1)).fillna(False)
            & pullback
            & ret_window
            & controlled_bar
        ).fillna(False)

    if event_type == "SUPPORT_RESISTANCE_BREAK":
        long_hi = high.rolling(window=288, min_periods=96).max().shift(1)
        long_lo = low.rolling(window=288, min_periods=96).min().shift(1)
        break_long = ((close > long_hi) | (close < long_lo)).fillna(False)
        return (break_long & (rv_96 >= rv_q60).fillna(False)).fillna(False)

    return pd.Series(False, index=df.index, dtype=bool)


def _event_score(df: pd.DataFrame, event_type: str) -> pd.Series:
    close = safe_series(df, "close")
    trend_96 = close.pct_change(96, fill_method=None).abs()
    rv_96 = safe_series(df, "rv_96")
    if event_type in {"RANGE_BREAKOUT", "SUPPORT_RESISTANCE_BREAK"}:
        return trend_96.fillna(0.0) + rv_96.fillna(0.0)
    return trend_96.fillna(0.0)


def main() -> int:
    parser = argparse.ArgumentParser(description="Family-specific analyzer for TREND_STRUCTURE events")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--event_type", required=True)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--seed", type=int, default=13)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    event_type = str(args.event_type).strip().upper()
    if event_type not in EVENT_TYPES:
        print(f"Unsupported TREND_STRUCTURE event_type: {event_type}", file=sys.stderr)
        return 1
    spec = EVENT_REGISTRY_SPECS.get(event_type)
    if spec is None:
        print(f"Unknown event_type: {event_type}", file=sys.stderr)
        return 1

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / spec.reports_dir / args.run_id
    out_path = out_dir / spec.events_file

    events_parts: List[pd.DataFrame] = []
    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    for symbol in symbols:
        features = _load_features(run_id=str(args.run_id), symbol=symbol, timeframe=str(args.timeframe))
        if features.empty:
            continue
        mask = _event_mask(features, event_type=event_type)
        part = rows_for_event(
            features,
            symbol=symbol,
            event_type=event_type,
            mask=mask,
            event_score=_event_score(features, event_type=event_type),
            min_spacing=6,
        )
        if not part.empty:
            events_parts.append(part)

    new_df = pd.concat(events_parts, ignore_index=True) if events_parts else pd.DataFrame(columns=EVENT_COLUMNS)
    new_df = merge_event_csv(out_path, event_type=event_type, new_df=new_df)

    summary = {
        "run_id": str(args.run_id),
        "event_type": event_type,
        "rows": int(len(new_df[new_df["event_type"].astype(str) == event_type])) if not new_df.empty else 0,
        "events_file": str(out_path),
    }
    (out_dir / f"{event_type.lower()}_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote {summary['rows']} rows for {event_type} to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
