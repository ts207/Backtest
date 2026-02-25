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
    rolling_z,
    rows_for_event,
    safe_series,
)

EVENT_TYPES = {
    "SPREAD_REGIME_WIDENING_EVENT",
    "SLIPPAGE_SPIKE_EVENT",
    "FEE_REGIME_CHANGE_EVENT",
}

_load_features = load_features


def _fee_series(df: pd.DataFrame) -> pd.Series:
    for col in ("fee_bps_per_side", "taker_fee_bps", "maker_fee_bps", "fee_bps", "fees_bps"):
        s = safe_series(df, col)
        if s.notna().any():
            return s
    return pd.Series(np.nan, index=df.index, dtype=float)


def _spread_z(df: pd.DataFrame) -> pd.Series:
    spread_z = safe_series(df, "spread_zscore")
    if spread_z.notna().any():
        return spread_z
    spread_bps = safe_series(df, "spread_bps")
    if spread_bps.notna().any():
        return rolling_z(spread_bps, 288)
    return pd.Series(np.nan, index=df.index, dtype=float)


def _event_mask(df: pd.DataFrame, event_type: str) -> pd.Series:
    close = safe_series(df, "close")
    high = safe_series(df, "high")
    low = safe_series(df, "low")
    rv_96 = safe_series(df, "rv_96")
    spread_z = _spread_z(df)
    fee = _fee_series(df)

    ret_1 = close.pct_change(1, fill_method=None)
    ret_abs = ret_1.abs()
    bar_range = (high - low) / close.replace(0.0, np.nan)

    spread_hi = past_quantile(spread_z, 0.97)
    spread_shock_hi = past_quantile(spread_z, 0.93)
    ret_hi = past_quantile(ret_abs, 0.90)
    range_hi = past_quantile(bar_range, 0.90)
    rv_hi = past_quantile(rv_96, 0.70)

    if event_type == "SPREAD_REGIME_WIDENING_EVENT":
        threshold = spread_hi.where(spread_hi >= 2.0, 2.0)
        return (
            (spread_z >= threshold).fillna(False)
            & (spread_z.diff() > 0).fillna(False)
        ).fillna(False)

    if event_type == "SLIPPAGE_SPIKE_EVENT":
        return (
            (spread_z >= spread_shock_hi).fillna(False)
            & (ret_abs >= ret_hi).fillna(False)
            & (bar_range >= range_hi).fillna(False)
            & (rv_96 >= rv_hi).fillna(False)
        ).fillna(False)

    if event_type == "FEE_REGIME_CHANGE_EVENT":
        fee_step = fee.diff().abs() > 1e-12
        if fee.notna().any():
            return fee_step.fillna(False)
        ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        return ((ts.dt.day == 1) & (ts.dt.hour == 0) & (ts.dt.minute == 0)).fillna(False)

    return pd.Series(False, index=df.index, dtype=bool)


def _event_score(df: pd.DataFrame, event_type: str) -> pd.Series:
    spread_z = _spread_z(df).abs().fillna(0.0)
    rv_96 = safe_series(df, "rv_96").fillna(0.0)
    if event_type == "FEE_REGIME_CHANGE_EVENT":
        return _fee_series(df).diff().abs().fillna(0.0)
    return spread_z + rv_96


def main() -> int:
    parser = argparse.ArgumentParser(description="Family-specific analyzer for EXECUTION_FRICTION events")
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
        print(f"Unsupported EXECUTION_FRICTION event_type: {event_type}", file=sys.stderr)
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
            min_spacing=3,
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
