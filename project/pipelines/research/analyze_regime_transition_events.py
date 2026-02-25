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
    "VOL_REGIME_SHIFT_EVENT",
    "TREND_TO_CHOP_SHIFT",
    "CHOP_TO_TREND_SHIFT",
    "CORRELATION_BREAKDOWN_EVENT",
    "BETA_SPIKE_EVENT",
}

_load_features = load_features


def _event_mask(df: pd.DataFrame, event_type: str) -> pd.Series:
    close = safe_series(df, "close")
    rv_96 = safe_series(df, "rv_96")
    spread_z = safe_series(df, "spread_zscore")
    basis_z = safe_series(df, "basis_zscore")
    if basis_z.isna().all():
        basis_z = safe_series(df, "cross_exchange_spread_z")

    ret_1 = close.pct_change(1, fill_method=None)
    ret_abs = ret_1.abs()
    trend_96 = close.pct_change(96, fill_method=None)
    trend_abs = trend_96.abs()
    basis_abs = basis_z.abs()
    spread_abs = spread_z.abs()

    trend_hi = past_quantile(trend_abs, 0.70)
    trend_lo = past_quantile(trend_abs, 0.35)
    rv_hi = past_quantile(rv_96, 0.70)
    rv_lo = past_quantile(rv_96, 0.35)
    rv_low_th = past_quantile(rv_96, 0.33)
    rv_high_th = past_quantile(rv_96, 0.66)
    basis_hi = past_quantile(basis_abs, 0.92)
    basis_med_hi = past_quantile(basis_abs, 0.85)
    spread_hi = past_quantile(spread_abs, 0.80)
    ret_tail = past_quantile(ret_abs, 0.99)

    if event_type == "VOL_REGIME_SHIFT_EVENT":
        up_shift = (rv_96 >= rv_high_th).fillna(False) & (rv_96.shift(1) < rv_high_th.shift(1)).fillna(False)
        down_shift = (rv_96 <= rv_low_th).fillna(False) & (rv_96.shift(1) > rv_low_th.shift(1)).fillna(False)
        return (up_shift | down_shift).fillna(False)

    if event_type == "TREND_TO_CHOP_SHIFT":
        return (
            (trend_abs.shift(1) >= trend_hi.shift(1)).fillna(False)
            & (trend_abs <= trend_lo).fillna(False)
            & (rv_96 <= rv_lo).fillna(False)
        ).fillna(False)

    if event_type == "CHOP_TO_TREND_SHIFT":
        return (
            (trend_abs.shift(1) <= trend_lo.shift(1)).fillna(False)
            & (trend_abs >= trend_hi).fillna(False)
            & (rv_96 >= rv_hi).fillna(False)
        ).fillna(False)

    if event_type == "CORRELATION_BREAKDOWN_EVENT":
        desync = (basis_abs >= basis_hi).fillna(False) & (spread_abs >= spread_hi).fillna(False)
        direction_conflict = ((ret_1 * basis_z) < 0).fillna(False)
        return (desync & direction_conflict).fillna(False)

    if event_type == "BETA_SPIKE_EVENT":
        return (
            (ret_abs >= ret_tail).fillna(False)
            & (basis_abs >= basis_med_hi).fillna(False)
            & (rv_96 >= rv_hi).fillna(False)
        ).fillna(False)

    return pd.Series(False, index=df.index, dtype=bool)


def _event_score(df: pd.DataFrame, event_type: str) -> pd.Series:
    close = safe_series(df, "close")
    rv_96 = safe_series(df, "rv_96").fillna(0.0)
    ret_abs = close.pct_change(1, fill_method=None).abs().fillna(0.0)
    basis = safe_series(df, "basis_zscore")
    if basis.isna().all():
        basis = safe_series(df, "cross_exchange_spread_z")
    basis_abs = basis.abs().fillna(0.0)
    if event_type == "CORRELATION_BREAKDOWN_EVENT":
        return basis_abs + safe_series(df, "spread_zscore").abs().fillna(0.0)
    if event_type == "BETA_SPIKE_EVENT":
        return ret_abs + basis_abs + rv_96
    return rv_96 + close.pct_change(96, fill_method=None).abs().fillna(0.0)


def main() -> int:
    parser = argparse.ArgumentParser(description="Family-specific analyzer for REGIME_TRANSITION events")
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
        print(f"Unsupported REGIME_TRANSITION event_type: {event_type}", file=sys.stderr)
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
