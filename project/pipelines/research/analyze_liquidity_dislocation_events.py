from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List

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
    "DEPTH_COLLAPSE",
    "SPREAD_BLOWOUT",
    "ORDERFLOW_IMBALANCE_SHOCK",
    "SWEEP_STOPRUN",
    "ABSORPTION_EVENT",
    "LIQUIDITY_GAP_PRINT",
}

_load_features = load_features


def _event_mask(df: pd.DataFrame, event_type: str) -> pd.Series:
    close = safe_series(df, "close")
    high = safe_series(df, "high")
    low = safe_series(df, "low")
    rv_96 = safe_series(df, "rv_96")
    spread_z = safe_series(df, "spread_zscore")
    rv_z = rolling_z(rv_96.ffill(), 288)
    ret_1 = close.pct_change(1, fill_method=None)
    ret_abs = ret_1.abs()

    spread_q80 = past_quantile(spread_z, 0.80)
    spread_q90 = past_quantile(spread_z, 0.90)
    spread_q97 = past_quantile(spread_z, 0.97)
    rv_q70 = past_quantile(rv_z, 0.70)
    ret_q35 = past_quantile(ret_abs, 0.35)
    ret_q90 = past_quantile(ret_abs, 0.90)
    ret_q99 = past_quantile(ret_abs, 0.99)
    ret_q995 = past_quantile(ret_abs, 0.995)

    if event_type == "DEPTH_COLLAPSE":
        return ((spread_z >= spread_q90).fillna(False) & (rv_z >= rv_q70).fillna(False)).fillna(False)
    if event_type == "SPREAD_BLOWOUT":
        threshold = spread_q97.where(spread_q97 >= 2.0, 2.0)
        return (spread_z >= threshold).fillna(False)
    if event_type == "ORDERFLOW_IMBALANCE_SHOCK":
        return ((ret_abs >= ret_q99).fillna(False) & (rv_z >= rv_q70).fillna(False)).fillna(False)
    if event_type == "SWEEP_STOPRUN":
        wick = ((high - close).abs() + (close - low).abs()) / close.replace(0.0, pd.NA)
        wick_q97 = past_quantile(wick.astype(float), 0.97)
        return ((wick >= wick_q97).fillna(False) & (ret_abs >= ret_q90).fillna(False)).fillna(False)
    if event_type == "ABSORPTION_EVENT":
        return ((ret_abs <= ret_q35).fillna(False) & (spread_z >= spread_q80).fillna(False)).fillna(False)
    if event_type == "LIQUIDITY_GAP_PRINT":
        return (ret_abs >= ret_q995).fillna(False)
    return pd.Series(False, index=df.index, dtype=bool)


def _event_score(df: pd.DataFrame) -> pd.Series:
    return safe_series(df, "spread_zscore").abs().fillna(0.0) + safe_series(df, "rv_96").fillna(0.0)


def main() -> int:
    parser = argparse.ArgumentParser(description="Family-specific analyzer for LIQUIDITY_DISLOCATION events")
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
        print(f"Unsupported LIQUIDITY_DISLOCATION event_type: {event_type}", file=sys.stderr)
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
            event_score=_event_score(features),
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
