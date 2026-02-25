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
    "FUNDING_FLIP",
    "DELEVERAGING_WAVE",
}

_load_features = load_features


def _event_mask(df: pd.DataFrame, event_type: str) -> pd.Series:
    funding = safe_series(df, "funding_rate_scaled")
    funding_abs = funding.abs()
    oi_delta_1h = safe_series(df, "oi_delta_1h")
    rv_96 = safe_series(df, "rv_96")
    rv_z = rolling_z(rv_96.ffill(), 288)

    if event_type == "FUNDING_FLIP":
        funding_q60 = past_quantile(funding_abs, 0.60)
        return ((np.sign(funding) != np.sign(funding.shift(1))) & (funding_abs >= funding_q60)).fillna(False)

    if event_type == "DELEVERAGING_WAVE":
        oi_q05 = past_quantile(oi_delta_1h, 0.05)
        rv_q70 = past_quantile(rv_z, 0.70)
        return ((oi_delta_1h <= oi_q05).fillna(False) & (rv_z >= rv_q70).fillna(False)).fillna(False)

    return pd.Series(False, index=df.index, dtype=bool)


def _event_score(df: pd.DataFrame, event_type: str) -> pd.Series:
    if event_type == "FUNDING_FLIP":
        return safe_series(df, "funding_rate_scaled").abs().fillna(0.0)
    return rolling_z(safe_series(df, "rv_96").ffill(), 288).abs().fillna(0.0)


def main() -> int:
    parser = argparse.ArgumentParser(description="Family-specific analyzer for POSITIONING_EXTREMES events")
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
        print(f"Unsupported POSITIONING_EXTREMES event_type: {event_type}", file=sys.stderr)
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
