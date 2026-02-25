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
    rows_for_event,
)

EVENT_TYPES = {
    "SESSION_OPEN_EVENT",
    "SESSION_CLOSE_EVENT",
    "FUNDING_TIMESTAMP_EVENT",
    "SCHEDULED_NEWS_WINDOW_EVENT",
}

_load_features = load_features


def _event_mask(df: pd.DataFrame, event_type: str) -> pd.Series:
    ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    if event_type == "SESSION_OPEN_EVENT":
        return ((ts.dt.minute == 0) & ts.dt.hour.isin([0, 8, 13])).fillna(False)
    if event_type == "SESSION_CLOSE_EVENT":
        return ((ts.dt.minute >= 55) & ts.dt.hour.isin([7, 12, 23])).fillna(False)
    if event_type == "FUNDING_TIMESTAMP_EVENT":
        return ((ts.dt.minute == 0) & ts.dt.hour.isin([0, 8, 16])).fillna(False)
    if event_type == "SCHEDULED_NEWS_WINDOW_EVENT":
        return (ts.dt.hour.isin([12, 13, 14, 18]) & ts.dt.minute.between(25, 35)).fillna(False)
    return pd.Series(False, index=df.index, dtype=bool)


def _event_score(df: pd.DataFrame) -> pd.Series:
    return pd.Series(1.0, index=df.index, dtype=float)


def main() -> int:
    parser = argparse.ArgumentParser(description="Family-specific analyzer for TEMPORAL_STRUCTURE events")
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
        print(f"Unsupported TEMPORAL_STRUCTURE event_type: {event_type}", file=sys.stderr)
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
            min_spacing=1,
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
