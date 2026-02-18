from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from events.registry import (
    EVENT_REGISTRY_SPECS,
    build_event_flags,
    collect_registry_events,
    write_event_registry_artifacts,
)
from pipelines._lib.run_manifest import finalize_manifest, start_manifest


def _parse_symbols(symbols_csv: str) -> List[str]:
    symbols = [s.strip().upper() for s in str(symbols_csv).split(",") if s.strip()]
    return list(dict.fromkeys(symbols))


def main() -> int:
    parser = argparse.ArgumentParser(description="Build canonical event registry artifacts from phase1 outputs")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--event_type", default="all", choices=["all", *sorted(EVENT_REGISTRY_SPECS.keys())])
    parser.add_argument("--timeframe", default="15m")
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    symbols = _parse_symbols(args.symbols)
    selected_event_types = sorted(EVENT_REGISTRY_SPECS.keys()) if args.event_type == "all" else [str(args.event_type)]

    params = {
        "run_id": args.run_id,
        "symbols": symbols,
        "event_type": args.event_type,
        "timeframe": str(args.timeframe),
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("build_event_registry", args.run_id, params, inputs, outputs)

    try:
        for event_type in selected_event_types:
            spec = EVENT_REGISTRY_SPECS[event_type]
            src = DATA_ROOT / "reports" / spec.reports_dir / args.run_id / spec.events_file
            inputs.append({"path": str(src), "rows": None, "start_ts": None, "end_ts": None})

        events = collect_registry_events(
            data_root=DATA_ROOT,
            run_id=args.run_id,
            event_types=selected_event_types,
        )
        flags = build_event_flags(
            events=events,
            symbols=symbols,
            data_root=DATA_ROOT,
            run_id=args.run_id,
            timeframe=str(args.timeframe),
        )
        paths = write_event_registry_artifacts(
            data_root=DATA_ROOT,
            run_id=args.run_id,
            events=events,
            event_flags=flags,
        )

        if events.empty:
            per_family_counts: Dict[str, int] = {event_type: 0 for event_type in selected_event_types}
        else:
            per_family_counts = {
                str(event_type): int(count)
                for event_type, count in events.groupby("event_type", sort=True).size().to_dict().items()
            }
        summary = {
            "run_id": args.run_id,
            "selected_event_types": selected_event_types,
            "event_rows": int(len(events)),
            "event_flag_rows": int(len(flags)),
            "per_family_counts": per_family_counts,
            **paths,
        }
        summary_path = Path(paths["registry_root"]) / "registry_manifest.json"
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

        outputs.append({"path": str(paths["events_path"]), "rows": int(len(events)), "start_ts": None, "end_ts": None})
        outputs.append({"path": str(paths["event_flags_path"]), "rows": int(len(flags)), "start_ts": None, "end_ts": None})
        outputs.append({"path": str(summary_path), "rows": 1, "start_ts": None, "end_ts": None})

        finalize_manifest(
            manifest,
            "success",
            stats={
                "event_rows": int(len(events)),
                "event_flag_rows": int(len(flags)),
                "event_family_count": int(len(per_family_counts)),
            },
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
