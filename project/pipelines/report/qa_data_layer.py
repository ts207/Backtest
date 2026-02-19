from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import choose_partition_dir, ensure_dir, list_parquet_files, read_parquet, run_scoped_lake_path
from pipelines._lib.run_manifest import finalize_manifest, start_manifest


def _check_quality(df: pd.DataFrame, name: str) -> Dict[str, object]:
    if df.empty:
        return {"status": "empty"}
    
    ts = pd.to_datetime(df["timestamp"], utc=True)
    gaps = ts.diff() > ts.diff().min() # simple gap check
    gap_count = int(gaps.sum())
    
    return {
        "rows": int(len(df)),
        "start": ts.min().isoformat(),
        "end": ts.max().isoformat(),
        "gap_count": gap_count,
        "is_monotonic": bool(ts.is_monotonic_increasing),
        "duplicate_count": int(ts.duplicated().sum()),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="QA Data Layer (Slice 1)")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    manifest = start_manifest("qa_data_layer", args.run_id, {"symbols": symbols}, [], [])
    stats: Dict[str, object] = {"symbols": {}}

    try:
        for symbol in symbols:
            symbol_stats = {}
            
            # Check bars_1m_perp
            perp_bars_dir = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "bars_1m"
            perp_bars = read_parquet(list_parquet_files(perp_bars_dir))
            symbol_stats["perp_bars_1m"] = _check_quality(perp_bars, "perp_bars_1m")
            
            # Check bars_1m_spot
            spot_bars_dir = DATA_ROOT / "lake" / "cleaned" / "spot" / symbol / "bars_1m"
            spot_bars = read_parquet(list_parquet_files(spot_bars_dir))
            symbol_stats["spot_bars_1m"] = _check_quality(spot_bars, "spot_bars_1m")
            
            # Check tob_1m_agg
            tob_agg_dir = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "tob_1m_agg"
            tob_agg = read_parquet(list_parquet_files(tob_agg_dir))
            symbol_stats["tob_1m_agg"] = _check_quality(tob_agg, "tob_1m_agg")
            
            # Check basis_1m
            basis_dir = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "basis_1m"
            basis = read_parquet(list_parquet_files(basis_dir))
            symbol_stats["basis_1m"] = _check_quality(basis, "basis_1m")
            
            stats["symbols"][symbol] = symbol_stats

        report_dir = DATA_ROOT / "reports" / "qa" / args.run_id
        ensure_dir(report_dir)
        report_path = report_dir / "slice1_data_integrity.json"
        report_path.write_text(json.dumps(stats, indent=2, sort_keys=True), encoding="utf-8")

        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("QA failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
