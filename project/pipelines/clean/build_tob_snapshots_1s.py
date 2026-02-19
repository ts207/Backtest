from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import (
    choose_partition_dir,
    ensure_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
    write_parquet,
)
from pipelines._lib.run_manifest import finalize_manifest, start_manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Build 1s ToB snapshots from bookTicker")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    params = {
        "symbols": symbols,
        "force": int(args.force),
        "snapshot_interval": "1s",
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("build_tob_snapshots_1s", args.run_id, params, inputs, outputs)
    stats: Dict[str, object] = {"symbols": {}}

    try:
        for symbol in symbols:
            raw_dir = DATA_ROOT / "lake" / "raw" / "binance" / "perp" / symbol / "book_ticker"
            files = list_parquet_files(raw_dir)
            if not files:
                logging.warning("No bookTicker data for %s", symbol)
                continue

            for file_path in sorted(files):
                data = read_parquet([file_path])
                if data.empty:
                    continue

                inputs.append({
                    "path": str(file_path),
                    "rows": int(len(data)),
                })

                data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)
                data = data.sort_values("timestamp")

                start_ts = data["timestamp"].min().floor("1s")
                end_ts = data["timestamp"].max().ceil("1s")
                full_index = pd.date_range(start=start_ts, end=end_ts, freq="1s", tz=timezone.utc)
                
                grid = pd.DataFrame({"timestamp": full_index})
                resampled = pd.merge_asof(
                    grid,
                    data,
                    on="timestamp",
                    direction="backward"
                )
                
                first_ts = data["timestamp"].min()
                month_key = f"{first_ts.year}-{first_ts.month:02d}"
                
                out_dir = (
                    DATA_ROOT 
                    / "lake" 
                    / "cleaned" 
                    / "perp" 
                    / symbol 
                    / "tob_1s" 
                    / f"year={first_ts.year}" 
                    / f"month={first_ts.month:02d}"
                )
                out_path = out_dir / f"tob_{symbol}_1s_{month_key}.parquet"
                
                ensure_dir(out_dir)
                written, storage = write_parquet(resampled, out_path)
                outputs.append({
                    "path": str(written),
                    "rows": int(len(resampled)),
                    "start_ts": resampled["timestamp"].min().isoformat(),
                    "end_ts": resampled["timestamp"].max().isoformat(),
                    "storage": storage,
                })
                
                stats["symbols"].setdefault(symbol, {})[month_key] = {
                    "raw_rows": int(len(data)),
                    "snapshot_rows": int(len(resampled)),
                }

        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("ToB snapshot build failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
