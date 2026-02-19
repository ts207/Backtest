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
    parser = argparse.ArgumentParser(description="Build 1m Basis state (perp vs spot)")
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
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("build_basis_state_1m", args.run_id, params, inputs, outputs)
    stats: Dict[str, object] = {"symbols": {}}

    try:
        for symbol in symbols:
            perp_dir = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "bars_1m"
            spot_dir = DATA_ROOT / "lake" / "cleaned" / "spot" / symbol / "bars_1m"
            
            perp_files = list_parquet_files(perp_dir)
            spot_files = list_parquet_files(spot_dir)
            
            if not perp_files or not spot_files:
                logging.warning("Missing perp or spot data for %s", symbol)
                continue

            perp_files_map = {f.name: f for f in perp_files}
            spot_files_map = {f.name: f for f in spot_files}
            common_months = set(perp_files_map.keys()) & set(spot_files_map.keys())

            for month_file in sorted(common_months):
                perp_data = read_parquet([perp_files_map[month_file]])
                spot_data = read_parquet([spot_files_map[month_file]])
                
                if perp_data.empty or spot_data.empty:
                    continue

                inputs.append({"path": str(perp_files_map[month_file]), "rows": int(len(perp_data))})
                inputs.append({"path": str(spot_files_map[month_file]), "rows": int(len(spot_data))})

                perp_data = perp_data[["timestamp", "close"]].rename(columns={"close": "perp_close"})
                spot_data = spot_data[["timestamp", "close"]].rename(columns={"close": "spot_close"})
                
                merged = pd.merge(perp_data, spot_data, on="timestamp", how="inner")
                if merged.empty:
                    continue
                
                merged["basis_bps"] = (merged["perp_close"] - merged["spot_close"]) / merged["spot_close"] * 10000
                merged["symbol"] = symbol
                
                first_ts = merged["timestamp"].iloc[0]
                month_key = f"{first_ts.year}-{first_ts.month:02d}"
                
                out_dir = (
                    DATA_ROOT 
                    / "lake" 
                    / "cleaned" 
                    / "perp" 
                    / symbol 
                    / "basis_1m" 
                    / f"year={first_ts.year}" 
                    / f"month={first_ts.month:02d}"
                )
                out_path = out_dir / f"basis_{symbol}_1m_{month_key}.parquet"
                
                ensure_dir(out_dir)
                written, storage = write_parquet(merged, out_path)
                outputs.append({
                    "path": str(written),
                    "rows": int(len(merged)),
                    "start_ts": merged["timestamp"].min().isoformat(),
                    "end_ts": merged["timestamp"].max().isoformat(),
                    "storage": storage,
                })
                
                stats["symbols"].setdefault(symbol, {})[month_key] = {
                    "merged_rows": int(len(merged)),
                }

        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Basis state build failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
