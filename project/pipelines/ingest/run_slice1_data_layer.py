from __future__ import annotations

import argparse
import logging
import os
import sys
import subprocess
from pathlib import Path
from typing import List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

def run_script(script_path: str, args: List[str]):
    cmd = [sys.executable, script_path] + args
    logging.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        logging.error("Script %s failed with return code %d", script_path, result.returncode)
        sys.exit(result.returncode)

def main():
    parser = argparse.ArgumentParser(description="Run Slice 1 Data Layer Pipeline")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--force", type=int, default=0)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    common_args = ["--run_id", args.run_id, "--symbols", args.symbols]
    time_args = ["--start", args.start, "--end", args.end, "--force", str(args.force)]

    # 1. Ingestion (Raw)
    run_script("project/pipelines/ingest/ingest_binance_um_ohlcv_1m.py", common_args + time_args)
    run_script("project/pipelines/ingest/ingest_binance_spot_ohlcv_1m.py", common_args + time_args)
    run_script("project/pipelines/ingest/ingest_binance_um_mark_price_1m.py", common_args + time_args)
    run_script("project/pipelines/ingest/ingest_binance_um_book_ticker.py", common_args + time_args)
    run_script("project/pipelines/ingest/ingest_binance_um_funding.py", common_args + time_args)
    run_script("project/pipelines/ingest/ingest_binance_um_open_interest_hist.py", common_args + time_args)

    # 2. Cleaning (Cleaned)
    run_script("project/pipelines/clean/build_cleaned_1m.py", common_args + ["--market", "perp", "--force", str(args.force)])
    run_script("project/pipelines/clean/build_cleaned_1m.py", common_args + ["--market", "spot", "--force", str(args.force)])

    # 3. ToB Processing
    run_script("project/pipelines/clean/build_tob_snapshots_1s.py", common_args + ["--force", str(args.force)])
    run_script("project/pipelines/clean/build_tob_1m_agg.py", common_args + ["--force", str(args.force)])

    # 4. Basis Processing
    run_script("project/pipelines/clean/build_basis_state_1m.py", common_args + ["--force", str(args.force)])

    # 5. QA Report
    run_script("project/pipelines/report/qa_data_layer.py", common_args)

    logging.info("Slice 1 Data Layer Pipeline completed successfully.")

if __name__ == "__main__":
    main()
