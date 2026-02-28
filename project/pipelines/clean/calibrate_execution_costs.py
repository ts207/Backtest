"""
calibrate_execution_costs.py
Reads tob_5m_agg per symbol and writes per-symbol cost calibration JSON.
Output: data/reports/cost_calibration/<run_id>/<symbol>.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import choose_partition_dir, ensure_dir, list_parquet_files, read_parquet, run_scoped_lake_path
from pipelines._lib.run_manifest import finalize_manifest, start_manifest


def _calibrate_symbol(symbol: str, run_id: str) -> dict | None:
    """
    Load tob_5m_agg for a symbol and compute calibration coefficients.
    Returns None if insufficient data.
    """
    tob_dir = choose_partition_dir(
        [
            DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "tob_5m_agg",
            run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", "perp", symbol, "tob_5m_agg"),
        ]
    )
    files = list_parquet_files(tob_dir) if tob_dir else []
    if not files:
        return None

    frames = [read_parquet([f]) for f in files]
    df = pd.concat(frames, ignore_index=True)

    if df.empty:
        return None

    # Compute calibration coefficients from ToB aggregates
    spread_col = "spread_bps_mean" if "spread_bps_mean" in df.columns else "spread_bps"
    spread = pd.to_numeric(df.get(spread_col, pd.Series(dtype=float)), errors="coerce").dropna()

    if spread.empty or len(spread) < 100:
        return None

    median_spread = float(spread.median())
    p75_spread = float(spread.quantile(0.75))

    return {
        "base_slippage_bps": round(median_spread / 2.0, 4),  # half-spread as slippage proxy
        "spread_weight": 0.5,
        "p75_spread_bps": round(p75_spread, 4),
        "calibration_source": "tob_5m_agg",
        "n_bars": int(len(spread)),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Calibrate per-symbol execution costs from ToB aggregates")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", nargs="*", default=None,
        help="Symbols to calibrate. Defaults to all symbols in universe.")
    parser.add_argument("--out_dir", default=None)
    args = parser.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "cost_calibration" / args.run_id
    ensure_dir(out_dir)

    params = {"run_id": args.run_id, "out_dir": str(out_dir)}
    inputs: list = []
    outputs: list = []
    manifest = start_manifest("calibrate_execution_costs", args.run_id, params, inputs, outputs)

    try:
        # Discover symbols
        symbols = args.symbols or []
        if not symbols:
            perp_root = DATA_ROOT / "lake" / "cleaned" / "perp"
            if perp_root.exists():
                symbols = [p.name for p in sorted(perp_root.iterdir()) if p.is_dir()]

        calibrated = 0
        for symbol in symbols:
            calib = _calibrate_symbol(symbol, args.run_id)
            if calib is None:
                continue
            out_path = out_dir / f"{symbol}.json"
            out_path.write_text(json.dumps(calib, indent=2), encoding="utf-8")
            outputs.append({"path": str(out_path), "rows": 1, "start_ts": None, "end_ts": None})
            calibrated += 1

        finalize_manifest(manifest, "success", stats={"calibrated_symbols": calibrated})
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
