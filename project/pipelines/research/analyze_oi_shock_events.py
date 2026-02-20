from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
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
)

EVENT_TYPES = [
    "oi_spike_positive",
    "oi_spike_negative",
    "oi_flush",
]

def _rolling_zscore(series: pd.Series, window: int) -> pd.Series:
    mean = series.rolling(window=window).mean()
    std = series.rolling(window=window).std()
    return (series - mean) / std

def _load_features(symbol: str, run_id: str) -> pd.DataFrame:
    # We need OI data which is usually in raw lake or merged features
    # Assuming it's in features_v1 for simplicity
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "features", "perp", symbol, "5m", "features_v1"),
        DATA_ROOT / "lake" / "features" / "perp" / symbol / "5m" / "features_v1",
    ]
    features_dir = choose_partition_dir(candidates)
    files = list_parquet_files(features_dir) if features_dir else []
    if not files:
        raise ValueError(f"No feature partitions found for {symbol}")
    df = read_parquet(files)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df.sort_values("timestamp").reset_index(drop=True)

def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze Open Interest (OI) shock events")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--oi_window", type=int, default=96)
    parser.add_argument("--spike_z_th", type=float, default=2.5)
    parser.add_argument("--flush_pct_th", type=float, default=-0.05)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "oi_shocks" / args.run_id
    ensure_dir(out_dir)

    all_events = []

    for symbol in symbols:
        try:
            df = _load_features(symbol, args.run_id)
            if "oi_notional" not in df.columns:
                print(f"Warning: oi_notional missing for {symbol}")
                continue
            
            oi = df["oi_notional"].astype(float)
            # Use log difference for relative changes
            oi_log_delta = np.log(oi).diff().fillna(0.0)
            oi_z = _rolling_zscore(oi_log_delta, args.oi_window)
            
            # 1. OI Spike Positive (Aggressive positioning)
            spike_pos = (oi_z >= args.spike_z_th) & (df["close"].pct_change(1) > 0)
            
            # 2. OI Spike Negative (Aggressive shorting)
            spike_neg = (oi_z >= args.spike_z_th) & (df["close"].pct_change(1) < 0)
            
            # 3. OI Flush (Liquidation/Unwinding)
            oi_pct_change = oi.pct_change(1)
            flush = (oi_pct_change <= args.flush_pct_th)
            
            masks = {
                "oi_spike_positive": spike_pos,
                "oi_spike_negative": spike_neg,
                "oi_flush": flush
            }
            
            for et, mask in masks.items():
                indices = np.flatnonzero(mask.fillna(False).values)
                for idx in indices:
                    all_events.append({
                        "symbol": symbol,
                        "event_type": et,
                        "event_idx": int(idx),
                        "timestamp": df.at[idx, "timestamp"].isoformat(),
                        "oi_z": float(oi_z.iloc[idx]) if np.isfinite(oi_z.iloc[idx]) else 0.0,
                        "oi_pct_change": float(oi_pct_change.iloc[idx]) if np.isfinite(oi_pct_change.iloc[idx]) else 0.0
                    })
        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    events_df = pd.DataFrame(all_events)
    events_csv = out_dir / "oi_shock_events.csv"
    events_df.to_csv(events_csv, index=False)
    
    print(f"Wrote {len(events_df)} OI shock events to {events_csv}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
