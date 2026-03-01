from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

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
from pipelines._lib.run_manifest import finalize_manifest, start_manifest

def _load_bars(run_id: str, symbol: str, timeframe: str = "5m") -> pd.DataFrame:
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", "perp", symbol, f"bars_{timeframe}"),
        DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / f"bars_{timeframe}",
    ]
    bars_dir = choose_partition_dir(candidates)
    if bars_dir is None:
        return pd.DataFrame()
    files = list_parquet_files(bars_dir)
    if not files:
        return pd.DataFrame()
    frame = read_parquet(files)
    if frame.empty:
        return pd.DataFrame()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.sort_values("timestamp").reset_index(drop=True)
    return frame

def detect_funding_dislocations(
    df: pd.DataFrame, 
    symbol: str, 
    threshold_bps: float = 2.0,
    cooldown_bars: int = 12
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    
    # Required columns check
    # We need the funding_rate_scaled (typically from cleaned bars)
    # Note: cleaned bars already have funding_rate_scaled
    # Add simple vol regime proxy
    df["logret"] = np.log(df["close"] / df["close"].shift(1))
    df["rv_proxy"] = df["logret"].rolling(96).std()
    rv_quantiles = df["rv_proxy"].quantile([0.33, 0.66]).to_dict()
    
    def _vol_regime(rv: float) -> str:
        if pd.isna(rv): return "unknown"
        if rv <= rv_quantiles.get(0.33, 0): return "low"
        if rv <= rv_quantiles.get(0.66, 0): return "mid"
        return "high"
    
    df["vol_regime_proxy"] = df["rv_proxy"].map(_vol_regime)

    events = []
    n = len(df)
    i = 0
    cooldown_until = -1
    event_num = 0
    
    while i < n:
        if i <= cooldown_until:
            i += 1
            continue
            
        fr_scaled = float(df["funding_rate_scaled"].iat[i])
        fr_bps = fr_scaled * 10000.0 # Convert back to bps
        
        if abs(fr_bps) > threshold_bps:
            event_num += 1
            event_id = f"fd_v1_{symbol}_{event_num:06d}"
            
            events.append({
                "event_id": event_id,
                "symbol": symbol,
                "timestamp": df["timestamp"].iat[i],
                "enter_idx": i,
                "exit_idx": i, # Impulse event
                "duration_bars": 1,
                "severity": abs(fr_bps),
                "fr_magnitude": abs(fr_bps),
                "fr_sign": 1.0 if fr_bps > 0 else -1.0,
                "vol_regime": df["vol_regime_proxy"].iat[i],
                "event_type": "FND_DISLOC",
                "severity_bucket": "base"
            })
            cooldown_until = i + cooldown_bars
            
        i += 1
        
    return pd.DataFrame(events)

def main() -> int:
    parser = argparse.ArgumentParser(description="Phase-1 analyzer for funding dislocation events")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--threshold_bps", type=float, default=2.0)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    run_id = args.run_id
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "funding_dislocation" / run_id
    ensure_dir(out_dir)

    manifest = start_manifest("analyze_funding_dislocation", run_id, vars(args), [], [])

    try:
        all_events = []
        for sym in symbols:
            bars = _load_bars(run_id, sym, timeframe=args.timeframe)
            if bars.empty:
                continue
            
            events = detect_funding_dislocations(
                bars, 
                sym, 
                threshold_bps=args.threshold_bps
            )
            
            if not events.empty:
                # Add severity buckets
                qs = events["severity"].quantile([0.8, 0.9, 0.95]).to_dict()
                q80, q90, q95 = qs.get(0.8, 1e18), qs.get(0.9, 1e18), qs.get(0.95, 1e18)
                
                def _bucket(val):
                    if val >= q95: return "extreme_5pct"
                    if val >= q90: return "top_10pct"
                    if val >= q80: return "top_20pct"
                    return "base"
                events["severity_bucket"] = events["severity"].map(_bucket)
                all_events.append(events)
                logging.info(f"Detected {len(events)} funding dislocations for {sym}")

        events_df = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()
        csv_path = out_dir / "funding_dislocation_events.parquet"
        events_df.to_parquet(csv_path, index=False)
        
        finalize_manifest(manifest, "success", stats={"event_count": len(events_df)})
        return 0
    except Exception as exc:
        logging.exception("Funding dislocation analysis failed")
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1

if __name__ == "__main__":
    sys.exit(main())
