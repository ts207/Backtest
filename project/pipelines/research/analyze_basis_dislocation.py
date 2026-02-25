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

def _load_bars(run_id: str, symbol: str, market: str, timeframe: str = "5m") -> pd.DataFrame:
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", market, symbol, f"bars_{timeframe}"),
        DATA_ROOT / "lake" / "cleaned" / market / symbol / f"bars_{timeframe}",
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

def detect_basis_dislocations(
    perp_df: pd.DataFrame, 
    spot_df: pd.DataFrame,
    symbol: str, 
    z_threshold: float = 3.0,
    lookback_window: int = 288,
    cooldown_bars: int = 12
) -> pd.DataFrame:
    if perp_df.empty or spot_df.empty:
        return pd.DataFrame()
    
    # Calculate simple vol regime proxy for perp
    p_df = perp_df.copy()
    p_df["logret"] = np.log(p_df["close"] / p_df["close"].shift(1))
    p_df["rv_proxy"] = p_df["logret"].rolling(96).std()
    rv_quantiles = p_df["rv_proxy"].quantile([0.33, 0.66]).to_dict()
    
    def _vol_regime(rv: float) -> str:
        if pd.isna(rv): return "unknown"
        if rv <= rv_quantiles.get(0.33, 0): return "low"
        if rv <= rv_quantiles.get(0.66, 0): return "mid"
        return "high"
    
    p_df["vol_regime_proxy"] = p_df["rv_proxy"].map(_vol_regime)

    # Align spot and perp
    merged = pd.merge(
        p_df[["timestamp", "close", "vol_regime_proxy"]], 
        spot_df[["timestamp", "close"]], 
        on="timestamp", 
        suffixes=("_perp", "_spot")
    ).dropna()
    
    if merged.empty:
        return pd.DataFrame()

    # Calculate Basis in bps
    merged["basis_bps"] = (merged["close_perp"] - merged["close_spot"]) / merged["close_spot"] * 10000.0
    
    # Calculate rolling z-score of basis
    rolling = merged["basis_bps"].rolling(window=lookback_window, min_periods=max(24, lookback_window // 10))
    merged["basis_median"] = rolling.median()
    merged["basis_std"] = rolling.std().replace(0.0, np.nan)
    merged["basis_zscore"] = (merged["basis_bps"] - merged["basis_median"]) / merged["basis_std"]
    
    events = []
    n = len(merged)
    i = 0
    cooldown_until = -1
    event_num = 0
    
    while i < n:
        if i <= cooldown_until:
            i += 1
            continue
            
        z = float(merged["basis_zscore"].iat[i])
        
        if abs(z) > z_threshold:
            event_num += 1
            event_id = f"bd_v1_{symbol}_{event_num:06d}"
            
            events.append({
                "event_id": event_id,
                "symbol": symbol,
                "timestamp": merged["timestamp"].iat[i],
                "enter_idx": i,
                "exit_idx": i, # Impulse
                "duration_bars": 1,
                "severity": abs(z),
                "basis_bps": merged["basis_bps"].iat[i],
                "basis_zscore": z,
                "vol_regime": merged["vol_regime_proxy"].iat[i],
                "event_type": "BASIS_DISLOC",
                "severity_bucket": "base"
            })
            cooldown_until = i + cooldown_bars
            
        i += 1
        
    return pd.DataFrame(events)

def main() -> int:
    parser = argparse.ArgumentParser(description="Phase-1 analyzer for basis dislocation events")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--z_threshold", type=float, default=3.0)
    parser.add_argument("--lookback_window", type=int, default=288)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    run_id = args.run_id
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "basis_dislocation" / run_id
    ensure_dir(out_dir)

    manifest = start_manifest("analyze_basis_dislocation", run_id, vars(args), [], [])

    try:
        all_events = []
        for sym in symbols:
            perp_bars = _load_bars(run_id, sym, "perp", timeframe=args.timeframe)
            spot_bars = _load_bars(run_id, sym, "spot", timeframe=args.timeframe)
            
            if perp_bars.empty or spot_bars.empty:
                logging.warning("Missing perp or spot bars for %s", sym)
                continue
            
            events = detect_basis_dislocations(
                perp_bars, 
                spot_bars,
                sym, 
                z_threshold=args.z_threshold,
                lookback_window=args.lookback_window
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
                logging.info(f"Detected {len(events)} basis dislocations for {sym}")

        events_df = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()
        csv_path = out_dir / "basis_dislocation_events.csv"
        events_df.to_csv(csv_path, index=False)
        
        finalize_manifest(manifest, "success", stats={"event_count": len(events_df)})
        return 0
    except Exception as exc:
        logging.exception("Basis dislocation analysis failed")
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1

if __name__ == "__main__":
    sys.exit(main())
