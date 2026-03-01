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
import yaml

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

def _load_features(run_id: str, symbol: str) -> pd.DataFrame:
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "features", "perp", symbol, "5m", "features_v1"),
        DATA_ROOT / "lake" / "features" / "perp" / symbol / "5m" / "features_v1",
    ]
    features_dir = choose_partition_dir(candidates)
    if features_dir is None:
        return pd.DataFrame()
    files = list_parquet_files(features_dir)
    if not files:
        return pd.DataFrame()
    df = read_parquet(files)
    if df.empty:
        return pd.DataFrame()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df

def detect_shocks(
    df: pd.DataFrame,
    symbol: str,
    median_window: int = 288,  # 1 day
    volume_collapse_th: float = 0.5,  # Vol drops to < 50% of median
    range_spike_th: float = 2.0,      # Range (H-L) spikes to > 200% of median
    cooldown_bars: int = 12,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    
    # Use quote_volume and price range as proxies for liquidity/volatility
    required = ["quote_volume", "high", "low", "close"]
    for col in required:
        if col not in df.columns:
            logging.warning(f"Missing column {col} for {symbol} in liquidity shock analysis")
            return pd.DataFrame()

    df["bar_range"] = (df["high"] - df["low"]) / df["close"] * 10000 # in bps
    
    # Calculate rolling medians
    df["vol_median"] = df["quote_volume"].rolling(window=median_window, min_periods=24).median().fillna(method='ffill').fillna(method='bfill')
    df["range_median"] = df["bar_range"].rolling(window=median_window, min_periods=24).median().fillna(method='ffill').fillna(method='bfill')
    
    events = []
    n = len(df)
    i = 0
    cooldown_until = -1
    event_num = 0
    
    while i < n:
        if i <= cooldown_until:
            i += 1
            continue
            
        vol = float(df["quote_volume"].iat[i])
        rng = float(df["bar_range"].iat[i])
        vol_m = float(df["vol_median"].iat[i])
        rng_m = float(df["range_median"].iat[i])
        
        # Rule: Volume COLLAPSE AND Range SPIKE
        is_shock = (vol < (vol_m * volume_collapse_th)) and (rng > (rng_m * range_spike_th))
        
        if is_shock:
            event_num += 1
            event_id = f"ls_v1_{symbol}_{event_num:06d}"
            
            events.append({
                "event_id": event_id,
                "symbol": symbol,
                "timestamp": df["timestamp"].iat[i],
                "enter_idx": i,
                "exit_idx": i,
                "duration_bars": 1,
                "severity": rng / (rng_m + 1e-9),
                "vol_ratio": vol / (vol_m + 1e-9),
                "range_ratio": rng / (rng_m + 1e-9),
                "vol_regime": df["vol_regime"].iat[i] if "vol_regime" in df.columns else "unknown",
                "severity_bucket": "base"
            })
            
            cooldown_until = i + cooldown_bars
            i += 1
            continue
        i += 1
        
    return pd.DataFrame(events)

def main() -> int:
    parser = argparse.ArgumentParser(description="Phase-1 analyzer for liquidity shock events")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--volume_collapse_th", type=float, default=0.5)
    parser.add_argument("--range_spike_th", type=float, default=2.0)
    parser.add_argument("--median_window", type=int, default=288)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    run_id = args.run_id
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "liquidity_shock" / run_id
    ensure_dir(out_dir)

    manifest = start_manifest("analyze_liquidity_shock", run_id, vars(args), [], [])

    try:
        all_events = []
        for sym in symbols:
            feats = _load_features(run_id, sym)
            if feats.empty:
                continue
            
            events = detect_shocks(
                feats,
                sym,
                median_window=args.median_window,
                volume_collapse_th=args.volume_collapse_th,
                range_spike_th=args.range_spike_th,
            )
            
            if not events.empty:
                # Add severity buckets (based on spread spike magnitude)
                qs = events["severity"].quantile([0.8, 0.9, 0.95]).to_dict()
                q80, q90, q95 = qs.get(0.8, 1e18), qs.get(0.9, 1e18), qs.get(0.95, 1e18)
                
                def _bucket(val):
                    if val >= q95: return "extreme_5pct"
                    if val >= q90: return "top_10pct"
                    if val >= q80: return "top_20pct"
                    return "base"
                events["severity_bucket"] = events["severity"].map(_bucket)
                all_events.append(events)
                logging.info(f"Detected {len(events)} shocks for {sym}")

        events_df = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()
        csv_path = out_dir / "liquidity_shock_events.csv"
        events_df.to_csv(csv_path, index=False)
        
        finalize_manifest(manifest, "success", stats={"event_count": len(events_df)})
        return 0
    except Exception as exc:
        logging.exception("Liquidity shock analysis failed")
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1

if __name__ == "__main__":
    sys.exit(main())
