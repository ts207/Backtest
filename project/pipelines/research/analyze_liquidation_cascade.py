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

def detect_cascades(
    df: pd.DataFrame, 
    symbol: str, 
    liq_vol_th: float, 
    oi_drop_th: float,
    cooldown_bars: int = 12
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    
    # Required columns check
    for col in ["liquidation_notional", "oi_delta_1h"]:
        if col not in df.columns:
            logging.warning(f"Missing column {col} for {symbol}")
            return pd.DataFrame()

    events = []
    n = len(df)
    i = 0
    cooldown_until = -1
    event_num = 0
    
    while i < n:
        if i <= cooldown_until:
            i += 1
            continue
            
        liq = float(df["liquidation_notional"].iat[i])
        oi_delta = float(df["oi_delta_1h"].iat[i])
        
        # Rule: Liquidation spike AND OI drop
        if liq >= liq_vol_th and oi_delta <= oi_drop_th:
            event_num += 1
            event_id = f"lc_v1_{symbol}_{event_num:06d}"
            
            events.append({
                "event_id": event_id,
                "symbol": symbol,
                "timestamp": df["timestamp"].iat[i],
                "enter_idx": i,
                "severity": liq,
                "liquidation_total": liq,
                "oi_drop_total": oi_delta,
                "duration_bars": 1, # Minimal representation
                "vol_regime": df["vol_regime"].iat[i] if "vol_regime" in df.columns else "unknown",
                "severity_bucket": "base" # Will be updated later if needed
            })
            cooldown_until = i + cooldown_bars
            i = cooldown_until + 1
            continue
        i += 1
        
    return pd.DataFrame(events)

def main() -> int:
    parser = argparse.ArgumentParser(description="Phase-1 analyzer for liquidation cascade events")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--liq_vol_th", type=float, default=100000.0)
    parser.add_argument("--oi_drop_th", type=float, default=-500000.0)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    run_id = args.run_id
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "liquidation_cascade" / run_id
    ensure_dir(out_dir)

    manifest = start_manifest("analyze_liquidation_cascade", run_id, vars(args), [], [])

    try:
        all_events = []
        for sym in symbols:
            feats = _load_features(run_id, sym)
            if feats.empty:
                continue
            
            # Use market context if available for vol_regime
            # For simplicity, we'll just check if it exists in features
            # (In a real run, build_market_context would have run before this)
            
            events = detect_cascades(feats, sym, args.liq_vol_th, args.oi_drop_th)
            if not events.empty:
                # Add severity buckets
                qs = events["severity"].quantile([0.8, 0.9, 0.95]).to_dict()
                def _bucket(val):
                    if val >= qs.get(0.95, 1e18): return "extreme_5pct"
                    if val >= qs.get(0.9, 1e18): return "top_10pct"
                    if val >= qs.get(0.8, 1e18): return "top_20pct"
                    return "base"
                events["severity_bucket"] = events["severity"].map(_bucket)
                all_events.append(events)
                logging.info(f"Detected {len(events)} cascades for {sym}")

        events_df = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()
        csv_path = out_dir / "liquidation_cascade_events.csv"
        events_df.to_csv(csv_path, index=False)
        
        finalize_manifest(manifest, "success", stats={"event_count": len(events_df)})
        return 0
    except Exception as exc:
        logging.exception("Liquidation cascade analysis failed")
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1

if __name__ == "__main__":
    sys.exit(main())
