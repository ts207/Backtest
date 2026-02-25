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
    liq_median_window: int = 288,  # 1 day of 5m bars
    liq_multiplier: float = 3.0,
    cooldown_bars: int = 12
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    
    # Required columns check
    required = ["liquidation_notional", "oi_delta_1h", "oi_notional", "close", "high", "low"]
    for col in required:
        if col not in df.columns:
            logging.warning(f"Missing column {col} for {symbol}")
            return pd.DataFrame()

    # Calculate dynamic threshold
    # Note: Using rolling median to identify "normal" liquidation levels
    min_p = min(liq_median_window, 24)
    df["liq_median"] = df["liquidation_notional"].rolling(window=liq_median_window, min_periods=min_p).median().fillna(0.0)
    df["liq_th"] = df["liq_median"] * liq_multiplier
    
    # Handle extremely low liquidity periods by enforcing a floor if needed, 
    # but spec says > 3.0 * median.
    
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
        threshold = float(df["liq_th"].iat[i])
        
        # Rule: Liquidation spike AND OI drop
        if liq > threshold and oi_delta < 0:
            # Found a cascade start
            start_idx = i
            end_idx = i
            
            # Aggregate multi-bar cascades (simple lookahead for continuation)
            # If the next bar also meets criteria, include it.
            # We'll allow a small gap or just strict sequence? 
            # Let's do sequence for now.
            while end_idx + 1 < n:
                next_liq = float(df["liquidation_notional"].iat[end_idx + 1])
                next_oi_delta = float(df["oi_delta_1h"].iat[end_idx + 1])
                next_threshold = float(df["liq_th"].iat[end_idx + 1])
                
                if next_liq > next_threshold and next_oi_delta < 0:
                    end_idx += 1
                else:
                    break
            
            event_num += 1
            event_id = f"lc_v1_{symbol}_{event_num:06d}"
            
            # Calculate metadata
            window = df.iloc[start_idx : end_idx + 1]
            total_liq = window["liquidation_notional"].sum()
            
            # OI Reduction: from before the cascade to the end
            # We use oi_notional. oi_delta_1h is a flow, we want stock change.
            oi_before = df["oi_notional"].iat[max(0, start_idx - 1)]
            oi_after = df["oi_notional"].iat[end_idx]
            oi_reduction = oi_before - oi_after
            oi_reduction_pct = (oi_reduction / oi_before) if oi_before > 0 else 0.0
            
            # Price Drawdown: from high in window to low in window
            price_start = df["close"].iat[max(0, start_idx - 1)]
            price_low = window["low"].min()
            price_drawdown = (price_start - price_low) / price_start if price_start > 0 else 0.0
            
            events.append({
                "event_id": event_id,
                "symbol": symbol,
                "timestamp": df["timestamp"].iat[start_idx],
                "enter_idx": start_idx,
                "exit_idx": end_idx,
                "duration_bars": end_idx - start_idx + 1,
                "severity": total_liq,
                "total_liquidation_notional": total_liq,
                "oi_reduction_pct": oi_reduction_pct,
                "price_drawdown": price_drawdown,
                "vol_regime": df["vol_regime"].iat[start_idx] if "vol_regime" in df.columns else "unknown",
                "severity_bucket": "base"
            })
            
            cooldown_until = end_idx + cooldown_bars
            i = end_idx + 1
            continue
        i += 1
        
    return pd.DataFrame(events)

def main() -> int:
    parser = argparse.ArgumentParser(description="Phase-1 analyzer for liquidation cascade events")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--liq_multiplier", type=float, default=3.0)
    parser.add_argument("--median_window", type=int, default=288)
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
            
            events = detect_cascades(
                feats, 
                sym, 
                liq_median_window=args.median_window, 
                liq_multiplier=args.liq_multiplier
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
