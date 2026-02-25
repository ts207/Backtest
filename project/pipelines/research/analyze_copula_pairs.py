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
from pipelines._lib.stats_utils import calculate_kendalls_tau, test_cointegration
from pipelines._lib.copula_utils import (
    fit_gaussian_copula,
    calculate_gaussian_conditional_prob,
    get_empirical_uniforms,
)

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

def detect_copula_events(
    df1: pd.DataFrame, 
    df2: pd.DataFrame, 
    symbol1: str, 
    symbol2: str,
    alpha1: float = 0.05,
    alpha2: float = 0.95,
    lookback_window: int = 288,
    cooldown_bars: int = 12
) -> pd.DataFrame:
    # Align on timestamps
    merged = pd.merge(
        df1[["timestamp", "close"]], 
        df2[["timestamp", "close"]], 
        on="timestamp", 
        suffixes=("_1", "_2")
    ).dropna()
    
    if len(merged) < lookback_window:
        return pd.DataFrame()
    
    merged["ret_1"] = np.log(merged["close_1"] / merged["close_1"].shift(1))
    merged["ret_2"] = np.log(merged["close_2"] / merged["close_2"].shift(1))
    merged = merged.dropna()
    
    n = len(merged)
    events = []
    
    # We'll use a sliding window to fit the copula and detect mispricing
    # For performance in Phase 1, we might fit once or use a large sliding step.
    # The spec implies discovery-style detection.
    
    # Pre-calculate uniforms for the whole series (simplification)
    u1 = get_empirical_uniforms(merged["ret_1"])
    u2 = get_empirical_uniforms(merged["ret_2"])
    
    # Global Kendall's Tau and Cointegration
    tau = calculate_kendalls_tau(merged["ret_1"], merged["ret_2"])
    coint_p = test_cointegration(merged["close_1"], merged["close_2"])
    
    # Fit copula on the whole series for this analyzer version
    rho = fit_gaussian_copula(u1.values, u2.values)
    
    mispricing_index = []
    for i in range(len(merged)):
        prob = calculate_gaussian_conditional_prob(u1.iloc[i], u2.iloc[i], rho)
        mispricing_index.append(prob)
    
    merged["mispricing_index"] = mispricing_index
    
    event_num = 0
    cooldown_until = -1
    
    for i in range(len(merged)):
        if i <= cooldown_until:
            continue
            
        m_idx = merged["mispricing_index"].iloc[i]
        if m_idx < alpha1 or m_idx > alpha2:
            event_num += 1
            event_id = f"cp_v1_{symbol1}_{symbol2}_{event_num:06d}"
            
            events.append({
                "event_id": event_id,
                "symbol": symbol1, # Primary symbol
                "pair_symbol": symbol2,
                "timestamp": merged["timestamp"].iloc[i],
                "enter_idx": i,
                "exit_idx": i, # Impulse event
                "duration_bars": 1,
                "mispricing_index": m_idx,
                "kendalls_tau": tau,
                "cointegration_pval": coint_p,
                "severity": abs(m_idx - 0.5) * 2, # Scale to [0, 1]
                "event_type": "COPULA_PAIRS_TRADING",
                "severity_bucket": "base"
            })
            cooldown_until = i + cooldown_bars
            
    return pd.DataFrame(events)

def main() -> int:
    parser = argparse.ArgumentParser(description="Phase-1 analyzer for Copula Pairs Trading events")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--pairs", required=True, help="Comma-separated pairs (e.g. BTCUSDT:ETHUSDT)")
    parser.add_argument("--alpha1", type=float, default=0.05)
    parser.add_argument("--alpha2", type=float, default=0.95)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--out_dir", default=None)
    args = parser.parse_args()
    
    run_id = args.run_id
    pair_list = [p.strip() for p in args.pairs.split(",") if ":" in p]
    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "copula_pairs" / run_id
    ensure_dir(out_dir)
    
    manifest = start_manifest("analyze_copula_pairs", run_id, vars(args), [], [])
    
    all_events = []
    
    try:
        for pair in pair_list:
            s1, s2 = pair.split(":")
            df1 = _load_bars(run_id, s1, timeframe=args.timeframe)
            df2 = _load_bars(run_id, s2, timeframe=args.timeframe)
            
            if df1.empty or df2.empty:
                logging.warning("Missing bars for pair %s", pair)
                continue
                
            events = detect_copula_events(
                df1, df2, s1, s2, 
                alpha1=args.alpha1, 
                alpha2=args.alpha2
            )
            
            if not events.empty:
                all_events.append(events)
                logging.info("Detected %d copula events for %s", len(events), pair)
                
        events_df = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()
        csv_path = out_dir / "copula_pairs_events.csv"
        events_df.to_csv(csv_path, index=False)
        
        finalize_manifest(manifest, "success", stats={"event_count": len(events_df)})
        return 0
    except Exception as exc:
        logging.exception("Copula pairs analysis failed")
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1

if __name__ == "__main__":
    sys.exit(main())
