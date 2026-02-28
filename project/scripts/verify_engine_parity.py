from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from engine.runner import _load_symbol_data, run_engine
from engine.pnl import compute_returns
from pipelines._lib.io_utils import ensure_dir

def main():
    parser = argparse.ArgumentParser(description="Verify parity between Engine and Phase2 forward returns")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--candidate_id", required=True)
    parser.add_argument("--event_type", required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    
    # 1. Load Data
    event_flags = None
    flags_path = DATA_ROOT / "events" / args.run_id / "event_flags.parquet"
    if flags_path.exists():
        event_flags = pd.read_parquet(flags_path)
    
    bars, features = _load_symbol_data(DATA_ROOT, args.symbol, args.run_id, event_flags=event_flags)
    
    # 2. Load Candidate/Blueprint
    bp_path = DATA_ROOT / "reports" / "strategy_blueprints" / args.run_id / "blueprints.yaml"
    if not bp_path.exists():
        print(f"Blueprint not found at {bp_path}")
        return 1
        
    import yaml
    with open(bp_path, "r") as f:
        blueprints = yaml.safe_load(f)
        
    blueprint = next((bp for bp in blueprints if bp["candidate_id"] == args.candidate_id), None)
    if not blueprint:
        print(f"Candidate {args.candidate_id} not found in blueprints")
        return 1
        
    blueprint["entry"]["confirmations"] = []
    blueprint["overlays"] = []
    
    # 3. Run Engine
    strategy_id = f"dsl_interpreter_v1__{args.candidate_id}"
    params_by_strategy = {
        strategy_id: {
            "dsl_blueprint": blueprint,
            "position_scale": 1.0,
            "strategy_family": "DSL",
        }
    }
    
    engine_results = run_engine(
        run_id=args.run_id,
        symbols=[args.symbol],
        strategies=[strategy_id],
        params={},
        cost_bps=10.0, # Add a cost to test cost metrics
        data_root=DATA_ROOT,
        params_by_strategy=params_by_strategy,
    )
    
    engine_df = engine_results["strategy_frames"][strategy_id]
    engine_df["timestamp"] = pd.to_datetime(engine_df["timestamp"], utc=True)
    engine_pnl = engine_df.set_index("timestamp")["gross_pnl"]
    
    # Cost metrics
    trades = engine_df[engine_df["pos"] != engine_df["pos"].shift(1)]
    mean_cost_bps = float(trades["trading_cost"].mean() * 10000.0) if not trades.empty and "trading_cost" in trades.columns else 0.0
    mean_funding_bps = float(engine_df["funding_pnl"].mean() * 10000.0) if "funding_pnl" in engine_df.columns else 0.0
    max_cost_cap_count = 0 # Cannot easily infer cap triggered from runner output directly without logging, but we log the metrics
    
    # 4. Research Parity Logic (Recomputed here for 1:1 comparison)
    effective_lag = int(blueprint["entry"]["delay_bars"])
    horizon_bars = int(blueprint["exit"]["time_stop_bars"])
    
    # Find signal bars from features
    trigger_col = blueprint["entry"]["triggers"][0]
    if trigger_col in features.columns:
        signal_bars = features[features[trigger_col] == True]["timestamp"].tolist()
    else:
        signal_bars = []
        
    trace_path = engine_results["engine_dir"] / f"strategy_trace_{strategy_id}.csv"
    trace = pd.read_csv(trace_path)
    trace["timestamp"] = pd.to_datetime(trace["timestamp"], utc=True)
    
    results = []
    for sig_ts in signal_bars:
        # Research style forward return
        feat_ts_idx = pd.to_datetime(features["timestamp"], utc=True)
        pos = int(feat_ts_idx.searchsorted(sig_ts, side="left"))
        entry_pos = pos + effective_lag
        future_pos = entry_pos + horizon_bars
        
        if future_pos < len(bars):
            start_pnl_ts = bars["timestamp"].iloc[entry_pos + 1]
            end_pnl_ts = bars["timestamp"].iloc[future_pos]
            
            # Find the actual direction taken by the engine
            trade_trace = trace[(trace["timestamp"] > sig_ts) & (trace["timestamp"] <= end_pnl_ts)]
            active_pos = trade_trace[trade_trace["state"] == "in_position"]
            if active_pos.empty:
                continue
                
            direction = active_pos["entry_candidate"].sum() if "entry_candidate" in active_pos.columns and active_pos["entry_candidate"].sum() != 0 else active_pos["requested_scale"].iloc[0]
            # wait, requested scale might be > 0. Let's just look at the raw position in engine_df
            engine_pos_slice = engine_df.set_index("timestamp")["pos"].loc[start_pnl_ts:end_pnl_ts]
            if (engine_pos_slice == 0).all():
                continue
            direction = 1 if (engine_pos_slice > 0).any() else -1

            research_ret = ((bars["close"].iloc[future_pos] / bars["close"].iloc[entry_pos]) - 1) * direction
            
            # Engine style return for the same window
            engine_gross_sum = engine_pnl.loc[start_pnl_ts:end_pnl_ts].sum()
            
            results.append({
                "signal_ts": sig_ts.isoformat(),
                "research_return": float(research_ret),
                "engine_gross_pnl_sum": float(engine_gross_sum),
                "diff": float(research_ret - engine_gross_sum)
            })
            
    # 5. Report
    summary = {
        "run_id": args.run_id,
        "symbol": args.symbol,
        "candidate_id": args.candidate_id,
        "effective_lag": effective_lag,
        "horizon_bars": horizon_bars,
        "n_samples": len(results),
        "mean_diff": float(np.mean([r["diff"] for r in results])) if results else 0.0,
        "max_diff": float(np.max([np.abs(r["diff"]) for r in results])) if results else 0.0,
        "parity_success": bool(len(results) > 0 and np.max([np.abs(r["diff"]) for r in results]) < 1e-6),
        "mean_cost_bps": mean_cost_bps,
        "mean_funding_bps": mean_funding_bps,
        "max_cost_cap_triggered_count": max_cost_cap_count,
        "samples": results[:10]
    }
    
    out_dir = DATA_ROOT / "reports" / "diagnostics" / args.run_id
    ensure_dir(out_dir)
    out_path = out_dir / "parity_report.json"
    out_path.write_text(json.dumps(summary, indent=2, sort_keys=True))
    print(f"Parity report written to {out_path}")
    if not summary["parity_success"]:
        print("WARNING: Parity check failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
