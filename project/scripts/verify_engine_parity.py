from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from engine.runner import _load_symbol_data, run_engine
from engine.pnl import compute_returns
from pipelines._lib.io_utils import ensure_dir

def calculate_trade_parity(
    sig_ts: pd.Timestamp,
    entry_ts: pd.Timestamp,
    last_held_ts: pd.Timestamp,
    bars: pd.DataFrame,
    engine_df: pd.DataFrame,
    direction: int,
    horizon_bars: int,
    tol_abs: float = 1e-3,
    tol_rel: float = 1e-4,
) -> Dict[str, Any]:
    """
    Calculates parity metrics for a single observed engine trade.
    """
    bars_ts = pd.to_datetime(bars["timestamp"], utc=True)
    entry_pos = int(bars_ts.searchsorted(entry_ts, side="left"))
    
    # Research expected exit
    future_pos = entry_pos + horizon_bars
    if future_pos >= len(bars):
        return {"skipped": "horizon_out_of_bounds"}
        
    expected_last_held_ts = bars_ts.iloc[entry_pos + horizon_bars - 1]
    expected_exit_ts = bars_ts.iloc[future_pos]
    
    # 1. Timing Parity
    timing_ok = (last_held_ts == expected_last_held_ts)
    
    # 2. Return Parity
    # P_exit is at entry_pos + horizon_bars
    p_entry = bars["close"].iloc[entry_pos]
    p_exit = bars["close"].iloc[future_pos]
    
    # Research arithmetic return: direction * (P_exit / P_entry - 1)
    research_ret = direction * (p_exit / p_entry - 1)
    # Cumulative return in log-space: log(1 + cumulative_return)
    research_logret = np.log(1 + research_ret)
    
    # Engine: sum(log(1 + direction * ret_i)) where ret_i are bar-by-bar returns
    # This represents the cumulative log-return of the compounded position
    engine_ret_slice = engine_df.set_index("timestamp")["ret"].loc[bars_ts.iloc[entry_pos+1]:expected_exit_ts]
    engine_logret = np.log(1 + direction * engine_ret_slice).sum()
    
    logret_diff = research_logret - engine_logret
    threshold = tol_abs + tol_rel * max(1.0, np.abs(research_logret))
    logret_ok = np.abs(logret_diff) <= threshold
    
    return {
        "sig_ts": sig_ts.isoformat(),
        "entry_ts": entry_ts.isoformat(),
        "timing_ok": bool(timing_ok),
        "logret_ok": bool(logret_ok),
        "expected_last_held": expected_last_held_ts.isoformat(),
        "actual_last_held": last_held_ts.isoformat(),
        "research_return": float(research_ret),
        "research_logret": float(research_logret),
        "engine_logret": float(engine_logret),
        "cum_logret_diff": float(logret_diff),
        "cum_logret_tol": float(threshold),
        "direction": direction,
    }

def main():
    parser = argparse.ArgumentParser(description="Verify parity between Engine and Phase2 forward returns")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--candidate_id", required=True)
    parser.add_argument("--event_type", required=True)
    parser.add_argument("--tol_abs", type=float, default=1e-3)
    parser.add_argument("--tol_rel", type=float, default=1e-4)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    
    # 1. Load Data
    event_flags = None
    flags_path = DATA_ROOT / "events" / args.run_id / "event_flags.parquet"
    if flags_path.exists():
        event_flags = pd.read_parquet(flags_path)
        if not event_flags.empty and "symbol" in event_flags.columns:
            event_flags = event_flags[event_flags["symbol"] == args.symbol].copy()
    
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
    # Use massive stop/target values to ensure no early exits during parity check.
    blueprint["exit"]["stop_value"] = 1000.0
    blueprint["exit"]["target_value"] = 1000.0
    blueprint["exit"]["trailing_stop_type"] = "none"
    blueprint["exit"]["trailing_stop_value"] = 0.0
    blueprint["exit"]["break_even_r"] = 0.0
    blueprint["exit"]["invalidation"] = {"metric": "close", "operator": ">", "value": 1e12}
    
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
        cost_bps=10.0,
        data_root=DATA_ROOT,
        params_by_strategy=params_by_strategy,
    )
    
    engine_df = engine_results["strategy_frames"][strategy_id]
    if not engine_df.empty and "symbol" in engine_df.columns:
        engine_df = engine_df[engine_df["symbol"] == args.symbol].copy()
    engine_df["timestamp"] = pd.to_datetime(engine_df["timestamp"], utc=True)
    
    trace_path = engine_results["engine_dir"] / f"strategy_trace_{strategy_id}.csv"
    trace = pd.read_csv(trace_path)
    if not trace.empty and "symbol" in trace.columns:
        trace = trace[trace["symbol"] == args.symbol].copy().reset_index(drop=True)
    trace["timestamp"] = pd.to_datetime(trace["timestamp"], utc=True)
    
    # 4. Parity Logic
    horizon_bars = int(blueprint["exit"]["time_stop_bars"])
    delay_bars = int(blueprint["entry"]["delay_bars"])
    
    # Find observed trades from trace
    trace["state_prev"] = trace["state"].shift(1).fillna("flat")
    trade_starts = trace[(trace["state"] == "in_position") & (trace["state_prev"] != "in_position")]
    
    results = []
    for _, row in trade_starts.iterrows():
        entry_ts = row["timestamp"]
        # sig_ts is entry_ts - delay_bars * 5m
        sig_ts = entry_ts - pd.Timedelta(minutes=5 * delay_bars)
        
        trace_after = trace[trace["timestamp"] >= entry_ts]
        exit_bar = trace_after[trace_after["state"] != "in_position"]
        if not exit_bar.empty:
            exit_idx = exit_bar.index[0]
            last_held_row = trace.loc[exit_idx - 1]
            last_held_ts = last_held_row["timestamp"]
        else:
            last_held_ts = trace_after["timestamp"].iloc[-1]
            
        dir_match = engine_df[engine_df["timestamp"] == entry_ts]["pos"]
        if dir_match.empty:
            logging.warning("No engine position at entry_ts=%s, skipping trade", entry_ts)
            continue
        direction = int(dir_match.iloc[0])
        
        res = calculate_trade_parity(
            sig_ts,
            entry_ts,
            last_held_ts,
            bars,
            engine_df,
            direction,
            horizon_bars,
            tol_abs=args.tol_abs,
            tol_rel=args.tol_rel,
        )
        if "skipped" not in res:
            results.append(res)
            
    # 5. Aggregate Report
    timing_ok = all(r.get("timing_ok", False) for r in results) if results else False
    logret_ok = all(r.get("logret_ok", False) for r in results) if results else False
    
    logret_diffs = [np.abs(r["cum_logret_diff"]) for r in results if "cum_logret_diff" in r]
    max_logret_diff = float(np.max(logret_diffs)) if logret_diffs else 0.0
    mean_logret_diff = float(np.mean(logret_diffs)) if logret_diffs else 0.0
    
    summary = {
        "run_id": args.run_id,
        "symbol": args.symbol,
        "candidate_id": args.candidate_id,
        "horizon_bars": horizon_bars,
        "n_samples": len(results),
        "timing_ok": bool(timing_ok),
        "logret_ok": bool(logret_ok),
        "parity_success": bool(timing_ok and logret_ok and len(results) > 0),
        "max_cum_logret_diff": max_logret_diff,
        "mean_cum_logret_diff": mean_logret_diff,
        "cum_logret_tol_abs": args.tol_abs,
        "cum_logret_tol_rel": args.tol_rel,
        "samples": results[:10]
    }
    
    out_dir = DATA_ROOT / "reports" / "diagnostics" / args.run_id
    ensure_dir(out_dir)
    out_path = out_dir / "parity_report.json"
    out_path.write_text(json.dumps(summary, indent=2, sort_keys=True))
    print(f"Parity report written to {out_path}")
    
    if not summary["parity_success"]:
        if not timing_ok:
            print("CRITICAL: Timing parity failed (observed trade duration != horizon)!")
        if not logret_ok:
            print(f"ERROR: Return parity failed (exceeded tol_abs={args.tol_abs}, tol_rel={args.tol_rel})!")
        if len(results) == 0:
            print("ERROR: No trades observed in backtest!")
        sys.exit(1)
    else:
        print("SUCCESS: Timing and Return parity verified.")

if __name__ == "__main__":
    main()
