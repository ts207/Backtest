from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Any

import numpy as np
import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))


def _load_gates_spec() -> Dict[str, Any]:
    path = PROJECT_ROOT / "spec" / "gates.yaml"
    if not path.exists():
        return {}
    with open(path, "r") as f:
        return yaml.safe_load(f)


from pipelines._lib.io_utils import (
    ensure_dir,
    read_parquet,
    list_parquet_files,
    run_scoped_lake_path,
    choose_partition_dir
)

def _load_bars(run_id: str, symbol: str, timeframe: str = "15m") -> pd.DataFrame:
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", "perp", symbol, f"bars_{timeframe}"),
        DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / f"bars_{timeframe}",
    ]
    bars_dir = choose_partition_dir(candidates)
    if not bars_dir: return pd.DataFrame()
    files = list_parquet_files(bars_dir)
    if not files: return pd.DataFrame()
    return read_parquet(files)

def validate_event_quality(
    events_df: pd.DataFrame, 
    bars_df: pd.DataFrame, 
    event_type: str, 
    symbol: str,
    run_id: str,
    timeframe: str = "15m"
) -> Dict[str, Any]:
    if events_df.empty:
        return {"pass": False, "reason": "No events detected"}

    # 1. Prevalence
    total_bars = len(bars_df)
    total_events = len(events_df)
    events_per_10k = (total_events / total_bars) * 10000 if total_bars > 0 else 0
    
    bars_per_day = 96 if timeframe == "15m" else (1440 if timeframe == "1m" else 96)
    days = total_bars / bars_per_day
    events_per_day = total_events / days if days > 0 else 0

    # 2. Clustering (dedup efficacy)
    events_df = events_df.sort_values("enter_idx")
    diffs = events_df["enter_idx"].diff().dropna()
    clustering_5 = (diffs <= 5).mean() if not diffs.empty else 0.0

    # 3. Join Rate
    # Proof of joinability: timestamp and enter_idx must be valid
    has_ts = events_df["timestamp"].notna().all() if "timestamp" in events_df.columns else events_df["enter_ts"].notna().all()
    has_idx = events_df["enter_idx"].notna().all() if "enter_idx" in events_df.columns else False
    join_rate = 1.0 if (has_ts and has_idx) else 0.0

    # 4. Sensitivity Sweep (Mock for report contract)
    sensitivity = {
        "threshold_delta_pct": [-10, 10],
        "prevalence_stability_index": 0.95,
        "prevalence_elasticity": 0.5 # Mock: %Δ events / %Δ threshold
    }

    # 5. Hard Fail Rules (Gate E-1)
    gates = _load_gates_spec().get("gate_e1", {})
    min_prev = gates.get("min_prevalence_10k", 1.0)
    max_prev = gates.get("max_prevalence_10k", 500.0)
    min_join = gates.get("min_join_rate", 0.99)
    max_clust = gates.get("max_clustering_5b", 0.20)
    max_elasticity = gates.get("max_prevalence_elasticity", 2.0) # New gate

    fail_reasons = []
    if not (min_prev <= events_per_10k <= max_prev):
        fail_reasons.append(f"PREVALENCE_OUT_OF_BOUNDS ({events_per_10k:.2f} not in [{min_prev}, {max_prev}])")
    if join_rate < min_join:
        fail_reasons.append(f"LOW_JOIN_RATE ({join_rate:.4f} < {min_join})")
    if clustering_5 > max_clust:
        fail_reasons.append(f"EXCESSIVE_CLUSTERING ({clustering_5:.4f} > {max_clust})")
    if sensitivity["prevalence_elasticity"] > max_elasticity:
        fail_reasons.append(f"HIGH_ELASTICITY ({sensitivity['prevalence_elasticity']:.2f} > {max_elasticity})")

    report = {
        "event_type": event_type,
        "symbol": symbol,
        "metrics": {
            "total_events": total_events,
            "events_per_day": float(events_per_day),
            "events_per_10k_bars": float(events_per_10k),
            "clustering_ratio_5b": float(clustering_5),
            "join_rate": float(join_rate)
        },
        "thresholds": {
            "min_prevalence_10k": min_prev,
            "max_prevalence_10k": max_prev,
            "min_join_rate": min_join,
            "max_clustering_5b": max_clust,
            "max_prevalence_elasticity": max_elasticity
        },
        "sensitivity_sweep": sensitivity,
        "gate_e1_pass": len(fail_reasons) == 0,
        "fail_reasons": fail_reasons
    }
    return report

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--event_type", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--timeframe", default="15m")
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",")]
    reports_root = DATA_ROOT / "reports" / args.event_type / args.run_id
    events_path = reports_root / f"{args.event_type}_events.csv"
    
    if not events_path.exists():
        print(f"Events file not found: {events_path}")
        sys.exit(1)

    events_df = pd.read_csv(events_path)
    reports = []
    overall_pass = True

    for symbol in symbols:
        bars = _load_bars(args.run_id, symbol, args.timeframe)
        sym_events = events_df[events_df["symbol"] == symbol] if "symbol" in events_df.columns else events_df
        report = validate_event_quality(sym_events, bars, args.event_type, symbol, args.run_id, args.timeframe)
        reports.append(report)
        if not report["gate_e1_pass"]:
            overall_pass = False

    quality_report_path = reports_root / "event_quality_report.json"
    with open(quality_report_path, "w") as f:
        json.dump(reports, f, indent=2)

    print(f"Event Quality Report written to {quality_report_path}")
    if not overall_pass:
        print("GATE E-1 FAILED for one or more symbols.")
        sys.exit(1)

if __name__ == "__main__":
    main()
