import os
import sys
import pandas as pd
import numpy as np
import yaml
from pathlib import Path
from typing import Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "project"))
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT / "data"))

from project.features.microstructure import (
    calculate_roll_spread,
    calculate_amihud_illiquidity,
    calculate_vpin
)
from pipelines._lib.io_utils import list_parquet_files, read_parquet

def load_concept_spec(concept_id: str) -> Dict[str, Any]:
    spec_path = PROJECT_ROOT / "spec" / "concepts" / f"{concept_id}.yaml"
    if not spec_path.exists():
        raise FileNotFoundError(f"Spec not found: {spec_path}")
    with open(spec_path, "r") as f:
        return yaml.safe_load(f)

def get_test_threshold(spec: Dict[str, Any], test_id: str) -> float:
    for test in spec.get("tests", []):
        if test["id"] == test_id:
            return float(test.get("threshold", 0.0))
    raise ValueError(f"Test ID {test_id} not found in spec")

def run_acceptance_tests(symbol: str, run_id: str):
    concept_id = "C_MICROSTRUCTURE_METRICS"
    print(f"Running {concept_id} Acceptance Tests for {symbol} (Run: {run_id})")
    
    # Load Spec
    spec = load_concept_spec(concept_id)
    print(f"Loaded Spec: {spec['name']}")
    
    # 1. Load Data
    perp_dir = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "bars_1m"
    files = list_parquet_files(perp_dir)
    if not files:
        print(f"FAILED: No cleaned bars found for {symbol}")
        return False
    
    df = read_parquet(files)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp")
    
    # 2. Calculate Features
    df["roll_spread_bps"] = calculate_roll_spread(df["close"], window=20)
    df["returns"] = df["close"].pct_change()
    df["amihud"] = calculate_amihud_illiquidity(df["returns"], df["quote_volume"], window=20)
    df["vpin"] = calculate_vpin(df["volume"], df["taker_base_volume"], window=5)
    
    results = {}
    
    # T_MICRO_01: Positive Spread
    target_01 = get_test_threshold(spec, "T_MICRO_01")
    valid_bars = df[df["volume"] > 0].dropna(subset=["roll_spread_bps"])
    pos_spread_ratio = (valid_bars["roll_spread_bps"] > 0).mean()
    results["T_MICRO_01"] = {
        "passed": pos_spread_ratio >= target_01,
        "metric": float(pos_spread_ratio),
        "target": target_01,
        "details": f"Positive spread in {pos_spread_ratio:.2%} of bars (Target: >={target_01:.2%})"
    }
    
    # T_MICRO_02: Volatility Correlation
    target_02 = get_test_threshold(spec, "T_MICRO_02")
    df["rv"] = df["returns"].rolling(5).std()
    corr = df["vpin"].corr(df["rv"], method="spearman")
    # Criteria in YAML is abs(corr), so we check magnitude
    results["T_MICRO_02"] = {
        "passed": abs(corr) >= target_02,
        "metric": float(corr),
        "target": target_02,
        "details": f"Spearman correlation (VPIN vs RV): {corr:.4f} (Target abs: >={target_02})"
    }
    
    # T_MICRO_03: Impact Sensitivity
    target_03 = get_test_threshold(spec, "T_MICRO_03")
    high_vol_mask = df["rv"] > df["rv"].quantile(0.9)
    illiq_high = df[high_vol_mask == True]["amihud"].median()
    illiq_low = df[high_vol_mask == False]["amihud"].median()
    ratio = illiq_high / illiq_low if illiq_low > 0 else 0
    results["T_MICRO_03"] = {
        "passed": ratio >= target_03,
        "metric": float(ratio),
        "target": target_03,
        "details": f"Amihud ratio (High Vol / Normal): {ratio:.2f}x (Target: >={target_03:.2f}x)"
    }
    
    print("\nAcceptance Report:")
    all_passed = True
    for tid, res in results.items():
        status = "PASS" if res["passed"] else "FAIL"
        print(f"[{status}] {tid}: {res['details']}")
        if not res["passed"]:
            all_passed = False
            
    return all_passed

if __name__ == "__main__":
    success = run_acceptance_tests("BTCUSDT", "acceptance_run_1")
    sys.exit(0 if success else 1)
