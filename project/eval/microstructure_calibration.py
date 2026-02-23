import os
import sys
import pandas as pd
import numpy as np
import yaml
from pathlib import Path
from typing import Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from features.microstructure import (
    calculate_roll_spread,
    calculate_amihud_illiquidity,
    calculate_vpin
)
from pipelines._lib.io_utils import list_parquet_files, read_parquet

def load_concept_spec(concept_id: str) -> Dict[str, Any]:
    spec_path = PROJECT_ROOT.parent / "spec" / "concepts" / f"{concept_id}.yaml"
    if not spec_path.exists():
        raise FileNotFoundError(f"Spec not found: {spec_path}")
    with open(spec_path, "r") as f:
        return yaml.safe_load(f)

def run_calibration(symbols: list[str], run_id: str):
    print(f"Running Microstructure Calibration for {symbols} (Run: {run_id})")
    
    concept_id = "C_MICROSTRUCTURE_METRICS"
    spec = load_concept_spec(concept_id)
    
    rows = []
    
    for symbol in symbols:
        print(f"Processing {symbol}...")
        perp_dir = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "bars_1m"
        files = list_parquet_files(perp_dir)
        if not files:
            print(f"  No data for {symbol}")
            continue
        
        df = read_parquet(files)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.sort_values("timestamp")
        
        # Features
        df["roll_spread_bps"] = calculate_roll_spread(df["close"], window=20)
        df["returns"] = df["close"].pct_change()
        df["amihud"] = calculate_amihud_illiquidity(df["returns"], df["quote_volume"], window=20)
        df["vpin"] = calculate_vpin(df["volume"], df["taker_base_volume"], window=5)
        
        # T_MICRO_01 Stats
        valid_bars = df[df["volume"] > 0].dropna(subset=["roll_spread_bps"])
        pos_spread_ratio = (valid_bars["roll_spread_bps"] > 0).mean()
        roll_median = valid_bars["roll_spread_bps"].median()
        roll_p90 = valid_bars["roll_spread_bps"].quantile(0.9)
        
        # T_MICRO_02 Stats
        df["rv"] = df["returns"].rolling(5).std()
        corr_vpin_rv = df["vpin"].corr(df["rv"], method="spearman")
        
        # T_MICRO_03 Stats
        high_vol_mask = df["rv"] > df["rv"].quantile(0.9)
        illiq_high = df[high_vol_mask == True]["amihud"].median()
        illiq_low = df[high_vol_mask == False]["amihud"].median()
        amihud_ratio = illiq_high / illiq_low if illiq_low > 0 else 0
        
        rows.append({
            "symbol": symbol,
            "pos_roll_pct": pos_spread_ratio,
            "roll_median_bps": roll_median,
            "roll_p90_bps": roll_p90,
            "vpin_rv_corr": corr_vpin_rv,
            "amihud_ratio": amihud_ratio
        })
        
    res_df = pd.DataFrame(rows)
    print("\nCalibration Table:")
    print(res_df.to_markdown(index=False, floatfmt=".4f"))
    
    # Save to report
    out_dir = DATA_ROOT / "reports" / "calibration" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    res_df.to_csv(out_dir / "microstructure_calibration.csv", index=False)
    print(f"\nSaved to {out_dir / 'microstructure_calibration.csv'}")

if __name__ == "__main__":
    run_calibration(["BTCUSDT", "ETHUSDT", "SOLUSDT"], "acceptance_run_1")
