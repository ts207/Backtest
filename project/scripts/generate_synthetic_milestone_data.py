import argparse
import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir

def generate_synthetic_bars(symbol: str, start_date: str, end_date: str, output_dir: Path):
    start_ts = pd.Timestamp(start_date, tz="UTC")
    end_ts = pd.Timestamp(end_date, tz="UTC")
    freq = "5min"
    
    dates = pd.date_range(start=start_ts, end=end_ts, freq=freq)
    n = len(dates)
    
    # Random walk price
    np.random.seed(42 + hash(symbol) % 1000)
    returns = np.random.normal(0, 0.001, n)
    price = 10000 * np.exp(np.cumsum(returns)) # Start at 10k
    
    data = {
        "timestamp": dates,
        "open": price,
        "high": price * (1 + np.abs(np.random.normal(0, 0.0005, n))),
        "low": price * (1 - np.abs(np.random.normal(0, 0.0005, n))),
        "close": price * (1 + np.random.normal(0, 0.0001, n)),
        "volume": np.abs(np.random.normal(100, 50, n)),
        "quote_volume": np.abs(np.random.normal(1000000, 500000, n)),
        "taker_buy_volume": np.abs(np.random.normal(50, 25, n)),
        "taker_buy_quote_volume": np.abs(np.random.normal(500000, 250000, n)),
    }
    
    df = pd.DataFrame(data)
    
    # Save partitioned? No, just one file for simplicity or partitioned by day?
    # Existing pipeline expects partitions? Or one file?
    # build_features_v1 reads parquet.
    
    ensure_dir(output_dir)
    file_path = output_dir / f"{symbol}_synthetic_5m.parquet"
    if not file_path.exists():
        df.to_parquet(file_path, index=False)
        print(f"Generated synthetic bars for {symbol} at {file_path}")
    else:
        print(f"Data already exists for {symbol}, skipping.")

def generate_synthetic_funding(symbol: str, start_date: str, end_date: str, output_dir: Path):
    start_ts = pd.Timestamp(start_date, tz="UTC")
    end_ts = pd.Timestamp(end_date, tz="UTC")
    freq = "8H" # Funding usually 8h
    
    dates = pd.date_range(start=start_ts, end=end_ts, freq=freq)
    n = len(dates)
    
    data = {
        "timestamp": dates,
        "funding_rate": np.random.normal(0.0001, 0.00005, n), # Positive bias
    }
    
    df = pd.DataFrame(data)
    
    ensure_dir(output_dir)
    file_path = output_dir / f"{symbol}_synthetic_funding.parquet"
    if not file_path.exists():
        df.to_parquet(file_path, index=False)
        print(f"Generated synthetic funding for {symbol} at {file_path}")
    else:
        print(f"Funding data already exists for {symbol}, skipping.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True) # Used to scope data path if needed, but we write to lake/cleaned for now as base
    parser.add_argument("--start_date", default="2025-01-01")
    parser.add_argument("--end_date", default="2025-03-01")
    args = parser.parse_args()
    
    symbols = ["BTC", "ETH", "SOL"]
    
    # Write to Run-Scoped Lake to avoid polluting main lake? 
    # Or write to main lake? The plan said data/lake/cleaned...
    # Let's write to data/runs/{run_id}/cleaned/perp/ to be safe and isolated.
    
    cleaned_root = DATA_ROOT / "runs" / args.run_id / "cleaned" / "perp"
    
    for symbol in symbols:
        generate_synthetic_bars(symbol, args.start_date, args.end_date, cleaned_root / symbol / "bars_5m")
        # Funding rate location? features pipeline looks in lake/funding?
        # Usually ingested to lake/funding.
        # Let's put it in run scoped location too.
        generate_synthetic_funding(symbol, args.start_date, args.end_date, cleaned_root / symbol / "funding_rate")

if __name__ == "__main__":
    main()
