import pandas as pd
import sys

path = "/home/tstuv/backtest/Backtest/data/lake/runs/discovery_2025_q1/cleaned/perp/BTCUSDT/bars_5m/year=2025/month=01/bars_BTCUSDT_5m_2025-01.parquet"
try:
    df = pd.read_parquet(path)
    print(f"Columns: {df.columns.tolist()}")
except Exception as e:
    print(f"Error reading {path}: {e}")
