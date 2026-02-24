import pandas as pd
from pathlib import Path

path = Path("/home/tstuv/backtest/Backtest/data/reports/phase2/discovery_2025_q1/LIQUIDATION_CASCADE/phase2_candidates_raw.parquet")

if not path.exists():
    print("File does not exist")
else:
    try:
        df = pd.read_parquet(path)
        print(f"Rows: {len(df)}")
    except Exception as e:
        print(f"Error: {e}")
