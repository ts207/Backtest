import pandas as pd
import sys

path = "data/lake/runs/discovery_2025_q1/features/perp/BTCUSDT/5m/features_v1/year=2026/month=01/features_BTCUSDT_v1_2026-01.parquet"
try:
    df = pd.read_parquet(path)
    print(f"Rows: {len(df)}")
    if "liquidation_notional" in df.columns:
        print(f"Liquidation notional stats: {df['liquidation_notional'].describe()}")
        print(f"Non-zero count: {(df['liquidation_notional'] > 0).sum()}")
    else:
        print("liquidation_notional column missing")
    
    if "oi_delta_1h" in df.columns:
        print(f"OI delta stats: {df['oi_delta_1h'].describe()}")
except Exception as e:
    print(f"Error: {e}")
