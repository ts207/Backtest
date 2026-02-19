
import os
import pandas as pd
from datetime import datetime, timedelta

def main():
    root = os.environ.get("BACKTEST_DATA_ROOT", "/tmp/backtest_smoke_root")
    os.makedirs(root, exist_ok=True)

    # synthetic 2-day 15m bars
    ts = pd.date_range("2024-01-01", periods=2*24*4, freq="15min", tz="UTC")
    df = pd.DataFrame({
        "timestamp": ts,
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.0,
        "volume": 1.0,
        "funding_rate_scaled": 0.008,
    })

    out = os.path.join(root, "synthetic_clean.csv")
    df.to_csv(out, index=False)
    print("Synthetic dataset written:", out)

if __name__ == "__main__":
    main()
