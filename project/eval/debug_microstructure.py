import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from features.microstructure import calculate_roll_spread, calculate_vpin
from pipelines._lib.io_utils import list_parquet_files, read_parquet

def debug():
    symbol = "BTCUSDT"
    perp_dir = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "bars_1m"
    df = read_parquet(list_parquet_files(perp_dir))
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp")
    
    df["roll_spread"] = calculate_roll_spread(df["close"])
    df["returns"] = df["close"].pct_change()
    
    print("Roll Spread Stats:")
    print(df["roll_spread"].describe())
    
    # Check returns autocorrelation
    autocorr = df["returns"].autocorr(lag=1)
    print("Returns Lag-1 Autocorrelation:")
    print(autocorr)
    
    # Check VPIN vs Vol
    df_15m = df.resample("15min", on="timestamp").agg({
        "close": "last",
        "volume": "sum",
        "taker_base_volume": "sum"
    })
    df_15m["returns"] = np.log(df_15m["close"] / df_15m["close"].shift(1))
    df_15m["rv"] = df_15m["returns"].rolling(20).std()
    df_15m["vpin"] = calculate_vpin(df_15m["volume"], df_15m["taker_base_volume"])
    
    corr = df_15m["vpin"].corr(df_15m["rv"])
    print("VPIN vs RV Correlation:")
    print(corr)

if __name__ == "__main__":
    debug()
