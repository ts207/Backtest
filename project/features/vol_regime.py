import numpy as np
import pandas as pd

def calculate_rv_percentile_24h(close_series: pd.Series, window: int = 60, lookback: int = 1440) -> pd.Series:
    log_ret = np.log(close_series / close_series.shift(1))
    rv = log_ret.rolling(window).std()
    # PIT-safe rolling rank: rank of current value within a lookback window
    rv_rank = rv.rolling(lookback).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1])
    return rv_rank
