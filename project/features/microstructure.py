import numpy as np
import pandas as pd
from typing import Dict, List

def calculate_roll(close_series: pd.Series, window: int = 24) -> pd.Series:
    """
    Standard Roll measure (raw units).
    """
    diff = close_series.diff()
    cov = diff.rolling(window).cov(diff.shift(1))
    return 2 * np.sqrt(np.maximum(0, -cov))

def calculate_roll_spread(close_series: pd.Series, window: int = 24) -> pd.Series:
    """
    Roll Spread (bps) = 2 * sqrt(-cov(dp_t, dp_{t-1})) / price * 10000
    """
    diff = close_series.diff()
    cov = diff.rolling(window).cov(diff.shift(1))
    roll_spread = 2 * np.sqrt(np.maximum(0, -cov)) / close_series * 10000
    return roll_spread

def calculate_amihud_illiquidity(returns: pd.Series, dollar_volume: pd.Series, window: int = 20) -> pd.Series:
    """
    Amihud Illiquidity = avg(|return| / dollar_volume)
    """
    # Use small epsilon to avoid div by zero
    illiq = returns.abs() / (dollar_volume + 1e-9)
    return illiq.rolling(window).mean()

def calculate_vpin(volume: pd.Series, buy_volume: pd.Series, window: int = 50) -> pd.Series:
    """
    VPIN using rolling volume windows.
    """
    sell_volume = volume - buy_volume
    oi = (buy_volume - sell_volume).abs()
    
    # Standard VPIN is sum(OI) / sum(V) over buckets.
    # Here we use a rolling window as a proxy for buckets.
    vpin = oi.rolling(window).sum() / volume.rolling(window).sum()
    return vpin

def calculate_effective_spread(tob_df: pd.DataFrame) -> pd.Series:
    mid = (tob_df['bid_price'] + tob_df['ask_price']) / 2
    spread = (tob_df['ask_price'] - tob_df['bid_price']) / mid * 10000
    return spread
