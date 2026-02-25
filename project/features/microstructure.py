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

def calculate_amihud(close: pd.Series, volume: pd.Series, window: int = 24) -> pd.Series:
    """
    Amihud Illiquidity = avg(|return| / dollar_volume)
    """
    log_ret = np.log(close / close.shift(1))
    dollar_vol = close * volume
    illiq = log_ret.abs() / (dollar_vol.replace(0.0, np.nan))
    return illiq.rolling(window).mean()

def calculate_kyle_lambda(close: pd.Series, buy_vol: pd.Series, sell_vol: pd.Series, window: int = 24) -> pd.Series:
    """
    Kyle's Lambda: price change = lambda * net_order_flow
    Uses rolling regression.
    """
    price_change = close.diff()
    net_flow = buy_vol - sell_vol
    
    def rolling_ols(y: np.ndarray, x: np.ndarray) -> float:
        if len(y) < 2: return np.nan
        # Simple OLS: beta = cov(x,y) / var(x)
        cov = np.cov(x, y)[0, 1]
        var = np.var(x)
        if var == 0: return 0.0
        return float(cov / var)

    # Use a more efficient approach if possible, but for Phase 1 this is fine.
    # Note: we need to align price_change and net_flow.
    df = pd.DataFrame({"y": price_change, "x": net_flow}).dropna()
    if df.empty:
        return pd.Series(np.nan, index=close.index)
        
    res = []
    # To keep it simple and correct, we'll use a manual loop or apply on indices
    # However, pandas rolling apply only takes one series.
    # We can use a trick: rolling on a combined index or just implement the math with rolling means/covs.
    
    # lambda = [E(XY) - E(X)E(Y)] / [E(X^2) - (E(X))^2]
    exy = (df["x"] * df["y"]).rolling(window).mean()
    ex = df["x"].rolling(window).mean()
    ey = df["y"].rolling(window).mean()
    ex2 = (df["x"]**2).rolling(window).mean()
    
    num = exy - (ex * ey)
    denom = ex2 - (ex**2)
    
    lambdas = num / denom.replace(0.0, np.nan)
    return lambdas.reindex(close.index)

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
