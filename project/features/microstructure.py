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

def calculate_roll_spread_bps(close: pd.Series, window: int = 24) -> pd.Series:
    """
    Roll Spread (bps) = 2 * sqrt(-cov(dp_t, dp_{t-1})) / price * 10000
    Matches spec: roll_spread_bps
    """
    diff = close.diff()
    cov = diff.rolling(window).cov(diff.shift(1))
    roll_spread = 2 * np.sqrt(np.maximum(0, -cov)) / close * 10000
    return roll_spread

def calculate_amihud_illiquidity(close: pd.Series, volume: pd.Series, window: int = 24) -> pd.Series:
    """
    Amihud Illiquidity = avg(|return| / dollar_volume)
    Matches spec: amihud_illiquidity
    """
    log_ret = np.log(close / close.shift(1))
    dollar_vol = close * volume
    # Use replacement of 0 with NaN to avoid division by zero and propagate NaNs correctly
    illiq = log_ret.abs() / (dollar_vol.replace(0.0, np.nan))
    return illiq.rolling(window).mean()

def calculate_kyle_lambda(close: pd.Series, buy_vol: pd.Series, sell_vol: pd.Series, window: int = 24) -> pd.Series:
    """
    Kyle's Lambda: price change = lambda * net_order_flow
    Uses rolling regression logic.
    """
    price_change = close.diff()
    net_flow = buy_vol - sell_vol
    
    # lambda = [E(XY) - E(X)E(Y)] / [E(X^2) - (E(X))^2]
    exy = (net_flow * price_change).rolling(window).mean()
    ex = net_flow.rolling(window).mean()
    ey = price_change.rolling(window).mean()
    ex2 = (net_flow**2).rolling(window).mean()
    
    num = exy - (ex * ey)
    denom = ex2 - (ex**2)
    
    lambdas = num / denom.replace(0.0, np.nan)
    return lambdas.reindex(close.index)

def calculate_vpin_score(volume: pd.Series, buy_volume: pd.Series, window: int = 50) -> pd.Series:
    """
    VPIN score using rolling volume windows.
    Matches spec: vpin_score
    """
    sell_volume = volume - buy_volume
    oi = (buy_volume - sell_volume).abs()
    
    # VPIN = sum|V_buy - V_sell| / Total_Volume over window
    vpin = oi.rolling(window).sum() / volume.rolling(window).sum()
    return vpin

def calculate_effective_spread_bps(tob_df: pd.DataFrame) -> pd.Series:
    """
    Real-time bid-ask spread relative to mid-price (bps).
    Matches spec: effective_spread_bps
    """
    mid = (tob_df['bid_price'] + tob_df['ask_price']) / 2
    spread = (tob_df['ask_price'] - tob_df['bid_price']) / mid * 10000
    return spread
