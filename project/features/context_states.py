import pandas as pd
import numpy as np

def calculate_ms_vol_state(rv_pct: pd.Series) -> pd.Series:
    """
    Volatility Dimension:
    0: LOW (0-33%)
    1: MID (33-66%)
    2: HIGH (66-95%)
    3: SHOCK (>95%)
    """
    bins = [0.0, 33.0, 66.0, 95.0, 100.0]
    labels = [0.0, 1.0, 2.0, 3.0]
    return pd.cut(rv_pct, bins=bins, labels=labels, include_lowest=True).astype(float)

def calculate_ms_liq_state(quote_volume: pd.Series, window: int = 288) -> pd.Series:
    """
    Liquidity Dimension based on rolling 24h quote volume quantiles:
    0: THIN (Bottom 20%)
    1: NORMAL (20-80%)
    2: FLUSH (Top 20%)
    """
    def pct_rank(values: np.ndarray) -> float:
        if len(values) == 0: return np.nan
        last = values[-1]
        return float(np.sum(values <= last) / len(values))

    min_p = min(window, max(24, window // 10))
    ranks = quote_volume.rolling(window=window, min_periods=min_p).apply(pct_rank, raw=True)
    
    bins = [0.0, 0.2, 0.8, 1.0]
    labels = [0.0, 1.0, 2.0]
    return pd.cut(ranks, bins=bins, labels=labels, include_lowest=True).astype(float)

def calculate_ms_oi_state(oi_delta_1h: pd.Series, window: int = 288) -> pd.Series:
    """
    OI Dimension based on 1h OI delta z-score:
    0: DECEL (z < -1.5)
    1: STABLE (-1.5 <= z <= 1.5)
    2: ACCEL (z > 1.5)
    """
    min_p = min(window, max(24, window // 10))
    rolling = oi_delta_1h.rolling(window=window, min_periods=min_p)
    mean = rolling.mean()
    std = rolling.std().replace(0.0, np.nan)
    z = (oi_delta_1h - mean) / std
    
    state = pd.Series(1.0, index=oi_delta_1h.index)
    state[z < -1.5] = 0.0
    state[z > 1.5] = 2.0
    # Keep NaN if inputs are NaN
    state[oi_delta_1h.isna()] = np.nan
    return state.astype(float)

def calculate_ms_funding_state(funding_rate_bps: pd.Series, window: int = 96) -> pd.Series:
    """
    Funding Dimension based on trailing 8h sign consistency:
    0: NEUTRAL
    1: PERSISTENT (Same sign for 70%+ of the window)
    2: EXTREME (Abs mean > 2.0 bps)
    """
    min_p = min(window, max(12, window // 8))
    abs_mean = funding_rate_bps.rolling(window=window, min_periods=min_p).mean().abs()
    
    def sign_consistency(values: np.ndarray) -> float:
        if len(values) == 0: return 0.0
        pos = np.sum(values > 0)
        neg = np.sum(values < 0)
        return float(max(pos, neg) / len(values))

    consistency = funding_rate_bps.rolling(window=window, min_periods=min_p).apply(sign_consistency, raw=True)
    
    state = pd.Series(0.0, index=funding_rate_bps.index)
    state[consistency >= 0.7] = 1.0
    state[abs_mean >= 2.0] = 2.0
    # Keep NaN if inputs are NaN
    state[funding_rate_bps.isna()] = np.nan
    return state.astype(float)

def encode_context_state_code(vol: pd.Series, liq: pd.Series, oi: pd.Series, fnd: pd.Series) -> pd.Series:
    """
    Generate ms_context_state_code as a unique permutation.
    Format: VLOF (Vol, Liq, OI, Funding)
    e.g. Vol=3, Liq=0, OI=2, Funding=1 -> 3021
    """
    # Use 1000, 100, 10, 1 to position the digits
    code = (
        vol.fillna(0) * 1000 +
        liq.fillna(0) * 100 +
        oi.fillna(0) * 10 +
        fnd.fillna(0) * 1
    )
    # If any are NaN, should the code be NaN? 
    # For now, fill with 0 to avoid massive breakage, but maybe better to keep NaN.
    # The requirement says "materialized in the market context".
    return code.astype(float)
