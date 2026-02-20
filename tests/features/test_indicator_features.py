import pandas as pd
import numpy as np
import pytest
from project.features.vol_regime import calculate_rv_percentile_24h
from project.features.carry_state import calculate_funding_rate_bps

def test_vol_regime_feature():
    # Synthetic data
    np.random.seed(42)
    # Low vol period
    returns_low = np.random.normal(0, 0.0001, 1000)
    # High vol period
    returns_high = np.random.normal(0, 0.01, 500)
    returns = np.concatenate([returns_low, returns_high])
    close = 100 * np.exp(np.cumsum(returns))
    close = pd.Series(close)
    
    rv_rank = calculate_rv_percentile_24h(close, window=20, lookback=200)
    
    assert len(rv_rank) == 1500
    assert not rv_rank.isna().all()
    # At the end, rank should be high
    assert rv_rank.iloc[-1] > 0.8
    # In the middle (of low vol), rank should be lower (once lookback is full)
    assert rv_rank.iloc[500] < 0.8

def test_funding_rate_feature():
    fr = pd.Series([0.0001, -0.0001])
    fr_bps = calculate_funding_rate_bps(fr)
    assert fr_bps.iloc[0] == 1.0
    assert fr_bps.iloc[1] == -1.0
