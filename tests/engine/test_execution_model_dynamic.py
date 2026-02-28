import pandas as pd
import numpy as np
import pytest
from project.engine.execution_model import estimate_transaction_cost_bps

def test_dynamic_cost_model_basic():
    idx = pd.date_range("2024-01-01", periods=10, freq="5min")
    frame = pd.DataFrame({
        "timestamp": idx,
        "spread_bps": [1.0] * 10,
        "tob_coverage": [1.0] * 10,
        "close": [100.0] * 10,
        "high": [100.1] * 10,
        "low": [99.9] * 10,
        "quote_volume": [1000000.0] * 10
    }).set_index("timestamp")
    turnover = pd.Series([1000.0] * 10, index=idx)
    
    config = {
        "cost_model": "dynamic",
        "min_tob_coverage": 0.8,
        "base_fee_bps": 2.0,
        "base_slippage_bps": 1.0,
        "spread_weight": 0.5,
        "volatility_weight": 0.0,
        "liquidity_weight": 0.0,
        "impact_weight": 0.0
    }
    
    # cost = base_fee + spread_weight * spread_bps = 2.0 + 0.5 * 1.0 = 2.5
    costs = estimate_transaction_cost_bps(frame, turnover, config)
    assert pytest.approx(costs.iloc[0]) == 2.5

def test_dynamic_cost_fallback_on_low_coverage():
    idx = pd.date_range("2024-01-01", periods=10, freq="5min")
    frame = pd.DataFrame({
        "timestamp": idx,
        "spread_bps": [10.0] * 10, # Wide spread
        "tob_coverage": [0.5] * 10, # Low coverage
        "close": [100.0] * 10,
        "high": [100.1] * 10,
        "low": [99.9] * 10,
        "quote_volume": [1000000.0] * 10
    }).set_index("timestamp")
    turnover = pd.Series([1000.0] * 10, index=idx)
    
    config = {
        "cost_model": "dynamic",
        "min_tob_coverage": 0.8,
        "base_fee_bps": 2.0,
        "base_slippage_bps": 1.0, # fallback slippage
        "spread_weight": 1.0
    }
    
    # Since coverage 0.5 < 0.8, should fallback to static: base_fee + base_slippage = 3.0
    # instead of dynamic: 2.0 + 10.0 = 12.0
    costs = estimate_transaction_cost_bps(frame, turnover, config)
    assert pytest.approx(costs.iloc[0]) == 3.0

def test_dynamic_cost_impact_scaling():
    idx = pd.date_range("2024-01-01", periods=10, freq="5min")
    frame = pd.DataFrame({
        "timestamp": idx,
        "spread_bps": [1.0] * 10,
        "tob_coverage": [1.0] * 10,
        "close": [100.0] * 10,
        "high": [100.1] * 10,
        "low": [99.9] * 10,
        "quote_volume": [1000000.0] * 10
    }).set_index("timestamp")
    
    config = {
        "cost_model": "dynamic",
        "impact_weight": 1.0,
        "spread_weight": 0.0,
        "base_fee_bps": 0.0,
        "base_slippage_bps": 0.0
    }
    
    # Small turnover
    costs_small = estimate_transaction_cost_bps(frame, pd.Series([1000.0]*10, index=idx), config)
    # Large turnover
    costs_large = estimate_transaction_cost_bps(frame, pd.Series([500000.0]*10, index=idx), config)
    
    assert costs_large.iloc[0] > costs_small.iloc[0]

if __name__ == "__main__":
    pytest.main([__file__])
