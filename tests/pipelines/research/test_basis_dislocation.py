import numpy as np
import pandas as pd
import pytest
from pipelines.research.analyze_basis_dislocation import detect_basis_dislocations

def test_detect_basis_dislocations_basic():
    ts = pd.date_range("2024-09-01", periods=100, freq="5min", tz="UTC")
    perp_df = pd.DataFrame({
        "timestamp": ts,
        "close": [100.0] * 100,
        "vol_regime": ["low"] * 100
    })
    spot_df = pd.DataFrame({
        "timestamp": ts,
        "close": [100.0] * 100
    })
    
    # Create some background variance in basis to avoid NaN in z-score
    # Rows 0-20: 1 bps
    perp_df.loc[0:20, "close"] = 100.01 
    # Rows 21-40: -1 bps
    perp_df.loc[21:40, "close"] = 99.99
    
    # Trigger basis shock at index 60 (well after window warmup)
    perp_df.loc[60, "close"] = 105.0 # 5% = 500 bps
    
    events = detect_basis_dislocations(perp_df, spot_df, "BTCUSDT", z_threshold=2.0, lookback_window=40)
    
    assert len(events) >= 1
    # Check that we caught the big one
    found_big = any(abs(e["basis_bps"] - 500.0) < 1.0 for _, e in events.iterrows())
    assert found_big
