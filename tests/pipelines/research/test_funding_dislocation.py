import numpy as np
import pandas as pd
import pytest
from pipelines.research.analyze_funding_dislocation import detect_funding_dislocations

def test_detect_funding_dislocations_basic():
    ts = pd.date_range("2024-09-01", periods=100, freq="5min", tz="UTC")
    df = pd.DataFrame({
        "timestamp": ts,
        "close": [100.0] * 100,
        "funding_rate_scaled": [0.0001] * 100, # 1 bps
        "vol_regime": ["low"] * 100
    })
    
    # Trigger at index 50
    df.loc[50, "funding_rate_scaled"] = 0.0003 # 3 bps
    
    events = detect_funding_dislocations(df, "BTCUSDT", threshold_bps=2.0)
    
    assert len(events) == 1
    assert events.iloc[0]["fr_magnitude"] == pytest.approx(3.0)
    assert events.iloc[0]["fr_sign"] == 1.0

def test_detect_funding_dislocations_cooldown():
    ts = pd.date_range("2024-09-01", periods=100, freq="5min", tz="UTC")
    df = pd.DataFrame({
        "timestamp": ts,
        "close": [100.0] * 100,
        "funding_rate_scaled": [0.0] * 100,
        "vol_regime": ["low"] * 100
    })
    
    df.loc[50, "funding_rate_scaled"] = 0.0005
    df.loc[55, "funding_rate_scaled"] = 0.0005 # Within cooldown
    
    events = detect_funding_dislocations(df, "BTCUSDT", threshold_bps=2.0, cooldown_bars=12)
    assert len(events) == 1
    assert events.iloc[0]["enter_idx"] == 50
