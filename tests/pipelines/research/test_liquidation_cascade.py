import numpy as np
import pandas as pd
import pytest
from pipelines.research.analyze_liquidation_cascade import detect_cascades

def test_detect_cascades_basic():
    ts = pd.date_range("2024-09-01", periods=100, freq="5min", tz="UTC")
    df = pd.DataFrame({
        "timestamp": ts,
        "liquidation_notional": [10.0] * 100,
        "oi_delta_1h": [0.0] * 100,
        "oi_notional": [1000.0] * 100,
        "close": [100.0] * 100,
        "high": [101.0] * 100,
        "low": [99.0] * 100,
        "vol_regime": ["low"] * 100
    })
    
    # Trigger a cascade at index 50
    df.loc[50, "liquidation_notional"] = 100.0
    df.loc[50, "oi_delta_1h"] = -50.0
    df.loc[50, "oi_notional"] = 950.0
    df.loc[50, "low"] = 95.0
    
    # median will be 10.0. 100.0 > 3.0 * 10.0
    events = detect_cascades(df, "BTCUSDT", liq_median_window=20, liq_multiplier=3.0)
    
    assert len(events) == 1
    assert events.iloc[0]["total_liquidation_notional"] == 100.0
    assert events.iloc[0]["oi_reduction_pct"] == pytest.approx(0.05) # (1000 - 950) / 1000
    assert events.iloc[0]["price_drawdown"] == pytest.approx(0.05) # (100 - 95) / 100

def test_detect_cascades_multi_bar():
    ts = pd.date_range("2024-09-01", periods=100, freq="5min", tz="UTC")
    df = pd.DataFrame({
        "timestamp": ts,
        "liquidation_notional": [10.0] * 100,
        "oi_delta_1h": [0.0] * 100,
        "oi_notional": [1000.0] * 100,
        "close": [100.0] * 100,
        "high": [101.0] * 100,
        "low": [99.0] * 100
    })
    
    # Cascade over 3 bars
    df.loc[50:52, "liquidation_notional"] = 100.0
    df.loc[50:52, "oi_delta_1h"] = -50.0
    df.loc[50, "oi_notional"] = 950.0
    df.loc[51, "oi_notional"] = 900.0
    df.loc[52, "oi_notional"] = 850.0
    df.loc[52, "low"] = 90.0
    
    events = detect_cascades(df, "BTCUSDT", liq_median_window=20, liq_multiplier=3.0)
    
    assert len(events) == 1
    assert events.iloc[0]["duration_bars"] == 3
    assert events.iloc[0]["total_liquidation_notional"] == 300.0
    assert events.iloc[0]["oi_reduction_pct"] == pytest.approx(0.15) # (1000 - 850) / 1000
    assert events.iloc[0]["price_drawdown"] == pytest.approx(0.10) # (100 - 90) / 100

def test_detect_cascades_no_trigger_on_positive_oi():
    ts = pd.date_range("2024-09-01", periods=100, freq="5min", tz="UTC")
    df = pd.DataFrame({
        "timestamp": ts,
        "liquidation_notional": [10.0] * 100,
        "oi_delta_1h": [0.0] * 100,
        "oi_notional": [1000.0] * 100,
        "close": [100.0] * 100,
        "high": [101.0] * 100,
        "low": [99.0] * 100
    })
    
    # High liquidation but positive OI delta (short squeeze?)
    df.loc[50, "liquidation_notional"] = 100.0
    df.loc[50, "oi_delta_1h"] = 50.0
    
    events = detect_cascades(df, "BTCUSDT", liq_median_window=20, liq_multiplier=3.0)
    assert len(events) == 0
