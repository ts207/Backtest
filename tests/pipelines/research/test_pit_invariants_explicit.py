from __future__ import annotations

import pandas as pd
import pytest

def test_explicit_pit_invariant_no_lookahead_merge_asof():
    """
    Explicit invariant test verifying that Point-In-Time (PIT) boundaries
    are strictly respected when joining event 'produced artifacts' 
    (which have enter_ts/timestamp) with feature artifacts.
    """
    events = pd.DataFrame({
        "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT"],
        "event_id": ["e1", "e2", "e3"],
        "timestamp": pd.to_datetime(["2024-01-01T10:01:00Z", "2024-01-01T10:05:00Z", "2024-01-01T10:06:00Z"], utc=True),
        "enter_ts": pd.to_datetime(["2024-01-01T10:01:00Z", "2024-01-01T10:05:00Z", "2024-01-01T10:06:00Z"], utc=True),
    })
    
    features = pd.DataFrame({
        "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT"],
        "timestamp": pd.to_datetime(["2024-01-01T10:00:00Z", "2024-01-01T10:05:00Z", "2024-01-01T10:10:00Z"], utc=True),
        "feature_val": [10.0, 20.0, 30.0],
    })
    
    events_sorted = events.sort_values("enter_ts")
    features_sorted = features.sort_values("timestamp")
    
    # Expected pattern for PIT safe joins in the codebase
    merged = pd.merge_asof(
        events_sorted,
        features_sorted.rename(columns={"timestamp": "feature_ts"}),
        left_on="enter_ts",
        right_on="feature_ts",
        by="symbol",
        direction="backward",
    )
    
    assert len(merged) == 3
    
    # Invariant: the joined feature's timestamp MUST NEVER exceed the event's enter_ts
    assert (merged["feature_ts"] <= merged["enter_ts"]).all(), "Lookahead leak detected! Feature timestamp is strictly > event enter_ts"
    
    # Specific assertions on the boundary logic
    assert merged.loc[0, "feature_val"] == 10.0  # e1 @ 10:01 gets 10:00 feature
    assert merged.loc[1, "feature_val"] == 20.0  # e2 @ 10:05 gets 10:05 feature (exact match allowed)
    assert merged.loc[2, "feature_val"] == 20.0  # e3 @ 10:06 gets 10:05 feature
    
    # Future leakage would incorrectly assign feature_val 30.0 to e3
    assert merged.loc[2, "feature_val"] != 30.0

def test_explicit_pit_invariant_forward_returns_leakage_guard():
    """
    Explicit invariant test verifying that when computing forward returns 
    for evaluation, we correctly shift/calculate relative to the entry_lag_bars
    and do not leak into the entry bar itself.
    """
    features = pd.DataFrame({
        "symbol": ["BTCUSDT"] * 5,
        "timestamp": pd.to_datetime([
            "2024-01-01T10:00:00Z",
            "2024-01-01T10:05:00Z",  # Event occurs here
            "2024-01-01T10:10:00Z",  # Entry bar (lag=1)
            "2024-01-01T10:15:00Z",
            "2024-01-01T10:20:00Z",
        ], utc=True),
        "close": [100.0, 100.0, 50.0, 25.0, 25.0],
    })
    
    # Suppose entry_lag=1. We enter at 10:10 close.
    # The return from entry (10:10) to 10:20 should be 25/50 - 1 = -50%
    # Not 24/100...
    
    # Simulate the codebase's manual forward return calculation from phase2
    # usually done via features['close'].pct_change(...) shifted
    event_idx = 1
    lag = 1
    horizon_bars = 2
    
    entry_idx = event_idx + lag
    exit_idx = entry_idx + horizon_bars
    
    assert entry_idx < len(features)
    assert exit_idx < len(features)
    
    entry_price = features.iloc[entry_idx]["close"]
    exit_price = features.iloc[exit_idx]["close"]
    
    # PIT Check: The entry price must be taken strictly AFTER the event bar if lag >= 1
    assert features.iloc[entry_idx]["timestamp"] > features.iloc[event_idx]["timestamp"]
    
    # Calculation check
    fwd_ret = (exit_price / entry_price) - 1.0
    assert fwd_ret == -0.5
