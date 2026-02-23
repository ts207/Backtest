
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import pytest
from pipelines.clean.build_cleaned_5m import _align_funding, _gap_lengths

def test_gap_lengths():
    is_gap = pd.Series([False, True, True, False, True, False])
    expected = pd.Series([0, 2, 2, 0, 1, 0])
    pd.testing.assert_series_equal(_gap_lengths(is_gap), expected)

def test_align_funding_basic():
    bars = pd.DataFrame({
        "timestamp": pd.to_datetime([
            "2026-01-01 00:00:00",
            "2026-01-01 00:05:00",
            "2026-01-01 00:10:00"
        ], utc=True)
    })
    funding = pd.DataFrame({
        "timestamp": pd.to_datetime(["2026-01-01 00:00:00"], utc=True),
        "funding_rate_scaled": [0.0001]
    })
    
    # 8 hours = 480 minutes. 480 / 5 = 96 bars.
    # 0.0001 / 96 = 1.0416666666666667e-06
    
    aligned, missing_pct = _align_funding(bars, funding)
    
    assert missing_pct == 0.0
    assert len(aligned) == 3
    assert aligned["funding_rate_scaled"].iloc[0] == pytest.approx(0.0001 / 96)
    assert aligned["funding_event_ts"].iloc[0] == pd.Timestamp("2026-01-01 00:00:00", tz=timezone.utc)

def test_align_funding_missing():
    bars = pd.DataFrame({
        "timestamp": pd.to_datetime(["2026-01-01 00:00:00"], utc=True)
    })
    funding = pd.DataFrame(columns=["timestamp", "funding_rate_scaled"])
    
    aligned, missing_pct = _align_funding(bars, funding)
    assert missing_pct == 1.0
    assert np.isnan(aligned["funding_rate_scaled"].iloc[0])

def test_full_index_generation_residue():
    # This test demonstrates the 15m residue issue
    start_ts = pd.Timestamp("2026-01-01 00:00:00", tz=timezone.utc)
    end_ts = pd.Timestamp("2026-01-01 23:55:00", tz=timezone.utc)
    
    # Current logic:
    end_exclusive = end_ts + timedelta(minutes=15) # 00:10
    full_index = pd.date_range(start=start_ts, end=end_exclusive - timedelta(minutes=15), freq="5min", tz=timezone.utc)
    
    assert full_index[-1] == end_ts
    assert len(full_index) == 288 # (24 * 60) / 5
    
    # If we had 5m residue logic:
    end_exclusive_fixed = end_ts + timedelta(minutes=5)
    full_index_fixed = pd.date_range(start=start_ts, end=end_exclusive_fixed - timedelta(minutes=5), freq="5min", tz=timezone.utc)
    
    assert full_index_fixed[-1] == end_ts
    assert len(full_index_fixed) == 288
    
    # The residue doesn't break 288 bars case, but it's confusing and potentially wrong if end_ts is not aligned.
