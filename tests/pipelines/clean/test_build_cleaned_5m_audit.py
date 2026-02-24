
import sys
from pathlib import Path
from datetime import timezone

import pandas as pd
import numpy as np
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.clean.build_cleaned_5m import _align_funding, _gap_lengths, FUNDING_MAX_STALENESS

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
    # 5m bar builder should use a 5m exclusive end residue.
    start_ts = pd.Timestamp("2026-01-01 00:00:00", tz=timezone.utc)
    end_ts = pd.Timestamp("2026-01-01 23:55:00", tz=timezone.utc)

    end_exclusive = end_ts + pd.Timedelta(minutes=5)
    full_index = pd.date_range(start=start_ts, end=end_exclusive - pd.Timedelta(minutes=5), freq="5min", tz=timezone.utc)

    assert full_index[-1] == end_ts
    assert len(full_index) == 288  # (24 * 60) / 5


def test_align_funding_marks_stale_rows_missing():
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-02 08:05:00",
                    "2026-01-02 08:10:00",
                ],
                utc=True,
            )
        }
    )
    funding = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01 00:00:00"], utc=True),
            "funding_rate_scaled": [0.0001],
        }
    )

    aligned, missing_pct = _align_funding(bars, funding)
    assert missing_pct == 1.0
    assert aligned["funding_missing"].all()
    assert aligned["funding_rate_scaled"].isna().all()
    assert FUNDING_MAX_STALENESS == pd.Timedelta("8h")
