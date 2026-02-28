from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from pipelines.report.qa_data_layer import (
    QCSeverity,
    check_duplicate_bars,
    check_funding_gaps,
    check_return_outliers,
)


def test_return_outliers_detected():
    ts = pd.date_range("2023-01-01", periods=500, freq="5min", tz="UTC")
    close = pd.Series(100.0, index=ts)
    close.iloc[200] = 200.0  # 100% return spike → outlier
    result = check_return_outliers(close, z_threshold=5.0)
    assert result["outlier_count"] >= 1
    assert result["severity"] in ("warning", "critical")


def test_no_outliers_in_clean_data():
    ts = pd.date_range("2023-01-01", periods=500, freq="5min", tz="UTC")
    close = pd.Series(np.linspace(100, 102, 500), index=ts)
    result = check_return_outliers(close, z_threshold=5.0)
    assert result["outlier_count"] == 0
    assert result["severity"] == "ok"


def test_duplicate_bars_detected():
    ts = pd.date_range("2023-01-01", periods=10, freq="5min", tz="UTC")
    ts_dup = ts.append(ts[:2])  # 2 duplicates
    result = check_duplicate_bars(pd.Series(ts_dup))
    assert result["duplicate_count"] == 2
    assert result["severity"] != "ok"


# --- Additional coverage tests ---


def test_return_outliers_critical_threshold():
    """Five or more outliers should yield critical severity."""
    ts = pd.date_range("2023-01-01", periods=500, freq="5min", tz="UTC")
    close = pd.Series(100.0, index=ts)
    # Insert 6 large spikes that will be far beyond any z_threshold=3
    for i in [50, 100, 150, 200, 250, 300]:
        close.iloc[i] = 1000.0
    result = check_return_outliers(close, z_threshold=3.0, critical_count=5)
    assert result["severity"] == "critical"
    assert result["outlier_count"] >= 5


def test_return_outliers_empty_series():
    """Empty series should return ok with zero outliers."""
    result = check_return_outliers(pd.Series([], dtype=float))
    assert result["outlier_count"] == 0
    assert result["severity"] == "ok"


def test_return_outliers_single_element():
    """Single-element series produces empty pct_change — should be ok."""
    result = check_return_outliers(pd.Series([100.0]))
    assert result["outlier_count"] == 0
    assert result["severity"] == "ok"


def test_duplicate_bars_no_duplicates():
    ts = pd.date_range("2023-01-01", periods=10, freq="5min", tz="UTC")
    result = check_duplicate_bars(pd.Series(ts))
    assert result["duplicate_count"] == 0
    assert result["severity"] == "ok"


def test_duplicate_bars_critical_threshold():
    """More than 5 duplicates should yield critical severity."""
    ts = pd.date_range("2023-01-01", periods=10, freq="5min", tz="UTC")
    ts_dup = ts.append(ts[:6])  # 6 duplicates
    result = check_duplicate_bars(pd.Series(ts_dup))
    assert result["duplicate_count"] == 6
    assert result["severity"] == "critical"


def test_funding_gaps_no_gaps():
    ts = pd.date_range("2023-01-01", periods=30, freq="8h", tz="UTC")
    result = check_funding_gaps(ts)
    assert result["gap_count"] == 0
    assert result["severity"] == "ok"


def test_funding_gaps_detected():
    """A gap of >12h (1.5x the 8h interval) should be flagged."""
    ts = pd.DatetimeIndex(
        [
            pd.Timestamp("2023-01-01 00:00", tz="UTC"),
            pd.Timestamp("2023-01-01 08:00", tz="UTC"),
            pd.Timestamp("2023-01-02 08:00", tz="UTC"),  # 24h gap — large
        ]
    )
    result = check_funding_gaps(ts)
    assert result["gap_count"] >= 1
    assert result["severity"] != "ok"
    assert result["max_gap_hours"] >= 24.0


def test_funding_gaps_empty():
    result = check_funding_gaps(pd.DatetimeIndex([]))
    assert result["gap_count"] == 0
    assert result["severity"] == "ok"


def test_funding_gaps_single_entry():
    ts = pd.DatetimeIndex([pd.Timestamp("2023-01-01", tz="UTC")])
    result = check_funding_gaps(ts)
    assert result["gap_count"] == 0
    assert result["severity"] == "ok"


def test_qcseverity_is_str_type():
    """QCSeverity is a type alias for str."""
    assert QCSeverity is str
