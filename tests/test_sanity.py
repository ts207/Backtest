import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines._lib.sanity import (
    assert_funding_event_grid,
    assert_monotonic_utc_timestamp,
    coerce_timestamps_to_hour,
    infer_and_apply_funding_scale,
    is_constant_series,
)


@pytest.mark.parametrize(
    "values, expected_scale",
    [
        ([0.0001, -0.0002, 0.00005], 1.0),
        ([0.01, -0.02, 0.005], 0.01),
        ([1.0, -2.0, 0.5], 0.0001),
    ],
)
def test_infer_funding_scale(values, expected_scale):
    df = pd.DataFrame({"funding_rate": values})
    scaled, scale_used = infer_and_apply_funding_scale(df, "funding_rate")
    assert scale_used == expected_scale
    expected = np.array(values) * expected_scale
    assert np.allclose(scaled["funding_rate_scaled"].values, expected)


def test_infer_funding_scale_known_sources_stay_decimal():
    df = pd.DataFrame(
        {
            "funding_rate": [0.0001, -0.0002, 0.00005],
            "source": ["archive_monthly", "archive_daily", "api"],
        }
    )
    scaled, scale_used = infer_and_apply_funding_scale(df, "funding_rate")
    assert scale_used == 1.0
    assert np.allclose(scaled["funding_rate_scaled"].values, df["funding_rate"].values)


def test_infer_funding_scale_known_source_rejects_non_decimal():
    df = pd.DataFrame({"funding_rate": [0.02], "source": ["api"]})
    with pytest.raises(ValueError, match="must already be decimal"):
        infer_and_apply_funding_scale(df, "funding_rate")


def test_constant_funding_detection():
    constant_series = pd.Series([0.0001, 0.0001, 0.0001])
    varying_series = pd.Series([0.0001, 0.0002, 0.0001])
    assert is_constant_series(constant_series) is True
    assert is_constant_series(varying_series) is False


def test_timestamp_utc_enforcement():
    df = pd.DataFrame({"timestamp": pd.date_range("2024-01-01", periods=3, freq="h")})
    with pytest.raises(ValueError):
        assert_monotonic_utc_timestamp(df, "timestamp")


def test_funding_event_grid_validation():
    ok = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-01 00:00", "2024-01-01 08:00", "2024-01-01 16:00"], utc=True
            )
        }
    )
    assert_funding_event_grid(ok, "timestamp", expected_hours=8)

    bad = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-01 01:00", "2024-01-01 09:00", "2024-01-01 17:00"], utc=True
            )
        }
    )
    with pytest.raises(ValueError):
        assert_funding_event_grid(bad, "timestamp", expected_hours=8)


def test_coerce_timestamps_to_hour():
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-01 08:00:00.001", "2024-01-01 16:00:00.000"], utc=True
            )
        }
    )
    rounded, adjusted = coerce_timestamps_to_hour(df, "timestamp")
    assert adjusted == 1
    assert rounded["timestamp"].iloc[0] == pd.Timestamp("2024-01-01 08:00:00", tz="UTC")
