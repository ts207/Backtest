from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.features import build_context_features


def test_align_funding_to_bars_uses_backward_asof_and_tolerance():
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z", "2026-01-01T00:10:00Z"],
                utc=True,
            )
        }
    )
    funding = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "funding_rate_scaled": [1e-6],
        }
    )

    out = build_context_features._align_funding_to_bars(bars, funding, symbol="BTCUSDT")

    assert len(out) == len(bars)
    assert out["funding_rate_scaled"].notna().all()
    assert set(out["funding_rate_scaled"].round(12).tolist()) == {1e-6}


def test_assert_complete_funding_series_rejects_alignment_gaps():
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T12:00:00Z"], utc=True)
        }
    )
    funding = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "funding_rate_scaled": [1e-6],
        }
    )
    aligned = build_context_features._align_funding_to_bars(bars, funding, symbol="BTCUSDT")

    with pytest.raises(ValueError, match="Funding alignment gaps"):
        build_context_features._assert_complete_funding_series(aligned, symbol="BTCUSDT")


def test_assert_complete_funding_series_rejects_funding_missing_flag():
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"], utc=True),
            "funding_rate_scaled": [1e-6, 2e-6],
            "funding_missing": [False, True],
        }
    )

    with pytest.raises(ValueError, match="Funding coverage gaps flagged"):
        build_context_features._assert_complete_funding_series(frame, symbol="BTCUSDT")

