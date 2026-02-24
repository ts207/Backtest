from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.features import build_features_v1


def _bars() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z", "2026-01-01T00:10:00Z"],
                utc=True,
            ),
            "open": [1.0, 1.0, 1.0],
            "high": [1.0, 1.0, 1.0],
            "low": [1.0, 1.0, 1.0],
            "close": [1.0, 1.0, 1.0],
        }
    )


def test_revision_lag_minutes_uses_5m_bar_size():
    assert build_features_v1._revision_lag_minutes(0) == 0
    assert build_features_v1._revision_lag_minutes(1) == 5
    assert build_features_v1._revision_lag_minutes(3) == 15


def test_merge_funding_rates_requires_unique_timestamps():
    funding = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"], utc=True),
            "funding_rate_scaled": [0.01, 0.02],
        }
    )

    with pytest.raises(ValueError, match="Funding timestamps must be unique"):
        build_features_v1._merge_funding_rates(_bars(), funding, symbol="BTCUSDT")


def test_merge_funding_rates_preserves_row_count():
    funding = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:10:00Z"], utc=True),
            "funding_rate_scaled": [0.01, 0.03],
        }
    )

    out = build_features_v1._merge_funding_rates(_bars(), funding, symbol="BTCUSDT")

    assert len(out) == 3
    assert "funding_rate_scaled" in out.columns


def test_merge_funding_rates_uses_backward_asof_for_sparse_updates():
    bars = _bars()
    bars = pd.concat(
        [
            bars,
            pd.DataFrame(
                {
                    "timestamp": pd.to_datetime(["2026-01-01T00:15:00Z", "2026-01-01T00:20:00Z"], utc=True),
                    "open": [1.0, 1.0],
                    "high": [1.0, 1.0],
                    "low": [1.0, 1.0],
                    "close": [1.0, 1.0],
                }
            ),
        ],
        ignore_index=True,
    ).sort_values("timestamp").reset_index(drop=True)
    funding = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "funding_rate_scaled": [0.01],
        }
    )

    out = build_features_v1._merge_funding_rates(bars, funding, symbol="BTCUSDT")

    assert out["funding_rate_scaled"].notna().all()
    assert set(out["funding_rate_scaled"].round(8).tolist()) == {0.01}


def test_merge_funding_rates_respects_max_staleness():
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T12:00:00Z"], utc=True),
            "open": [1.0, 1.0],
            "high": [1.0, 1.0],
            "low": [1.0, 1.0],
            "close": [1.0, 1.0],
        }
    )
    funding = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "funding_rate_scaled": [0.01],
        }
    )

    out = build_features_v1._merge_funding_rates(bars, funding, symbol="BTCUSDT")

    assert out.loc[0, "funding_rate_scaled"] == 0.01
    assert pd.isna(out.loc[1, "funding_rate_scaled"])
