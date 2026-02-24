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


def test_merge_optional_oi_requires_unique_timestamps(monkeypatch):
    oi = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"], utc=True),
            "open_interest": [10.0, 11.0],
        }
    )

    def fake_read_optional(path):
        return oi.copy() if "open_interest" in str(path) else pd.DataFrame()

    monkeypatch.setattr(build_features_v1, "_read_optional_time_series", fake_read_optional)

    with pytest.raises(ValueError, match="Open interest timestamps must be unique"):
        build_features_v1._merge_optional_oi_liquidation(_bars(), symbol="BTCUSDT", market="perp", run_id="r1")


def test_merge_optional_liquidation_requires_unique_timestamps(monkeypatch):
    liq = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"], utc=True),
            "notional_usd": [100.0, 120.0],
            "event_count": [1, 1],
        }
    )

    def fake_read_optional(path):
        if "open_interest" in str(path):
            return pd.DataFrame()
        if "liquidation_snapshot" in str(path):
            return liq.copy()
        return pd.DataFrame()

    monkeypatch.setattr(build_features_v1, "_read_optional_time_series", fake_read_optional)

    with pytest.raises(ValueError, match="Liquidation timestamps must be unique"):
        build_features_v1._merge_optional_oi_liquidation(_bars(), symbol="BTCUSDT", market="perp", run_id="r1")


def test_merge_optional_oi_cardinality_guard(monkeypatch):
    oi = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"], utc=True),
            "open_interest": [10.0, 12.0],
        }
    )

    def fake_read_optional(path):
        return oi.copy() if "open_interest" in str(path) else pd.DataFrame()

    def fake_merge_asof(left, right, on, direction, **kwargs):
        return left.iloc[:-1].copy()

    monkeypatch.setattr(build_features_v1, "_read_optional_time_series", fake_read_optional)
    monkeypatch.setattr(build_features_v1.pd, "merge_asof", fake_merge_asof)

    with pytest.raises(ValueError, match="Cardinality mismatch after OI merge"):
        build_features_v1._merge_optional_oi_liquidation(_bars(), symbol="BTCUSDT", market="perp", run_id="r1")


def test_merge_optional_oi_respects_staleness_tolerance(monkeypatch):
    oi = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "open_interest": [10.0],
        }
    )

    def fake_read_optional(path):
        return oi.copy() if "open_interest" in str(path) else pd.DataFrame()

    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T05:00:00Z"], utc=True),
            "open": [1.0, 1.0],
            "high": [1.0, 1.0],
            "low": [1.0, 1.0],
            "close": [1.0, 1.0],
        }
    )

    monkeypatch.setattr(build_features_v1, "_read_optional_time_series", fake_read_optional)
    out = build_features_v1._merge_optional_oi_liquidation(bars, symbol="BTCUSDT", market="perp", run_id="r1")

    assert out.loc[0, "oi_notional"] == 10.0
    assert pd.isna(out.loc[1, "oi_notional"])
