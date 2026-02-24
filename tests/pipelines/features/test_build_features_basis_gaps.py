from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.features import build_features_v1


def test_basis_features_keep_nan_when_spot_is_missing(monkeypatch):
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-01T00:00:00Z", "2024-01-01T00:06:00Z", "2024-01-01T00:12:00Z"],
                utc=True,
            ),
            "close": [100.0, 101.0, 102.0],
        }
    )

    spot = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z"], utc=True),
            "spot_close": [100.0],
        }
    )

    monkeypatch.setattr(build_features_v1, "_load_spot_close_reference", lambda symbol, run_id: spot)
    out = build_features_v1._add_basis_features(frame, symbol="BTCUSDT", run_id="r1", market="perp")

    assert out.loc[0, "basis_bps"] == 0.0
    assert pd.isna(out.loc[1, "basis_bps"])
    assert pd.isna(out.loc[2, "basis_bps"])
    assert pd.isna(out.loc[1, "basis_zscore"])
    assert pd.isna(out.loc[2, "basis_zscore"])
    assert out.loc[0, "basis_spot_coverage"] == 1.0 / 3.0


def test_spread_zscore_uses_spread_bps_not_basis(monkeypatch):
    n = 120
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "close": [100.0] * n,
            "spread_bps": list(range(n)),
        }
    )
    spot = pd.DataFrame(
        {
            "timestamp": frame["timestamp"],
            "spot_close": [100.0] * n,
        }
    )
    monkeypatch.setattr(build_features_v1, "_load_spot_close_reference", lambda symbol, run_id: spot)
    out = build_features_v1._add_basis_features(frame, symbol="BTCUSDT", run_id="r1", market="perp")

    assert out["basis_bps"].fillna(0.0).abs().max() == 0.0
    assert out["basis_zscore"].fillna(0.0).abs().max() == 0.0
    assert out["spread_zscore"].fillna(0.0).abs().max() > 0.0
