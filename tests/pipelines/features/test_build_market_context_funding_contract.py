from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.features import build_market_context


def _feature_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                    "2026-01-01T00:15:00Z",
                ],
                utc=True,
            ),
            "close": [100.0, 101.0, 100.5, 101.5],
            "rv_96": [0.1, 0.2, 0.15, 0.22],
            "rv_pct_17280": [0.2, 0.4, 0.6, 0.8],
            "range_96": [2.0, 2.0, 2.0, 2.0],
            "range_med_2880": [4.0, 4.0, 4.0, 4.0],
        }
    )


def test_build_market_context_uses_canonical_funding_rate_scaled():
    features = _feature_frame()
    features["funding_rate_scaled"] = [0.0002, -0.0002, 0.0003, -0.0003]
    features["funding_rate"] = [999.0, 999.0, 999.0, 999.0]

    out = build_market_context._build_market_context(symbol="BTCUSDT", features=features)

    assert out["funding_rate_bps"].tolist() == pytest.approx([2.0, -2.0, 3.0, -3.0])
    assert set(out["carry_state_code"].tolist()) == {1.0, -1.0}


def test_build_market_context_requires_funding_rate_scaled_column():
    features = _feature_frame()
    features["funding_rate"] = [0.0002, -0.0002, 0.0003, -0.0003]

    with pytest.raises(ValueError, match="missing funding_rate_scaled"):
        build_market_context._build_market_context(symbol="BTCUSDT", features=features)


def test_build_market_context_rejects_funding_gaps():
    features = _feature_frame()
    features["funding_rate_scaled"] = [0.0002, None, 0.0003, -0.0003]

    with pytest.raises(ValueError, match="contains gaps"):
        build_market_context._build_market_context(symbol="BTCUSDT", features=features)
