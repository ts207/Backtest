import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from strategies.registry import get_strategy, list_strategies


def _bars_and_features() -> tuple[pd.DataFrame, pd.DataFrame]:
    timestamps = pd.date_range("2024-01-01 00:00", periods=12, freq="15min", tz="UTC")
    bars = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": [100.0] * 12,
            "high": [101.0 + (i % 3) for i in range(12)],
            "low": [99.0] * 12,
            "close": [100.0 + (0.3 * i) for i in range(12)],
            "is_gap": [False] * 12,
            "gap_len": [0] * 12,
        }
    )
    features = pd.DataFrame(
        {
            "timestamp": timestamps,
            "rv_pct_2880": [8.0] * 12,
            "high_96": [100.5] * 12,
            "low_96": [99.5] * 12,
            "range_96": [1.0] * 12,
            "range_med_480": [2.0] * 12,
        }
    )
    return bars, features


def test_registry_contains_three_strategies() -> None:
    names = list_strategies()
    assert "vol_compression_v1" in names
    assert "vol_compression_momentum_v1" in names
    assert "vol_compression_reversion_v1" in names


def test_new_strategies_generate_valid_positions() -> None:
    bars, features = _bars_and_features()
    for name in ["vol_compression_momentum_v1", "vol_compression_reversion_v1"]:
        strategy = get_strategy(name)
        positions = strategy.generate_positions(bars, features, {"trade_day_timezone": "UTC"})
        assert positions.index.tz is not None
        assert set(positions.unique()).issubset({-1, 0, 1})
