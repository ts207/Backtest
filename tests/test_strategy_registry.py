import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from strategies.registry import get_strategy, list_strategies


def _bars_and_features() -> tuple[pd.DataFrame, pd.DataFrame]:
    timestamps = pd.date_range("2024-01-01 00:00", periods=180, freq="15min", tz="UTC")
    close = [100.0 + 0.05 * i for i in range(180)]
    bars = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": close,
            "high": [x + 1.0 for x in close],
            "low": [x - 1.0 for x in close],
            "close": close,
            "is_gap": [False] * 180,
            "gap_len": [0] * 180,
        }
    )
    features = pd.DataFrame(
        {
            "timestamp": timestamps,
            "rv_pct_2880": [8.0] * 180,
            "high_96": [100.5] * 180,
            "low_96": [99.5] * 180,
            "range_96": [1.0] * 180,
            "range_med_480": [2.0] * 180,
        }
    )
    return bars, features


def test_registry_contains_core_strategies() -> None:
    names = list_strategies()
    assert "vol_compression_v1" in names
    assert "vol_compression_momentum_v1" in names
    assert "vol_compression_reversion_v1" in names
    assert "tsmom_v1" in names
    assert "funding_carry_v1" in names
    assert "intraday_reversion_v1" in names


def test_new_strategies_generate_valid_positions() -> None:
    bars, features = _bars_and_features()
    for name in ["vol_compression_momentum_v1", "vol_compression_reversion_v1", "tsmom_v1"]:
        strategy = get_strategy(name)
        positions = strategy.generate_positions(bars, features, {"trade_day_timezone": "UTC"})
        assert positions.index.tz is not None
        assert set(positions.unique()).issubset({-1, 0, 1})
        assert len(positions) == len(bars)


def test_tsmom_no_lookahead_on_last_bar_perturbation() -> None:
    bars, features = _bars_and_features()
    strategy = get_strategy("tsmom_v1")

    base = strategy.generate_positions(bars, features, {"fast_n": 16, "slow_n": 96, "band_bps": 10.0})

    perturbed_bars = bars.copy()
    perturbed_bars.loc[perturbed_bars.index[-1], "close"] = float(perturbed_bars.loc[perturbed_bars.index[-1], "close"]) * 10.0
    perturbed = strategy.generate_positions(perturbed_bars, features, {"fast_n": 16, "slow_n": 96, "band_bps": 10.0})

    pd.testing.assert_series_equal(base.iloc[:-1], perturbed.iloc[:-1], check_names=False)
