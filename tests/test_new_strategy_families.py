import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from strategies.registry import get_strategy, list_strategies


def _fixture() -> tuple[pd.DataFrame, pd.DataFrame]:
    ts = pd.date_range("2024-01-01 00:00", periods=220, freq="15min", tz="UTC")
    close = [100.0 + 0.03 * i + (0.7 if i % 13 == 0 else 0.0) for i in range(len(ts))]
    bars = pd.DataFrame(
        {
            "timestamp": ts,
            "open": close,
            "high": [x + 1.0 for x in close],
            "low": [x - 1.0 for x in close],
            "close": close,
            "is_gap": [False] * len(ts),
            "gap_len": [0] * len(ts),
        }
    )
    funding = [0.00008 if i < 110 else -0.00008 for i in range(len(ts))]
    features = pd.DataFrame(
        {
            "timestamp": ts,
            "funding_rate_scaled": funding,
            "rv_pct_2880": [8.0] * len(ts),
            "high_96": [101.0] * len(ts),
            "low_96": [99.0] * len(ts),
            "range_96": [1.0] * len(ts),
            "range_med_480": [2.0] * len(ts),
        }
    )
    return bars, features


def test_registry_contains_new_families() -> None:
    names = list_strategies()
    assert "funding_carry_v1" in names
    assert "intraday_reversion_v1" in names


def test_new_family_output_contracts() -> None:
    bars, features = _fixture()
    strategy_params = {
        "funding_carry_v1": {"n": 64, "thr": 0.00002},
        "intraday_reversion_v1": {"n": 32, "thr": 1.0},
    }
    for name in ["funding_carry_v1", "intraday_reversion_v1"]:
        strategy = get_strategy(name)
        positions = strategy.generate_positions(bars, features, strategy_params[name])
        assert positions.index.tz is not None
        assert positions.index.is_monotonic_increasing
        assert len(positions) == len(bars)
        assert set(positions.unique()).issubset({-1, 0, 1})
        assert positions.isna().sum() == 0


def test_funding_carry_no_lookahead_last_funding_perturbation() -> None:
    bars, features = _fixture()
    strategy = get_strategy("funding_carry_v1")
    base = strategy.generate_positions(bars, features, {"n": 64, "thr": 0.00002})

    features2 = features.copy()
    features2.loc[features2.index[-1], "funding_rate_scaled"] = 0.02
    perturbed = strategy.generate_positions(bars, features2, {"n": 64, "thr": 0.00002})

    pd.testing.assert_series_equal(base.iloc[:-1], perturbed.iloc[:-1], check_names=False)


def test_intraday_reversion_no_lookahead_last_price_perturbation() -> None:
    bars, features = _fixture()
    strategy = get_strategy("intraday_reversion_v1")
    base = strategy.generate_positions(bars, features, {"n": 32, "thr": 1.0})

    bars2 = bars.copy()
    bars2.loc[bars2.index[-1], "close"] = float(bars2.loc[bars2.index[-1], "close"]) * 10.0
    perturbed = strategy.generate_positions(bars2, features, {"n": 32, "thr": 1.0})

    pd.testing.assert_series_equal(base.iloc[:-1], perturbed.iloc[:-1], check_names=False)


def test_funding_carry_gap_flattening() -> None:
    bars, features = _fixture()
    bars.loc[120:122, "is_gap"] = True
    bars.loc[120:122, "gap_len"] = 1

    strategy = get_strategy("funding_carry_v1")
    positions = strategy.generate_positions(bars, features, {"n": 64, "thr": 0.00002, "gap_bars": 0})
    assert (positions.iloc[120:123] == 0).all()


def test_intraday_reversion_gap_flattening() -> None:
    bars, features = _fixture()
    bars.loc[150:153, "gap_len"] = 2

    strategy = get_strategy("intraday_reversion_v1")
    positions = strategy.generate_positions(bars, features, {"n": 32, "thr": 1.0, "gap_bars": 0})
    assert (positions.iloc[150:154] == 0).all()
