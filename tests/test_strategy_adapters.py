import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from strategies.registry import get_strategy, list_strategies


def _sample_bars_and_features() -> tuple[pd.DataFrame, pd.DataFrame]:
    ts = pd.date_range("2024-01-01", periods=6, freq="15min", tz="UTC")
    bars = pd.DataFrame(
        {
            "timestamp": ts,
            "open": [100, 101, 102, 103, 104, 105],
            "high": [101, 102, 103, 104, 105, 106],
            "low": [99, 100, 101, 102, 103, 104],
            "close": [100.5, 101.5, 102.5, 103.5, 104.5, 105.5],
            "is_gap": [False] * 6,
            "gap_len": [0] * 6,
        }
    )
    features = pd.DataFrame(
        {
            "timestamp": ts,
            "range_96": [1.0] * 6,
            "high_96": [102, 103, 104, 105, 106, 107],
            "low_96": [98, 99, 100, 101, 102, 103],
            "rv_pct_2880": [5.0, 10.0, 15.0, 20.0, 10.0, 5.0],
            "range_med_480": [1.2] * 6,
            "fp_norm_due": [0, 0, 1, 0, 0, 0],
            "fp_severity": [0.2, 1.8, 1.9, 0.3, 0.1, 0.0],
            "basis_zscore": [0.1, 1.4, 1.6, 0.7, 0.2, 0.1],
            "onchain_flow_mc": [0.2, 0.4, 0.1, -0.3, -0.5, 0.0],
        }
    )
    return bars, features


def test_registry_includes_strategy_adapters() -> None:
    names = set(list_strategies())
    assert "vol_compression_v1" in names
    assert "liquidity_refill_lag_v1" in names
    assert "liquidity_absence_gate_v1" in names
    assert "forced_flow_exhaustion_v1" in names
    assert "funding_extreme_reversal_v1" in names
    assert "cross_venue_desync_v1" in names
    assert "liquidity_vacuum_v1" in names
    assert "carry_funding_v1" in names
    assert "mean_reversion_exhaustion_v1" in names
    assert "spread_desync_v1" in names
    assert "onchain_flow_v1" in names


def test_strategy_adapter_generates_valid_positions() -> None:
    bars, features = _sample_bars_and_features()
    strategy = get_strategy("funding_extreme_reversal_v1")
    out = strategy.generate_positions(bars, features, params={"trade_day_timezone": "UTC"})

    assert len(out) == len(bars)
    assert out.index.tz is not None
    assert set(out.unique()).issubset({-1, 0, 1})


@pytest.mark.parametrize(
    "strategy_id",
    [
        "funding_extreme_reversal_v1",
        "forced_flow_exhaustion_v1",
        "liquidity_refill_lag_v1",
        "cross_venue_desync_v1",
    ],
)
def test_new_strategy_ids_are_resolvable_and_instantiable(strategy_id: str) -> None:
    bars, features = _sample_bars_and_features()
    strategy = get_strategy(strategy_id)
    out = strategy.generate_positions(bars, features, params={"trade_day_timezone": "UTC"})

    assert strategy is not None
    assert len(out) == len(bars)
    assert set(out.unique()).issubset({-1, 0, 1})
def test_strategy_adapter_filters_irrelevant_params(monkeypatch) -> None:
    strategy = get_strategy("funding_extreme_reversal_v1")
    captured = {}

    def _fake_generate_positions(bars, features, params):
        captured.update(params)
        return pd.Series([0] * len(bars), index=pd.DatetimeIndex(bars["timestamp"]))

    monkeypatch.setattr(strategy._base, "generate_positions", _fake_generate_positions)
    bars, features = _sample_bars_and_features()
    strategy.generate_positions(
        bars,
        features,
        params={
            "trade_day_timezone": "UTC",
            "compression_rv_pct_max": 10.0,
            "funding_percentile_entry_min": 98.0,
        },
    )

    assert "trade_day_timezone" in captured
    assert "compression_rv_pct_max" in captured
    assert "funding_percentile_entry_min" not in captured
def test_new_strategies_generate_positions_and_metadata() -> None:
    bars, features = _sample_bars_and_features()

    strategies = [
        "carry_funding_v1",
        "mean_reversion_exhaustion_v1",
        "spread_desync_v1",
        "onchain_flow_v1",
    ]
    for name in strategies:
        out = get_strategy(name).generate_positions(bars, features, params={"trade_day_timezone": "UTC"})
        assert len(out) == len(bars)
        assert out.index.tz is not None
        assert set(out.unique()).issubset({-1, 0, 1})
        assert "strategy_metadata" in out.attrs
        assert out.attrs["strategy_metadata"].get("strategy_id") == name
        assert "family" in out.attrs["strategy_metadata"]
        assert isinstance(out.attrs["strategy_metadata"].get("key_params"), dict)


def test_symbol_specific_strategy_name_is_resolvable() -> None:
    bars, features = _sample_bars_and_features()
    strategy = get_strategy("funding_extreme_reversal_v1_BTCUSDT")
    out = strategy.generate_positions(bars, features, params={"trade_day_timezone": "UTC"})

    assert len(out) == len(bars)
    assert "strategy_metadata" in out.attrs
    metadata = out.attrs["strategy_metadata"]
    assert metadata.get("strategy_id") == "funding_extreme_reversal_v1_BTCUSDT"
    assert metadata.get("strategy_symbol") == "BTCUSDT"
    assert metadata.get("base_strategy_id") == "funding_extreme_reversal_v1"
