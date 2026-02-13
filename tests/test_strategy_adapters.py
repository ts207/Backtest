import sys
from pathlib import Path

import pandas as pd

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
            "rv_pct_2880": [5.0] * 6,
            "range_med_480": [1.2] * 6,
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


def test_strategy_adapter_generates_valid_positions() -> None:
    bars, features = _sample_bars_and_features()
    strategy = get_strategy("funding_extreme_reversal_v1")
    out = strategy.generate_positions(bars, features, params={"trade_day_timezone": "UTC"})

    assert len(out) == len(bars)
    assert out.index.tz is not None
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
