import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

import engine.runner as runner
from engine.pnl import compute_returns
from engine.runner import _strategy_returns


class _StubStrategy:
    required_features = []

    def generate_positions(self, bars: pd.DataFrame, features: pd.DataFrame, params: dict) -> pd.Series:
        return pd.Series([1] * len(bars), index=pd.DatetimeIndex(bars["timestamp"]))


def test_returns_gap_safe():
    close = pd.Series([100.0, 101.0, np.nan, 103.0, 104.0], index=pd.date_range("2024-01-01", periods=5, freq="15min", tz="UTC"))
    ret = compute_returns(close)
    assert np.isnan(ret.iloc[0])
    assert np.isnan(ret.iloc[2])
    assert np.isnan(ret.iloc[3])
    assert ret.iloc[4] == pytest.approx(104.0 / 103.0 - 1.0)


def test_runner_forces_flat_on_nan_returns(monkeypatch):
    timestamps = pd.date_range("2024-01-01 00:00", periods=5, freq="15min", tz="UTC")
    bars = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": [100.0, 101.0, np.nan, 103.0, 104.0],
            "high": [101.0, 102.0, np.nan, 104.0, 105.0],
            "low": [99.0, 100.0, np.nan, 102.0, 103.0],
            "close": [100.0, 101.0, np.nan, 103.0, 104.0],
            "is_gap": [False, False, True, False, False],
            "gap_len": [0, 0, 1, 0, 0],
        }
    )
    features = pd.DataFrame({"timestamp": timestamps})

    monkeypatch.setattr(runner, "get_strategy", lambda name: _StubStrategy())
    result = _strategy_returns("BTCUSDT", bars, features, "stub", {}, 0.0)

    df = result.data
    nan_ret_mask = df["ret"].isna()
    assert (df.loc[nan_ret_mask, "pos"] == 0).all()
    assert (df.loc[nan_ret_mask, "pnl"] == 0).all()
    assert result.diagnostics["forced_flat_bars"] > 0
