import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from engine import runner


class _AlwaysLongStrategy:
    required_features = []

    @staticmethod
    def generate_positions(bars: pd.DataFrame, features: pd.DataFrame, params: dict) -> pd.Series:
        out = pd.Series(1, index=pd.to_datetime(bars["timestamp"], utc=True), name="position")
        out.attrs["strategy_metadata"] = {"family": "carry"}
        return out


def test_strategy_returns_flattens_positions_for_ineligible_universe(monkeypatch) -> None:
    monkeypatch.setattr(runner, "get_strategy", lambda _: _AlwaysLongStrategy())

    ts = pd.date_range("2024-01-01", periods=4, freq="15min", tz="UTC")
    bars = pd.DataFrame(
        {
            "timestamp": ts,
            "open": [100.0, 101.0, 102.0, 103.0],
            "high": [101.0, 102.0, 103.0, 104.0],
            "low": [99.0, 100.0, 101.0, 102.0],
            "close": [100.0, 101.0, 102.0, 103.0],
        }
    )
    features = pd.DataFrame({"timestamp": ts, "funding_rate_scaled": [0.0, 0.001, 0.001, 0.001]})
    eligibility_mask = pd.Series([True, False, True, False], index=ts)

    result = runner._strategy_returns(
        symbol="BTCUSDT",
        bars=bars,
        features=features,
        strategy_name="funding_extreme_reversal_v1",
        params={},
        cost_bps=0.0,
        eligibility_mask=eligibility_mask,
    )

    frame = result.data.set_index("timestamp")
    assert int(result.diagnostics["ineligible_universe_bars"]) == 2
    assert frame.loc[ts[1], "pos"] == 0
    assert frame.loc[ts[3], "pos"] == 0
