import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

import engine.runner as runner
from engine.runner import run_engine


class _StubStrategy:
    required_features = ["rv_pct_2880"]

    def generate_positions(self, bars: pd.DataFrame, features: pd.DataFrame, params: dict) -> pd.Series:
        return pd.Series([1] * len(bars), index=pd.DatetimeIndex(bars["timestamp"]))


def _write_gap_fixture(root: Path, symbol: str = "BTCUSDT") -> None:
    timestamps = pd.date_range("2024-01-01 00:00", periods=6, freq="15min", tz="UTC")
    bars = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": [100.0, 101.0, 102.0, None, 104.0, 105.0],
            "high": [101.0, 102.0, 103.0, None, 105.0, 106.0],
            "low": [99.0, 100.0, 101.0, None, 103.0, 104.0],
            "close": [100.0, 101.0, 102.0, None, 104.0, 105.0],
            "is_gap": [False, False, False, True, False, False],
            "gap_len": [0, 0, 0, 1, 0, 0],
        }
    )
    features = pd.DataFrame(
        {
            "timestamp": timestamps,
            "rv_pct_2880": [5.0] * 6,
        }
    )

    features_dir = root / "lake" / "features" / "perp" / symbol / "15m" / "features_v1" / "year=2024" / "month=01"
    bars_dir = root / "lake" / "cleaned" / "perp" / symbol / "bars_15m" / "year=2024" / "month=01"
    features_dir.mkdir(parents=True, exist_ok=True)
    bars_dir.mkdir(parents=True, exist_ok=True)
    features.to_parquet(features_dir / f"features_{symbol}_v1_2024-01.parquet", index=False)
    bars.to_parquet(bars_dir / f"bars_{symbol}_15m_2024-01.parquet", index=False)


def test_runner_diagnostics(tmp_path: Path, monkeypatch):
    _write_gap_fixture(tmp_path)
    monkeypatch.setattr(runner, "get_strategy", lambda name: _StubStrategy())

    result = run_engine(
        run_id="diag",
        symbols=["BTCUSDT"],
        strategies=["stub"],
        params={"trade_day_timezone": "UTC", "one_trade_per_day": True},
        cost_bps=0.0,
        data_root=tmp_path,
    )

    diagnostics = result["metrics"]["diagnostics"]["strategies"]["stub"]
    assert diagnostics["nan_return_bars"] > 0
    assert diagnostics["forced_flat_bars"] > 0
    assert "missing_feature_pct" in diagnostics
