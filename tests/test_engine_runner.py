import json
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from engine.pnl import compute_pnl, compute_returns
from engine.runner import _aggregate_portfolio, run_engine
from pipelines.backtest import backtest_vol_compression_v1 as backtest_stage
from strategies.vol_compression_v1 import VolCompressionV1


def _write_fixture(root: Path, symbol: str = "BTCUSDT") -> None:
    timestamps = pd.date_range("2024-01-01 00:00", periods=10, freq="15min", tz="UTC")
    bars = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": [100.0] * 10,
            "high": [101.0, 102.0, 124.0, 103.0, 102.0, 101.0, 102.0, 101.0, 102.0, 101.0],
            "low": [99.0] * 10,
            "close": [100.5, 101.5, 102.0, 101.0, 100.0, 100.2, 100.1, 100.3, 100.4, 100.5],
            "is_gap": [False] * 10,
            "gap_len": [0] * 10,
        }
    )
    features = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": bars["open"],
            "high": bars["high"],
            "low": bars["low"],
            "close": bars["close"],
            "funding_event_ts": pd.NaT,
            "funding_rate_scaled": 0.0,
            "rv_pct_17280": [5.0] * 10,
            "high_96": [100.0] * 10,
            "low_96": [90.0] * 10,
            "range_96": [10.0] * 10,
            "range_med_2880": [20.0] * 10,
        }
    )

    features_dir = root / "lake" / "features" / "perp" / symbol / "15m" / "features_v1" / "year=2024" / "month=01"
    bars_dir = root / "lake" / "cleaned" / "perp" / symbol / "bars_15m" / "year=2024" / "month=01"
    features_dir.mkdir(parents=True, exist_ok=True)
    bars_dir.mkdir(parents=True, exist_ok=True)
    features.to_parquet(features_dir / f"features_{symbol}_v1_2024-01.parquet", index=False)
    bars.to_parquet(bars_dir / f"bars_{symbol}_15m_2024-01.parquet", index=False)


def test_strategy_positions_values_and_tz() -> None:
    timestamps = pd.date_range("2024-01-01 00:00", periods=6, freq="15min", tz="UTC")
    bars = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": [100.0] * 6,
            "high": [101.0, 102.0, 124.0, 103.0, 102.0, 101.0],
            "low": [99.0] * 6,
            "close": [100.5, 101.5, 102.0, 101.0, 100.0, 100.2],
            "is_gap": [False] * 6,
            "gap_len": [0] * 6,
        }
    )
    features = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": bars["open"],
            "high": bars["high"],
            "low": bars["low"],
            "close": bars["close"],
            "funding_event_ts": pd.NaT,
            "funding_rate_scaled": 0.0,
            "rv_pct_17280": [5.0] * 6,
            "high_96": [100.0] * 6,
            "low_96": [90.0] * 6,
            "range_96": [10.0] * 6,
            "range_med_2880": [20.0] * 6,
        }
    )

    strategy = VolCompressionV1()
    positions = strategy.generate_positions(bars, features, {"trade_day_timezone": "UTC", "one_trade_per_day": True})

    assert positions.index.tz is not None
    assert set(positions.unique()).issubset({-1, 0, 1})


def test_compute_pnl_next_bar_and_cost() -> None:
    close = pd.Series([100.0, 110.0, 121.0], index=pd.date_range("2024-01-01", periods=3, freq="15min", tz="UTC"))
    pos = pd.Series([0, 1, 1], index=close.index)
    ret = compute_returns(close)
    pnl = compute_pnl(pos, ret, cost_bps=10)

    assert pytest.approx(pnl.iloc[0], abs=1e-9) == 0.0
    assert pytest.approx(pnl.iloc[1], abs=1e-9) == -0.001
    assert pytest.approx(pnl.iloc[2], abs=1e-9) == 0.1


def test_portfolio_aggregation_aligns() -> None:
    t0 = pd.Timestamp("2024-01-01 00:00", tz="UTC")
    t1 = pd.Timestamp("2024-01-01 00:15", tz="UTC")
    t2 = pd.Timestamp("2024-01-01 00:30", tz="UTC")

    frame_a = pd.DataFrame(
        {
            "timestamp": [t0, t1],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "pos": [1, 1],
            "ret": [0.1, 0.2],
            "pnl": [0.1, 0.2],
        }
    )
    frame_b = pd.DataFrame(
        {
            "timestamp": [t1, t2],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "pos": [1, 1],
            "ret": [0.05, 0.05],
            "pnl": [0.05, 0.05],
        }
    )

    portfolio = _aggregate_portfolio({"a": frame_a, "b": frame_b})
    assert portfolio["portfolio_pnl"].tolist() == [0.1, 0.25, 0.05]


def test_run_engine_determinism(tmp_path: Path) -> None:
    _write_fixture(tmp_path)
    result_1 = run_engine(
        run_id="run_a",
        symbols=["BTCUSDT"],
        strategies=["vol_compression_v1"],
        params={"trade_day_timezone": "UTC", "one_trade_per_day": True},
        cost_bps=6.0,
        project_root=tmp_path,
    )
    result_2 = run_engine(
        run_id="run_a",
        symbols=["BTCUSDT"],
        strategies=["vol_compression_v1"],
        params={"trade_day_timezone": "UTC", "one_trade_per_day": True},
        cost_bps=6.0,
        project_root=tmp_path,
    )

    file_path = result_1["engine_dir"] / "strategy_returns_vol_compression_v1.csv"
    assert file_path.read_text() == (result_2["engine_dir"] / "strategy_returns_vol_compression_v1.csv").read_text()
    assert result_1["portfolio"].equals(result_2["portfolio"])


def test_backtest_pipeline_outputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_fixture(tmp_path)
    monkeypatch.setattr(backtest_stage, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        backtest_stage,
        "load_configs",
        lambda paths: {
            "fee_bps_per_side": 4,
            "slippage_bps_per_fill": 2,
            "trade_day_timezone": "UTC",
        },
    )

    run_id = "run_pipeline"
    args = [
        "backtest_vol_compression_v1.py",
        "--run_id",
        run_id,
        "--symbols",
        "BTCUSDT",
        "--force",
        "1",
    ]
    monkeypatch.setattr(sys, "argv", args)
    assert backtest_stage.main() == 0

    trades_dir = tmp_path / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / run_id
    assert (trades_dir / "metrics.json").exists()
    assert (trades_dir / "equity_curve.csv").exists()
    assert (trades_dir / "fee_sensitivity.json").exists()

    engine_dir = tmp_path / "runs" / run_id / "engine"
    assert (engine_dir / "strategy_returns_vol_compression_v1.csv").exists()
    assert (engine_dir / "portfolio_returns.csv").exists()
    metrics = json.loads((engine_dir / "metrics.json").read_text())
    assert "portfolio" in metrics
