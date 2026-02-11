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
from pipelines.report import make_report as report_stage
from strategies.vol_compression_v1 import VolCompressionV1


def _write_fixture(root: Path, symbol: str = "BTCUSDT", periods: int = 10, run_id: str | None = None) -> None:
    timestamps = pd.date_range("2024-01-01 00:00", periods=periods, freq="15min", tz="UTC")
    bars = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": [100.0] * periods,
            "high": [101.0 + (i % 3) for i in range(periods)],
            "low": [99.0] * periods,
            "close": [100.0 + (i * 0.1) for i in range(periods)],
            "is_gap": [False] * periods,
            "gap_len": [0] * periods,
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
            "rv_pct_2880": [5.0] * periods,
            "high_96": [100.0] * periods,
            "low_96": [90.0] * periods,
            "range_96": [10.0] * periods,
            "range_med_480": [20.0] * periods,
        }
    )

    if run_id:
        lake_root = root / "lake" / "runs" / run_id
    else:
        lake_root = root / "lake"
    features_dir = lake_root / "features" / "perp" / symbol / "15m" / "features_v1" / "year=2024" / "month=01"
    bars_dir = lake_root / "cleaned" / "perp" / symbol / "bars_15m" / "year=2024" / "month=01"
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
            "rv_pct_2880": [5.0] * 6,
            "high_96": [100.0] * 6,
            "low_96": [90.0] * 6,
            "range_96": [10.0] * 6,
            "range_med_480": [20.0] * 6,
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
        data_root=tmp_path,
    )
    result_2 = run_engine(
        run_id="run_a",
        symbols=["BTCUSDT"],
        strategies=["vol_compression_v1"],
        params={"trade_day_timezone": "UTC", "one_trade_per_day": True},
        cost_bps=6.0,
        data_root=tmp_path,
    )

    file_path = result_1["engine_dir"] / "strategy_returns_vol_compression_v1.csv"
    assert file_path.read_text() == (result_2["engine_dir"] / "strategy_returns_vol_compression_v1.csv").read_text()
    assert result_1["portfolio"].equals(result_2["portfolio"])


def test_run_engine_prefers_run_scoped_inputs(tmp_path: Path) -> None:
    _write_fixture(tmp_path, periods=10)
    _write_fixture(tmp_path, periods=6, run_id="run_scoped")

    result = run_engine(
        run_id="run_scoped",
        symbols=["BTCUSDT"],
        strategies=["vol_compression_v1"],
        params={"trade_day_timezone": "UTC", "one_trade_per_day": True},
        cost_bps=6.0,
        data_root=tmp_path,
    )
    frame = result["strategy_frames"]["vol_compression_v1"]
    assert len(frame) == 6




def test_run_engine_mixed_run_scoped_and_fallback_inputs(tmp_path: Path) -> None:
    _write_fixture(tmp_path, symbol="BTCUSDT", periods=10)
    _write_fixture(tmp_path, symbol="ETHUSDT", periods=10)
    _write_fixture(tmp_path, symbol="BTCUSDT", periods=6, run_id="run_mixed")

    result = run_engine(
        run_id="run_mixed",
        symbols=["BTCUSDT", "ETHUSDT"],
        strategies=["vol_compression_v1"],
        params={"trade_day_timezone": "UTC", "one_trade_per_day": True},
        cost_bps=6.0,
        data_root=tmp_path,
    )
    frame = result["strategy_frames"]["vol_compression_v1"]
    by_symbol = frame.groupby("symbol").size().to_dict()
    assert by_symbol["BTCUSDT"] == 6
    assert by_symbol["ETHUSDT"] == 10

def test_backtest_pipeline_outputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_fixture(tmp_path)
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(backtest_stage, "DATA_ROOT", tmp_path)
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

    strategy_returns = pd.read_csv(engine_dir / "strategy_returns_vol_compression_v1.csv")
    expected_cols = {"timestamp", "symbol", "pos", "ret", "pnl", "fp_active", "fp_age_bars", "fp_norm_due"}
    assert expected_cols.issubset(set(strategy_returns.columns))
    assert pd.api.types.is_integer_dtype(strategy_returns["pos"])
    assert pd.api.types.is_numeric_dtype(strategy_returns["ret"])
    assert pd.api.types.is_numeric_dtype(strategy_returns["pnl"])

    stage_metrics = json.loads((trades_dir / "metrics.json").read_text())
    assert "net_total_return" in stage_metrics
    trades = pd.read_csv(trades_dir / "trades_BTCUSDT.csv")
    assert stage_metrics["total_trades"] == len(trades)
    assert stage_metrics["win_rate"] == pytest.approx(float((trades["pnl"] > 0).mean()))
    assert stage_metrics["avg_r"] == pytest.approx(float(trades["r_multiple"].mean()))
    stage_manifest = json.loads((tmp_path / "runs" / run_id / "backtest_vol_compression_v1.json").read_text())
    assert stage_manifest["stats"]["symbols"]["BTCUSDT"]["entries"] == len(trades)

    report_args = [
        "make_report.py",
        "--run_id",
        run_id,
    ]
    monkeypatch.setattr(report_stage, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(report_stage, "load_configs", lambda paths: {"trade_day_timezone": "UTC"})
    monkeypatch.setattr(sys, "argv", report_args)
    assert report_stage.main() == 0

    summary_md = (tmp_path / "reports" / "vol_compression_expansion_v1" / run_id / "summary.md").read_text()
    assert "- Win rate (combined):" in summary_md
    assert "- Net total return (combined):" in summary_md
    assert "- Ending equity (combined):" in summary_md

    summary_json = json.loads((tmp_path / "reports" / "vol_compression_expansion_v1" / run_id / "summary.json").read_text())
    assert "win_rate" in summary_json
    assert "net_total_return" in summary_json
    assert summary_json.get("objective", {}).get("target_metric") == "net_total_return"
    assert "ending_equity" in summary_json
