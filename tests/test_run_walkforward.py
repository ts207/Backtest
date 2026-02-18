import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.eval import run_walkforward


def _write_strategy_returns(engine_dir: Path, strategy_id: str) -> None:
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=6, freq="15min", tz="UTC"),
            "symbol": ["BTCUSDT"] * 6,
            "pos": [0, 1, 1, 0, -1, 0],
            "position_scale": [1.0] * 6,
            "pnl": [0.0, 0.001, -0.0002, 0.0, 0.0005, 0.0],
            "gross_pnl": [0.0, 0.0012, -0.0001, 0.0, 0.0007, 0.0],
            "trading_cost": [0.0, 0.0001, 0.0001, 0.0, 0.0001, 0.0],
            "funding_pnl": [0.0] * 6,
            "borrow_cost": [0.0] * 6,
        }
    )
    frame.to_csv(engine_dir / f"strategy_returns_{strategy_id}.csv", index=False)


def _write_metrics(metrics_dir: Path, *, trades: int) -> None:
    payload = {
        "total_trades": trades,
        "ending_equity": 1_010_000.0,
        "sharpe_annualized": 1.1,
        "max_drawdown": -0.1,
        "cost_decomposition": {"net_alpha": 0.25},
    }
    (metrics_dir / "metrics.json").write_text(json.dumps(payload), encoding="utf-8")


def test_run_walkforward_writes_summary_with_per_split_and_test_metrics(monkeypatch, tmp_path: Path) -> None:
    run_id = "wf_eval_run"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(run_walkforward, "DATA_ROOT", tmp_path)

    def _fake_split_backtest(cmd: list[str]) -> int:
        assert "--config" in cmd
        assert cmd.count("--config") == 2
        split_run_id = cmd[cmd.index("--run_id") + 1]
        metrics_dir = tmp_path / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / split_run_id
        metrics_dir.mkdir(parents=True, exist_ok=True)
        engine_dir = tmp_path / "runs" / split_run_id / "engine"
        engine_dir.mkdir(parents=True, exist_ok=True)
        is_test = split_run_id.endswith("__wf_test")
        _write_metrics(metrics_dir, trades=120 if is_test else 200)
        _write_strategy_returns(engine_dir, "vol_compression_v1")
        return 0

    monkeypatch.setattr(run_walkforward, "_run_split_backtest", _fake_split_backtest)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_walkforward.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-03-31",
            "--strategies",
            "vol_compression_v1",
            "--embargo_days",
            "1",
            "--force",
            "1",
            "--config",
            "configs/pipeline.yaml",
            "--config",
            "configs/fees.yaml",
        ],
    )
    assert run_walkforward.main() == 0

    summary_path = tmp_path / "reports" / "eval" / run_id / "walkforward_summary.json"
    assert summary_path.exists()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["tested_splits"] >= 2
    assert "per_split_metrics" in summary
    assert "test" in summary["per_split_metrics"]
    assert summary["final_test_metrics"].get("total_trades", 0) > 0
    assert "stressed_net_pnl" in summary["per_split_metrics"]["test"]
    assert "gate_precheck" in summary["per_split_metrics"]["test"]
    assert "per_strategy_split_metrics" in summary
    assert "vol_compression_v1" in summary["per_strategy_split_metrics"]
    assert "test" in summary["per_strategy_split_metrics"]["vol_compression_v1"]
    assert "net_pnl" in summary["per_strategy_split_metrics"]["vol_compression_v1"]["test"]
    assert summary["integrity_checks"]["artifacts_validated"] is True
    assert summary["integrity_checks"]["required_test_present"] is True
    assert summary["integrity_checks"]["config_passthrough_count"] == 2


def test_run_walkforward_requires_exactly_one_execution_mode(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(run_walkforward, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_walkforward.py",
            "--run_id",
            "wf_invalid",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
        ],
    )
    assert run_walkforward.main() == 1


def test_run_walkforward_fails_when_metrics_missing(monkeypatch, tmp_path: Path) -> None:
    run_id = "wf_missing_metrics"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(run_walkforward, "DATA_ROOT", tmp_path)

    def _fake_split_backtest(cmd: list[str]) -> int:
        split_run_id = cmd[cmd.index("--run_id") + 1]
        engine_dir = tmp_path / "runs" / split_run_id / "engine"
        engine_dir.mkdir(parents=True, exist_ok=True)
        _write_strategy_returns(engine_dir, "vol_compression_v1")
        return 0

    monkeypatch.setattr(run_walkforward, "_run_split_backtest", _fake_split_backtest)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_walkforward.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-03-31",
            "--strategies",
            "vol_compression_v1",
        ],
    )
    assert run_walkforward.main() == 1


def test_run_walkforward_fails_when_metrics_key_missing(monkeypatch, tmp_path: Path) -> None:
    run_id = "wf_metrics_missing_key"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(run_walkforward, "DATA_ROOT", tmp_path)

    def _fake_split_backtest(cmd: list[str]) -> int:
        split_run_id = cmd[cmd.index("--run_id") + 1]
        metrics_dir = tmp_path / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / split_run_id
        metrics_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "total_trades": 10,
            "ending_equity": 1_005_000.0,
            "sharpe_annualized": 0.8,
            # missing max_drawdown
            "cost_decomposition": {"net_alpha": 0.1},
        }
        (metrics_dir / "metrics.json").write_text(json.dumps(payload), encoding="utf-8")
        engine_dir = tmp_path / "runs" / split_run_id / "engine"
        engine_dir.mkdir(parents=True, exist_ok=True)
        _write_strategy_returns(engine_dir, "vol_compression_v1")
        return 0

    monkeypatch.setattr(run_walkforward, "_run_split_backtest", _fake_split_backtest)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_walkforward.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-03-31",
            "--strategies",
            "vol_compression_v1",
        ],
    )
    assert run_walkforward.main() == 1


def test_run_walkforward_fails_when_no_strategy_returns(monkeypatch, tmp_path: Path) -> None:
    run_id = "wf_missing_strategy_returns"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(run_walkforward, "DATA_ROOT", tmp_path)

    def _fake_split_backtest(cmd: list[str]) -> int:
        split_run_id = cmd[cmd.index("--run_id") + 1]
        metrics_dir = tmp_path / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / split_run_id
        metrics_dir.mkdir(parents=True, exist_ok=True)
        _write_metrics(metrics_dir, trades=30)
        (tmp_path / "runs" / split_run_id / "engine").mkdir(parents=True, exist_ok=True)
        return 0

    monkeypatch.setattr(run_walkforward, "_run_split_backtest", _fake_split_backtest)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_walkforward.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-03-31",
            "--strategies",
            "vol_compression_v1",
        ],
    )
    assert run_walkforward.main() == 1


def test_run_walkforward_fails_when_declared_strategy_file_missing(monkeypatch, tmp_path: Path) -> None:
    run_id = "wf_missing_declared_strategy"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(run_walkforward, "DATA_ROOT", tmp_path)

    def _fake_split_backtest(cmd: list[str]) -> int:
        split_run_id = cmd[cmd.index("--run_id") + 1]
        metrics_dir = tmp_path / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / split_run_id
        metrics_dir.mkdir(parents=True, exist_ok=True)
        _write_metrics(metrics_dir, trades=30)
        engine_dir = tmp_path / "runs" / split_run_id / "engine"
        engine_dir.mkdir(parents=True, exist_ok=True)
        _write_strategy_returns(engine_dir, "vol_compression_v1")
        return 0

    monkeypatch.setattr(run_walkforward, "_run_split_backtest", _fake_split_backtest)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_walkforward.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-03-31",
            "--strategies",
            "vol_compression_v1,carry_funding_v1",
        ],
    )
    assert run_walkforward.main() == 1


def test_run_walkforward_requires_test_split(monkeypatch, tmp_path: Path) -> None:
    run_id = "wf_no_test_split"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(run_walkforward, "DATA_ROOT", tmp_path)

    class _NoTestSplit:
        def __init__(self, label: str) -> None:
            self.label = label
            self.start = pd.Timestamp("2024-01-01", tz="UTC")
            self.end = pd.Timestamp("2024-01-10", tz="UTC")

        def to_dict(self) -> dict:
            return {"label": self.label, "start": self.start.isoformat(), "end": self.end.isoformat()}

    monkeypatch.setattr(
        run_walkforward,
        "build_time_splits",
        lambda **_: [_NoTestSplit("train"), _NoTestSplit("validation")],
    )
    called = {"count": 0}

    def _fake_split_backtest(_cmd: list[str]) -> int:
        called["count"] += 1
        return 0

    monkeypatch.setattr(run_walkforward, "_run_split_backtest", _fake_split_backtest)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_walkforward.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-03-31",
            "--strategies",
            "vol_compression_v1",
        ],
    )
    assert run_walkforward.main() == 1
    assert called["count"] == 0
