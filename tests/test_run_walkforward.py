import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.eval import run_walkforward


def test_run_walkforward_writes_summary_with_per_split_and_test_metrics(monkeypatch, tmp_path: Path) -> None:
    run_id = "wf_eval_run"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(run_walkforward, "DATA_ROOT", tmp_path)

    def _fake_split_backtest(cmd: list[str]) -> int:
        split_run_id = cmd[cmd.index("--run_id") + 1]
        metrics_dir = tmp_path / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / split_run_id
        metrics_dir.mkdir(parents=True, exist_ok=True)
        is_test = split_run_id.endswith("__wf_test")
        payload = {
            "total_trades": 120 if is_test else 200,
            "ending_equity": 1_020_000.0 if is_test else 1_010_000.0,
            "sharpe_annualized": 1.4 if is_test else 1.1,
            "max_drawdown": -0.08 if is_test else -0.1,
        }
        (metrics_dir / "metrics.json").write_text(json.dumps(payload), encoding="utf-8")
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
