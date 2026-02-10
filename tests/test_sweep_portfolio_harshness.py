import json
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import sweep_portfolio_harshness as sweep


def test_sweep_portfolio_harshness_outputs_frontier(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = "run_a"
    baseline_dir = tmp_path / "reports" / "vol_compression_expansion_v1" / run_id
    metrics_dir = tmp_path / "lake" / "trades" / "backtests" / "multi_edge_portfolio" / run_id
    baseline_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    portfolio_path = metrics_dir / "portfolio_returns_equal_risk.csv"
    symbol_contrib_path = metrics_dir / "symbol_contribution_equal_risk.csv"

    portfolio_df = pd.DataFrame(
        {
            "timestamp": ["2021-01-01T00:00:00Z", "2022-01-01T00:00:00Z"],
            "portfolio_pnl": [1.0, 2.0],
        }
    )
    portfolio_df.to_csv(portfolio_path, index=False)

    symbol_df = pd.DataFrame({"symbol": ["BTCUSDT", "ETHUSDT"], "total_pnl": [1.0, -0.5]})
    symbol_df.to_csv(symbol_contrib_path, index=False)

    (baseline_dir / "summary.json").write_text(json.dumps({"net_total_return": 0.1}), encoding="utf-8")
    metrics_payload = {
        "selected_mode": "equal_risk",
        "modes": {
            "equal_risk": {
                "net_total_return": 0.2,
                "max_drawdown": 0.05,
                "constraints_pass": True,
                "estimated_cost_drag": 0.01,
                "paths": {
                    "portfolio": str(portfolio_path),
                    "symbol_contribution": str(symbol_contrib_path),
                },
            }
        },
    }
    (metrics_dir / "metrics.json").write_text(json.dumps(metrics_payload), encoding="utf-8")

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(sweep, "DATA_ROOT", tmp_path)

    out_dir = tmp_path / "reports" / "multi_edge_validation" / "harshness_sweep"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "sweep_portfolio_harshness.py",
            "--run_ids",
            run_id,
            "--regime_sign_consistency_grid",
            "0.7,0.8",
            "--symbol_positive_ratio_grid",
            "0.5,0.6",
            "--out_dir",
            str(out_dir),
        ],
    )

    assert sweep.main() == 0
    frontier = pd.read_csv(out_dir / "frontier.csv")
    assert not frontier.empty
    assert "survival_rate" in frontier.columns
