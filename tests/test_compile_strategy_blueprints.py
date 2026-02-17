from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import compile_strategy_blueprints


def _write_inputs(tmp_path: Path, run_id: str) -> None:
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    edge_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "all__delay_8",
                "status": "PROMOTED",
                "candidate_symbol": "BTCUSDT",
                "edge_score": 0.4,
                "expectancy_per_trade": 0.02,
                "robustness_score": 0.9,
                "event_frequency": 0.3,
                "capacity_proxy": 1.0,
                "profit_density_score": 0.0054,
                "n_events": 120,
                "source_path": str(tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation" / "phase2_candidates.csv"),
            },
            {
                "run_id": run_id,
                "event": "liquidity_absence_window",
                "candidate_id": "all__no_action",
                "status": "DRAFT",
                "candidate_symbol": "ALL",
                "edge_score": 0.1,
                "expectancy_per_trade": 0.005,
                "robustness_score": 0.6,
                "event_frequency": 0.2,
                "capacity_proxy": 0.2,
                "profit_density_score": 0.0006,
                "n_events": 60,
                "source_path": str(tmp_path / "reports" / "phase2" / run_id / "liquidity_absence_window" / "phase2_candidates.csv"),
            },
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)

    phase2_vsr = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    phase2_vsr.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "all__delay_8",
                "condition": "all",
                "action": "delay_8",
                "sample_size": 120,
                "gate_oos_validation_test": True,
                "gate_multiplicity": True,
                "gate_c_regime_stable": True,
                "gate_all": True,
                "delta_adverse_mean": -0.02,
                "delta_opportunity_mean": 0.03,
                "robustness_score": 0.9,
                "profit_density_score": 0.0054,
            }
        ]
    ).to_csv(phase2_vsr / "phase2_candidates.csv", index=False)

    phase2_law = tmp_path / "reports" / "phase2" / run_id / "liquidity_absence_window"
    phase2_law.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "all__no_action",
                "condition": "all",
                "action": "no_action",
                "sample_size": 60,
                "gate_oos_validation_test": False,
                "gate_multiplicity": False,
                "gate_c_regime_stable": True,
                "gate_all": False,
                "delta_adverse_mean": -0.004,
                "delta_opportunity_mean": 0.005,
                "robustness_score": 0.6,
                "profit_density_score": 0.0006,
            }
        ]
    ).to_csv(phase2_law / "phase2_candidates.csv", index=False)

    report_vsr = tmp_path / "reports" / "vol_shock_relaxation" / run_id
    report_vsr.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "rv_decay_half_life": [12, 18, 24],
            "range_pct_96": [0.01, 0.02, 0.03],
            "forward_abs_return_h": [0.02, 0.03, 0.05],
        }
    ).to_csv(report_vsr / "vol_shock_relaxation_events.csv", index=False)


def test_compiler_emits_per_event_blueprints_with_required_fields(monkeypatch, tmp_path: Path) -> None:
    run_id = "dsl_compile_fields"
    _write_inputs(tmp_path=tmp_path, run_id=run_id)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compile_strategy_blueprints.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--max_per_event",
            "2",
        ],
    )
    assert compile_strategy_blueprints.main() == 0

    out_path = tmp_path / "reports" / "strategy_blueprints" / run_id / "blueprints.jsonl"
    assert out_path.exists()
    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert rows
    events = {row["event_type"] for row in rows}
    assert "vol_shock_relaxation" in events
    assert "liquidity_absence_window" in events
    for row in rows:
        assert row["id"]
        assert row["entry"]["triggers"]
        assert "time_stop_bars" in row["exit"]
        assert "trailing_stop_type" in row["exit"]
        assert "break_even_r" in row["exit"]
        assert "condition_logic" in row["entry"]
        assert "condition_nodes" in row["entry"]
        assert "min_trades" in row["evaluation"]


def test_compiler_is_deterministic_under_rerun(monkeypatch, tmp_path: Path) -> None:
    run_id = "dsl_compile_deterministic"
    _write_inputs(tmp_path=tmp_path, run_id=run_id)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)

    argv = [
        "compile_strategy_blueprints.py",
        "--run_id",
        run_id,
        "--symbols",
        "BTCUSDT,ETHUSDT",
    ]

    monkeypatch.setattr(sys, "argv", argv)
    assert compile_strategy_blueprints.main() == 0
    out_path = tmp_path / "reports" / "strategy_blueprints" / run_id / "blueprints.jsonl"
    first = out_path.read_bytes()

    monkeypatch.setattr(sys, "argv", argv)
    assert compile_strategy_blueprints.main() == 0
    second = out_path.read_bytes()

    assert first == second
