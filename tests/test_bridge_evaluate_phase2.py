import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import bridge_evaluate_phase2


def _write_phase2_candidates(tmp_path: Path, run_id: str, rows: list[dict]) -> Path:
    out_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "phase2_candidates.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return out_dir


def test_bridge_stage_updates_phase2_and_overlay_delta(monkeypatch, tmp_path: Path) -> None:
    run_id = "bridge_eval_pass"
    phase2_dir = _write_phase2_candidates(
        tmp_path,
        run_id=run_id,
        rows=[
            {
                "candidate_id": "c_base",
                "condition": "all",
                "action": "no_action",
                "after_cost_expectancy_per_trade": 0.0010,
                "stressed_after_cost_expectancy_per_trade": 0.0008,
                "validation_samples": 40,
                "sample_size": 100,
                "avg_dynamic_cost_bps": 2.0,
                "turnover_proxy_mean": 1.0,
                "cost_ratio": 0.20,
                "gate_all": True,
                "fail_reasons": "",
            },
            {
                "candidate_id": "c_overlay",
                "condition": "all",
                "action": "risk_throttle_0.5",
                "after_cost_expectancy_per_trade": 0.0015,
                "stressed_after_cost_expectancy_per_trade": 0.0012,
                "validation_samples": 40,
                "sample_size": 100,
                "avg_dynamic_cost_bps": 2.0,
                "turnover_proxy_mean": 0.5,
                "cost_ratio": 0.15,
                "gate_all": True,
                "fail_reasons": "",
            },
        ],
    )
    promoted_path = phase2_dir / "promoted_candidates.json"
    promoted_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "event_type": "vol_shock_relaxation",
                "decision": "promote",
                "promoted_count": 2,
                "candidates": [{"candidate_id": "c_base"}, {"candidate_id": "c_overlay"}],
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(bridge_evaluate_phase2, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "bridge_evaluate_phase2.py",
            "--run_id",
            run_id,
            "--event_type",
            "vol_shock_relaxation",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start",
            "2021-01-01",
            "--end",
            "2021-12-31",
            "--edge_cost_k",
            "2.0",
            "--min_validation_trades",
            "20",
        ],
    )
    assert bridge_evaluate_phase2.main() == 0

    updated = pd.read_csv(phase2_dir / "phase2_candidates.csv")
    assert "gate_bridge_tradable" in updated.columns
    assert "selection_score_executed" in updated.columns
    assert "exp_costed_x0_5" in updated.columns
    assert "exp_costed_x1_0" in updated.columns
    assert "exp_costed_x1_5" in updated.columns
    assert "exp_costed_x2_0" in updated.columns
    assert "bridge_edge_to_cost_ratio" in updated.columns
    assert updated["gate_bridge_tradable"].astype(bool).all()
    assert updated["gate_all"].astype(bool).all()
    assert (updated["exp_costed_x0_5"] >= updated["exp_costed_x1_0"]).all()
    assert (updated["exp_costed_x1_0"] >= updated["exp_costed_x1_5"]).all()
    assert (updated["exp_costed_x1_5"] >= updated["exp_costed_x2_0"]).all()

    overlay_rows = pd.read_csv(tmp_path / "reports" / "bridge_eval" / run_id / "vol_shock_relaxation" / "bridge_overlay_delta_metrics.csv")
    assert not overlay_rows.empty
    assert overlay_rows.iloc[0]["candidate_id"] == "c_overlay"

    promoted = json.loads(promoted_path.read_text(encoding="utf-8"))
    assert promoted["promoted_count"] == 2


def test_bridge_stage_blocks_edge_to_cost_ratio(monkeypatch, tmp_path: Path) -> None:
    run_id = "bridge_eval_cost_ratio_fail"
    phase2_dir = _write_phase2_candidates(
        tmp_path,
        run_id=run_id,
        rows=[
            {
                "candidate_id": "c1",
                "condition": "all",
                "action": "delay_8",
                "after_cost_expectancy_per_trade": 0.0001,
                "stressed_after_cost_expectancy_per_trade": 0.00005,
                "validation_samples": 50,
                "sample_size": 100,
                "avg_dynamic_cost_bps": 10.0,
                "turnover_proxy_mean": 1.0,
                "cost_ratio": 0.75,
                "gate_all": True,
                "fail_reasons": "",
            },
        ],
    )
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(bridge_evaluate_phase2, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "bridge_evaluate_phase2.py",
            "--run_id",
            run_id,
            "--event_type",
            "vol_shock_relaxation",
            "--symbols",
            "BTCUSDT",
            "--edge_cost_k",
            "2.0",
            "--min_validation_trades",
            "20",
        ],
    )
    assert bridge_evaluate_phase2.main() == 0
    updated = pd.read_csv(phase2_dir / "phase2_candidates.csv")
    assert bool(updated.loc[0, "gate_bridge_edge_cost_ratio"]) is False
    assert bool(updated.loc[0, "gate_bridge_tradable"]) is False
    assert "gate_bridge_edge_cost_ratio" in str(updated.loc[0, "fail_reasons"])


def test_bridge_stage_blocks_when_validation_trades_too_low(monkeypatch, tmp_path: Path) -> None:
    run_id = "bridge_eval_trade_count_fail"
    phase2_dir = _write_phase2_candidates(
        tmp_path,
        run_id=run_id,
        rows=[
            {
                "candidate_id": "c1",
                "condition": "all",
                "action": "delay_8",
                "after_cost_expectancy_per_trade": 0.0015,
                "stressed_after_cost_expectancy_per_trade": 0.0012,
                "validation_samples": 5,
                "sample_size": 20,
                "avg_dynamic_cost_bps": 1.0,
                "turnover_proxy_mean": 0.5,
                "cost_ratio": 0.10,
                "gate_all": True,
                "fail_reasons": "",
            },
        ],
    )
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(bridge_evaluate_phase2, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "bridge_evaluate_phase2.py",
            "--run_id",
            run_id,
            "--event_type",
            "vol_shock_relaxation",
            "--symbols",
            "BTCUSDT",
            "--edge_cost_k",
            "2.0",
            "--min_validation_trades",
            "20",
        ],
    )
    assert bridge_evaluate_phase2.main() == 0
    updated = pd.read_csv(phase2_dir / "phase2_candidates.csv")
    assert bool(updated.loc[0, "gate_bridge_has_trades_validation"]) is False
    assert bool(updated.loc[0, "gate_bridge_tradable"]) is False
    assert "gate_bridge_has_trades_validation" in str(updated.loc[0, "fail_reasons"])
