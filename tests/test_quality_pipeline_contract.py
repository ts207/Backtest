import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import build_strategy_candidates, summarize_discovery_quality


def test_quality_pipeline_contract_from_phase2_to_builder(monkeypatch, tmp_path: Path) -> None:
    run_id = "quality_contract"
    event = "vol_shock_relaxation"
    phase2_dir = tmp_path / "reports" / "phase2" / run_id / event
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    phase2_dir.mkdir(parents=True, exist_ok=True)
    edge_dir.mkdir(parents=True, exist_ok=True)

    phase2_csv = phase2_dir / "phase2_candidates.csv"
    pd.DataFrame(
        [
            {
                "candidate_id": "strict_pass",
                "condition": "all",
                "action": "delay_8",
                "gate_pass": 1,
                "gate_oos_consistency_strict": 1,
                "gate_bridge_tradable": 1,
                "selection_score_executed": 25.0,
                "quality_score": 0.55,
                "fail_reasons": "",
            },
            {
                "candidate_id": "strict_fail",
                "condition": "all",
                "action": "delay_16",
                "gate_pass": 1,
                "gate_oos_consistency_strict": 0,
                "gate_bridge_tradable": 0,
                "selection_score_executed": -5.0,
                "quality_score": 0.99,
                "fail_reasons": "gate_oos_consistency_strict,gate_bridge_tradable",
            },
        ]
    ).to_csv(phase2_csv, index=False)

    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "event": event,
                "candidate_id": "strict_pass",
                "status": "PROMOTED",
                "edge_score": 0.2,
                "expected_return_proxy": 0.01,
                "expectancy_per_trade": 0.01,
                "expectancy_after_multiplicity": 0.01,
                "stability_proxy": 0.8,
                "robustness_score": 0.8,
                "event_frequency": 0.3,
                "capacity_proxy": 1.0,
                "profit_density_score": 0.0024,
                "quality_score": 0.55,
                "selection_score_executed": 25.0,
                "gate_bridge_tradable": 1,
                "n_events": 120,
                "source_path": str(phase2_csv),
            },
            {
                "run_id": run_id,
                "event": event,
                "candidate_id": "strict_fail",
                "status": "PROMOTED",
                "edge_score": 0.9,
                "expected_return_proxy": 0.03,
                "expectancy_per_trade": 0.03,
                "expectancy_after_multiplicity": 0.03,
                "stability_proxy": 0.9,
                "robustness_score": 0.9,
                "event_frequency": 0.4,
                "capacity_proxy": 1.0,
                "profit_density_score": 0.0108,
                "quality_score": 0.99,
                "selection_score_executed": -5.0,
                "gate_bridge_tradable": 0,
                "n_events": 120,
                "source_path": str(phase2_csv),
            },
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(summarize_discovery_quality, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "summarize_discovery_quality.py",
            "--run_id",
            run_id,
        ],
    )
    assert summarize_discovery_quality.main() == 0
    summary_path = tmp_path / "reports" / "phase2" / run_id / "discovery_quality_summary.json"
    assert summary_path.exists()

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--top_k_per_event",
            "5",
            "--max_candidates_per_event",
            "5",
            "--max_candidates",
            "10",
            "--include_alpha_bundle",
            "0",
            "--ignore_checklist",
            "1",
        ],
    )
    assert build_strategy_candidates.main() == 0
    out_json = tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json"
    assert out_json.exists()

    payload = json.loads(out_json.read_text(encoding="utf-8"))
    selected_edge_rows = [row for row in payload if row.get("source_type") == "edge_candidate"]
    assert selected_edge_rows
    assert all(bool(row.get("gate_oos_consistency_strict")) for row in selected_edge_rows)
    assert all(bool(row.get("gate_bridge_tradable")) for row in selected_edge_rows)
    assert {row.get("candidate_id") for row in selected_edge_rows} == {"strict_pass"}
