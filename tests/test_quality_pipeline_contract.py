import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import build_strategy_candidates, summarize_discovery_quality

COST_SWEEP_COLUMNS = [
    "exp_costed_x0_5",
    "exp_costed_x1_0",
    "exp_costed_x1_5",
    "exp_costed_x2_0",
]


def test_quality_pipeline_contract_from_phase2_to_builder(monkeypatch, tmp_path: Path) -> None:
    run_id = "quality_contract"
    event = "vol_shock_relaxation"
    phase2_dir = tmp_path / "reports" / "phase2" / run_id / event
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    bridge_dir = tmp_path / "reports" / "bridge_eval" / run_id / event
    phase2_dir.mkdir(parents=True, exist_ok=True)
    edge_dir.mkdir(parents=True, exist_ok=True)
    bridge_dir.mkdir(parents=True, exist_ok=True)

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

    # Bridge candidate metrics CSV — includes cost sweep columns required by AGENTS.md bridge policy.
    bridge_csv = bridge_dir / "bridge_candidate_metrics.csv"
    pd.DataFrame(
        [
            {
                "candidate_id": "strict_pass",
                "bridge_eval_status": "evaluated",
                "gate_bridge_tradable": 1,
                "bridge_edge_to_cost_ratio": 3.5,
                "exp_costed_x0_5": 0.02,
                "exp_costed_x1_0": 0.01,
                "exp_costed_x1_5": 0.005,
                "exp_costed_x2_0": -0.002,
                "bridge_fail_reasons": "",
            },
            {
                "candidate_id": "strict_fail",
                "bridge_eval_status": "evaluated",
                "gate_bridge_tradable": 0,
                "bridge_edge_to_cost_ratio": 1.1,
                "exp_costed_x0_5": -0.01,
                "exp_costed_x1_0": -0.02,
                "exp_costed_x1_5": -0.03,
                "exp_costed_x2_0": -0.04,
                "bridge_fail_reasons": "gate_bridge_edge_cost_ratio",
            },
        ]
    ).to_csv(bridge_csv, index=False)

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
            {
                "run_id": run_id,
                "event": event,
                "candidate_id": "overlay_candidate",
                "candidate_type": "overlay",
                "status": "PROMOTED",
                "edge_score": 0.8,
                "expected_return_proxy": 0.02,
                "expectancy_per_trade": 0.02,
                "expectancy_after_multiplicity": 0.02,
                "stability_proxy": 0.85,
                "robustness_score": 0.85,
                "event_frequency": 0.35,
                "capacity_proxy": 1.0,
                "profit_density_score": 0.006,
                "quality_score": 0.80,
                "selection_score_executed": 20.0,
                "gate_bridge_tradable": 1,
                "n_events": 100,
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

    # --- Funnel summary contract assertions (AGENTS.md "Required Funnel Artifact") ---
    funnel_path = tmp_path / "reports" / run_id / "funnel_summary.json"
    assert funnel_path.exists(), "funnel_summary.json must be written by summarize_discovery_quality"
    funnel = json.loads(funnel_path.read_text(encoding="utf-8"))
    families = funnel.get("families", {})
    assert families, "funnel_summary must contain at least one event family"

    for family_name, family_data in families.items():
        # (4) top_failure_reasons must be present per family
        assert "top_failure_reasons" in family_data, (
            f"family '{family_name}' missing top_failure_reasons in funnel_summary"
        )
        assert isinstance(family_data["top_failure_reasons"], list), (
            f"family '{family_name}' top_failure_reasons must be a list"
        )

        # (5) bridge_pass_val must be present and numeric (integer) per family
        assert "bridge_pass_val" in family_data, (
            f"family '{family_name}' missing bridge_pass_val in funnel_summary"
        )
        assert isinstance(family_data["bridge_pass_val"], int), (
            f"family '{family_name}' bridge_pass_val must be an integer"
        )

        # (6) compiled_bases and compiled_overlays must be present per family
        assert "compiled_bases" in family_data, (
            f"family '{family_name}' missing compiled_bases in funnel_summary"
        )
        assert "compiled_overlays" in family_data, (
            f"family '{family_name}' missing compiled_overlays in funnel_summary"
        )

    # (5) bridge_pass_val reflects gate_bridge_tradable from bridge data (1 tradable row in fixture)
    assert families[event]["bridge_pass_val"] == 1, (
        "bridge_pass_val must equal the count of gate_bridge_tradable=1 rows in bridge_candidate_metrics.csv"
    )

    # (1) Cost sweep columns must be present in bridge_candidate_metrics.csv produced by bridge stage
    bridge_df = pd.read_csv(bridge_csv)
    for col in COST_SWEEP_COLUMNS:
        assert col in bridge_df.columns, (
            f"bridge_candidate_metrics.csv must contain cost sweep column '{col}' (AGENTS.md bridge policy)"
        )

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

    # (2) edge_to_cost >= 2.0 enforcement: gate_bridge_tradable must be a boolean field on all candidates
    assert all(isinstance(row.get("gate_bridge_tradable"), bool) for row in selected_edge_rows), (
        "gate_bridge_tradable must be a boolean field on all strategy candidates (bridge gate enforcement)"
    )

    # (3) Overlay-vs-base separation: candidates with candidate_type='overlay' must NOT appear in builder output
    assert not any(row.get("candidate_type") == "overlay" for row in payload), (
        "overlay candidates must not appear in strategy_candidates.json (overlay-only policy)"
    )
