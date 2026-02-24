from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.research.generate_recommendations_checklist as checklist


class _Args:
    min_edge_candidates = 1
    min_promoted_candidates = 1
    min_bridge_tradable_candidates = 1
    min_bridge_tradable_promoted_candidates = 1
    min_expectancy_evidence = 1
    min_robust_survivors = 1
    require_expectancy_exists = 1
    require_stability_pass = 1
    require_capacity_pass = 1


def test_checklist_uses_survivor_definition_note():
    payload = checklist._build_payload(
        run_id="r1",
        args=_Args(),
        edge_metrics={"rows": 1, "promoted": 1, "bridge_tradable": 1, "bridge_tradable_promoted": 1},
        expectancy_payload={"expectancy_exists": True, "expectancy_evidence": [{"x": 1}]},
        robustness_payload={
            "survivor_definition": "promotion_grade_v1",
            "survivors": [{"condition": "compression", "horizon": 4}],
            "stability_diagnostics": {"pass": True},
            "capacity_diagnostics": {"pass": True},
        },
        paths={},
    )

    gate = next(g for g in payload["gates"] if g["name"] == "robust_survivor_count")
    assert gate["passed"] is True
    assert gate["note"] == "definition=promotion_grade_v1"
    assert payload["metrics"]["robust_survivor_count"] == 1


def test_checklist_discovery_profile_relaxes_ops_gates():
    args = SimpleNamespace(
        gate_profile="discovery",
        min_edge_candidates=1,
        min_promoted_candidates=1,
        min_bridge_tradable_candidates=1,
        min_bridge_tradable_promoted_candidates=1,
        min_expectancy_evidence=1,
        min_robust_survivors=1,
        require_expectancy_exists=1,
        require_stability_pass=1,
        require_capacity_pass=1,
    )
    out = checklist._apply_checklist_gate_profile(args)
    assert out.require_stability_pass == 0
    assert out.require_capacity_pass == 0
