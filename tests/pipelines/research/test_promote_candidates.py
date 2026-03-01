from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research.promote_candidates import _evaluate_row


def _eval_row(**overrides):
    row = {
        "event_type": "VOL_SHOCK",
        "candidate_id": "cand_1",
        "plan_row_id": "p1",
        "q_value": 0.01,
        "n_events": 250,
        "effect_shrunk_state": 0.02,
        "std_return": 0.01,
        "gate_stability": True,
        "val_t_stat": 2.5,
        "oos1_t_stat": 2.0,
        "gate_after_cost_positive": True,
        "gate_after_cost_stressed_positive": True,
        "gate_bridge_after_cost_positive_validation": True,
        "gate_bridge_after_cost_stressed_positive_validation": False,
        "gate_delay_robustness": True,
        "validation_samples": 100,
    }
    row.update(overrides)
    return _evaluate_row(
        row=row,
        hypothesis_index={"p1": {"statuses": ["executed"], "executed": True}},
        negative_control_summary={"by_event": {"VOL_SHOCK": {"pass_rate_after_bh": 0.0}}},
        max_q_value=0.10,
        min_events=100,
        min_stability_score=0.05,
        min_sign_consistency=0.60,
        min_cost_survival_ratio=0.75,
        max_negative_control_pass_rate=0.01,
        min_tob_coverage=0.0,
        require_hypothesis_audit=True,
        allow_missing_negative_controls=False,
    )


def test_promote_candidate_happy_path():
    out = _eval_row()
    assert out["promotion_decision"] == "promoted"
    assert out["reject_reason"] == ""
    assert out["gate_promo_statistical"] is True
    assert out["gate_promo_stability"] is True
    assert out["gate_promo_cost_survival"] is True
    assert out["gate_promo_negative_control"] is True
    assert out["gate_promo_hypothesis_audit"] is True


def test_promote_candidate_rejects_cost_and_controls():
    out = _eval_row(
        gate_after_cost_positive=False,
        gate_after_cost_stressed_positive=False,
        gate_bridge_after_cost_positive_validation=False,
        gate_bridge_after_cost_stressed_positive_validation=False,
        control_pass_rate=0.25,
    )
    assert out["promotion_decision"] == "rejected"
    assert "cost_survival" in out["reject_reason"]
    assert "negative_control_fail" in out["reject_reason"]


def test_promote_candidate_rejects_missing_hypothesis_audit():
    out = _evaluate_row(
        row={
            "event_type": "VOL_SHOCK",
            "candidate_id": "cand_2",
            "plan_row_id": "missing",
            "q_value": 0.01,
            "n_events": 200,
            "effect_shrunk_state": 0.01,
            "std_return": 0.01,
            "gate_stability": True,
            "gate_after_cost_positive": True,
            "gate_after_cost_stressed_positive": True,
            "gate_bridge_after_cost_positive_validation": True,
            "gate_bridge_after_cost_stressed_positive_validation": True,
            "gate_delay_robustness": True,
        },
        hypothesis_index={},
        negative_control_summary={"pass_rate_after_bh": 0.0},
        max_q_value=0.10,
        min_events=100,
        min_stability_score=0.05,
        min_sign_consistency=0.0,
        min_cost_survival_ratio=0.75,
        max_negative_control_pass_rate=0.01,
        min_tob_coverage=0.0,
        require_hypothesis_audit=True,
        allow_missing_negative_controls=True,
    )
    assert out["promotion_decision"] == "rejected"
    assert "hypothesis_missing_audit" in out["reject_reason"]
