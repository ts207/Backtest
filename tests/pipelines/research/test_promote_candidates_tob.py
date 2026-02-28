import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "project"))

import pandas as pd
import numpy as np
import pytest
from pipelines.research.promote_candidates import _evaluate_row

def test_promotion_tob_coverage_gate():
    # Base row that passes all other gates
    row = {
        "candidate_id": "test_1",
        "event_type": "VOL_SHOCK",
        "n_events": 200,
        "q_value": 0.01,
        "gate_stability": True,
        "val_t_stat": 3.0,
        "oos1_t_stat": 2.5,
        "train_t_stat": 4.0,
        "std_return": 0.01,
        "expectancy": 0.001,
        "gate_after_cost_positive": True,
        "gate_after_cost_stressed_positive": True,
        "gate_bridge_after_cost_positive_validation": True,
        "gate_bridge_after_cost_stressed_positive_validation": True,
        "tob_coverage": 0.9 # High coverage
    }
    
    # 1. High coverage -> standard promotion
    res = _evaluate_row(
        row=row,
        hypothesis_index={},
        negative_control_summary={},
        max_q_value=0.1,
        min_events=100,
        min_stability_score=0.0,
        min_sign_consistency=0.0,
        min_cost_survival_ratio=0.0,
        max_negative_control_pass_rate=1.0,
        min_tob_coverage=0.8,
        require_hypothesis_audit=False,
        allow_missing_negative_controls=True
    )
    assert res["promotion_decision"] == "promoted"
    assert res["promotion_track"] == "standard"
    assert res["gate_promo_tob_coverage"] is True
    
    # 2. Low coverage -> fallback_only promotion
    row_low = row.copy()
    row_low["tob_coverage"] = 0.5
    res = _evaluate_row(
        row=row_low,
        hypothesis_index={},
        negative_control_summary={},
        max_q_value=0.1,
        min_events=100,
        min_stability_score=0.0,
        min_sign_consistency=0.0,
        min_cost_survival_ratio=0.0,
        max_negative_control_pass_rate=1.0,
        min_tob_coverage=0.8,
        require_hypothesis_audit=False,
        allow_missing_negative_controls=True
    )
    assert res["promotion_decision"] == "promoted"
    assert res["promotion_track"] == "fallback_only"
    assert res["gate_promo_tob_coverage"] is False

if __name__ == "__main__":
    pytest.main([__file__])
