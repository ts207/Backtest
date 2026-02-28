import pytest
import pandas as pd
import numpy as np
from eval.multiplicity import benjamini_hochberg, apply_multiplicity_control, report_discoveries

def test_benjamini_hochberg():
    p_values = [0.01, 0.04, 0.03, 0.005, 0.1, 0.002]
    # Sorted: 0.002, 0.005, 0.01, 0.03, 0.04, 0.1
    # Index:  1      2      3     4     5     6
    # q-vals = p * n / i
    # q(1) = 0.002 * 6 / 1 = 0.012
    # q(2) = 0.005 * 6 / 2 = 0.015
    # q(3) = 0.01 * 6 / 3 = 0.02
    # q(4) = 0.03 * 6 / 4 = 0.045
    # q(5) = 0.04 * 6 / 5 = 0.048
    # q(6) = 0.1 * 6 / 6 = 0.1
    reject, q_vals = benjamini_hochberg(p_values, alpha=0.05)
    
    # 0.012, 0.015, 0.02, 0.045, 0.048 are all <= 0.05
    # so first 5 sorted are rejected (all but 0.1)
    
    assert sum(reject) == 5
    assert reject[4] == False # The 0.1 is index 4 in original array
    
    # Check monotonicity properties and exact values
    # q_vals is mapped back to original indices
    # Index 4 is 0.1
    assert np.isclose(q_vals[4], 0.1)
    # Index 0 is 0.01 -> q(3) = 0.02
    assert np.isclose(q_vals[0], 0.02)
    # Index 5 is 0.002 -> q(1) = 0.012
    assert np.isclose(q_vals[5], 0.012)


def test_apply_multiplicity_control():
    df = pd.DataFrame({
        "strategy": ["A", "B", "C"],
        "p_value": [0.01, 0.1, 0.04]
    })
    
    out = apply_multiplicity_control(df, alpha=0.05)
    assert "pass_fdr" in out.columns
    assert "q_value_bh" in out.columns
    
    # q-vals: A (0.01) -> q(1)=0.03, B (0.1) -> q(3)=0.1, C (0.04) -> q(2)=0.06
    # So pass_fdr should be True, False, False for alpha 0.05. Wait...
    # p=[0.01, 0.04, 0.1]
    # q(1) = 0.01 * 3/1 = 0.03 -> min(0.03, 0.06) = 0.03
    # q(2) = 0.04 * 3/2 = 0.06 -> min(0.06, 0.1) = 0.06
    # q(3) = 0.1  * 3/3 = 0.1
    # Thus only A passes (0.03 <= 0.05).
    
    assert list(out["pass_fdr"]) == [True, False, False]
    assert np.allclose(out["q_value_bh"], [0.03, 0.1, 0.06])

def test_apply_multiplicity_missing_p_value():
    df = pd.DataFrame({"strategy": ["A", "B"]})
    with pytest.raises(ValueError, match="DataFrame must contain 'p_value' column"):
        apply_multiplicity_control(df)


def test_report_discoveries():
    df = pd.DataFrame({
        "strategy": ["A", "B", "C"],
        "pass_fdr": [True, False, True]
    })
    report = report_discoveries(df, alpha=0.05)
    assert report["total_hypotheses"] == 3
    assert report["fdr_target"] == 0.05
    assert report["discoveries"] == 2
    assert report["discovery_rate"] == 2.0 / 3.0
    
    report_empty = report_discoveries(pd.DataFrame({"strategy": []}))
    assert report_empty["total_hypotheses"] == 0
    assert report_empty["discoveries"] == 0
    assert report_empty["discovery_rate"] == 0.0
