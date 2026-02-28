import pytest
import pandas as pd
from eval.state_acceptance import run_state_acceptance

def test_state_acceptance_missing_columns():
    df = pd.DataFrame({"other_col": [1, 2, 3]})
    res = run_state_acceptance(df, "state_col", "target")
    assert not res["ok"]
    assert "missing" in res["error"].lower()

def test_state_acceptance_success():
    df = pd.DataFrame({
        "state_col": [0, 0, 1, 1, 2, 2],
        "target": [0.1, 0.2, 0.4, 0.5, 0.8, 0.9]
    })
    
    # Means: 0: 0.15, 1: 0.45, 2: 0.85 (Monotonic increasing)
    res = run_state_acceptance(df, "state_col", "target")
    assert res["ok"]
    assert res["is_monotonic"]
    assert res["join_rate"] == 1.0
    assert res["passed"]

def test_state_acceptance_fails_monotonicity():
    df = pd.DataFrame({
        "state_col": [0, 0, 1, 1, 2, 2],
        "target": [0.8, 0.9, 0.4, 0.5, 0.1, 0.2]
    })
    
    # Means: 0: 0.85, 1: 0.45, 2: 0.15 (Monotonic decreasing, not increasing)
    res = run_state_acceptance(df, "state_col", "target")
    assert res["ok"]
    assert not res["is_monotonic"]
    assert res["join_rate"] == 1.0
    assert not res["passed"]

def test_state_acceptance_fails_join_rate():
    # 5 out of 10 rows are NaN for state
    df = pd.DataFrame({
        "state_col": [0, 1, 2, None, None, None, None, None, 0, 1],
        "target": [0.1, 0.4, 0.8, 0.1, 0.1, 0.1, 0.1, 0.1, 0.2, 0.5]
    })
    
    res = run_state_acceptance(df, "state_col", "target")
    assert res["ok"]
    assert res["is_monotonic"]
    assert res["join_rate"] == 0.5
    assert not res["passed"]
