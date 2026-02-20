import pytest
import pandas as pd
from project.pipelines.research.compile_strategy_blueprints import _passes_fallback_gate, _choose_event_rows

def test_passes_fallback_gate():
    gates = {
        "min_t_stat": 2.5,
        "min_after_cost_expectancy_bps": 1.0,
        "min_sample_size": 100
    }
    # Pass
    row_pass = {
        "t_stat": 3.0,
        "after_cost_expectancy_per_trade": 0.0002, # 2 bps
        "n_events": 150
    }
    assert _passes_fallback_gate(row_pass, gates) == True
    
    # Fail T-stat
    row_fail_t = row_pass.copy()
    row_fail_t["t_stat"] = 2.0
    assert _passes_fallback_gate(row_fail_t, gates) == False

def test_choose_event_rows_fallback():
    phase2_df = pd.DataFrame([{
        "candidate_id": "test_1",
        "t_stat": 3.0,
        "after_cost_expectancy_per_trade": 0.0002,
        "stressed_after_cost_expectancy_per_trade": 0.0001, # Added this
        "n_events": 150,
        "rejected": False, # NOT a discovery
        "robustness_score": 0.8,
        "expectancy_per_trade": 0.0003,
        "cost_ratio": 0.1
    }])
    
    # In discovery mode, should return nothing
    selected, diag, _ = _choose_event_rows(
        "run_test", "event_test", [], phase2_df, 1, True, True, 50, mode="discovery"
    )
    assert len(selected) == 0
    
    # In fallback mode, should return test_1
    selected, diag, _ = _choose_event_rows(
        "run_test", "event_test", [], phase2_df, 1, True, True, 50, mode="fallback"
    )
    assert len(selected) == 1
    assert selected[0]["candidate_id"] == "test_1"
