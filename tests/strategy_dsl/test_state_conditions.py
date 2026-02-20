from project.strategy_dsl.contract_v1 import normalize_entry_condition

def test_normalize_carry_condition():
    cond, nodes, _ = normalize_entry_condition("carry_pos", event_type="test", candidate_id="1")
    assert cond == "carry_pos"
    assert nodes[0].feature == "carry_state_code"
    assert nodes[0].value == 1.0

def test_normalize_vol_condition():
    cond, nodes, _ = normalize_entry_condition("vol_regime_high", event_type="test", candidate_id="1")
    assert cond == "vol_regime_high"
    assert nodes[0].feature == "vol_regime_code"
    assert nodes[0].value == 2.0
