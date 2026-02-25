import pytest
from strategy_dsl.contract_v1 import normalize_entry_condition, is_executable_condition

def test_liquidity_dislocation_mr_filters():
    # Test volatility gate: vol_regime != "SHOCK"
    # Note: "SHOCK" regime code is 3 (LOW=0, MID=1, HIGH=2, SHOCK=3)
    # The spec used a string comparison, but for runtime we likely use vol_regime_code
    
    # Check severity gate
    cond_sev, nodes_sev, _ = normalize_entry_condition("liquidity_vacuum_shock_return > 0.001", event_type="LIQUIDITY_VACUUM", candidate_id="test")
    assert cond_sev == "liquidity_vacuum_shock_return>0.001"
    assert nodes_sev[0].feature == "liquidity_vacuum_shock_return"
    assert nodes_sev[0].operator == ">"
    assert nodes_sev[0].value == 0.001

def test_vol_regime_filter_variants():
    # Verify that we can express vol_regime != 3 (SHOCK)
    # The current regex supports ==, >=, <=, >, <. Let's see if != is supported.
    # Looking at contract_v1.py: r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|==|>|<)\s*(-?\d+(?:\.\d+)?)\s*$"
    # It seems != is NOT in the regex. 
    # I should probably use vol_regime_code <= 2.0 to exclude SHOCK(3).
    
    is_valid = is_executable_condition("vol_regime_code <= 2.0")
    assert is_valid is True
    
    cond, nodes, _ = normalize_entry_condition("vol_regime_code <= 2.0", event_type="LIQUIDITY_DISLOCATION", candidate_id="test")
    assert nodes[0].feature == "vol_regime_code"
    assert nodes[0].operator == "<="
    assert nodes[0].value == 2.0
