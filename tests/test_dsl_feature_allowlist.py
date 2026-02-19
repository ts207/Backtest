
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "project"))

import pytest
from strategy_dsl.contract_v1 import validate_feature_references

def test_allowlist_accepts_safe_feature_node():
    bp = {"entry": {"condition_nodes": [{"feature": "vol_regime_low"}], "conditions": []}}
    validate_feature_references(bp)

def test_denylist_rejects_forward_return_string():
    bp = {"entry": {"condition_nodes": [], "conditions": ["fwd_ret_16 >= 0.01"]}}
    with pytest.raises(ValueError):
        validate_feature_references(bp)

def test_unknown_prefix_rejected():
    bp = {"entry": {"condition_nodes": [{"feature": "weird_future_feature"}], "conditions": []}}
    with pytest.raises(ValueError):
        validate_feature_references(bp)
