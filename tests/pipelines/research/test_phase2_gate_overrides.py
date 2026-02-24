from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research import phase2_candidate_discovery as p2


def test_resolve_phase2_gate_params_applies_event_override():
    gate_v1_phase2 = {
        "max_q_value": 0.05,
        "min_after_cost_expectancy_bps": 0.1,
        "min_sample_size": 50,
        "quality_floor_fallback": 0.66,
        "min_events_fallback": 100,
        "conservative_cost_multiplier": 1.5,
        "event_overrides": {
            "OI_SPIKE_NEGATIVE": {
                "min_after_cost_expectancy_bps": 0.0,
                "conservative_cost_multiplier": 1.1,
            }
        },
    }

    cfg = p2._resolve_phase2_gate_params(gate_v1_phase2, "OI_SPIKE_NEGATIVE")
    assert cfg["max_q_value"] == 0.05
    assert cfg["min_after_cost_expectancy_bps"] == 0.0
    assert cfg["conservative_cost_multiplier"] == 1.1
    assert cfg["min_sample_size"] == 50


def test_select_phase2_gate_spec_auto_research_uses_discovery_profile():
    gates_spec = {
        "gate_v1_phase2": {"max_q_value": 0.05},
        "gate_v1_phase2_profiles": {
            "discovery": {"max_q_value": 0.10},
            "promotion": {"max_q_value": 0.01},
        },
    }
    selected = p2._select_phase2_gate_spec(gates_spec, mode="research", gate_profile="auto")
    assert selected["max_q_value"] == 0.10
    assert selected["_resolved_profile"] == "discovery"


def test_select_phase2_gate_spec_auto_production_uses_promotion_profile():
    gates_spec = {
        "gate_v1_phase2": {"max_q_value": 0.05},
        "gate_v1_phase2_profiles": {
            "discovery": {"max_q_value": 0.10},
            "promotion": {"max_q_value": 0.01},
        },
    }
    selected = p2._select_phase2_gate_spec(gates_spec, mode="production", gate_profile="auto")
    assert selected["max_q_value"] == 0.01
    assert selected["_resolved_profile"] == "promotion"


def test_load_family_spec_reads_repo_spec():
    spec = p2._load_family_spec()
    families = spec.get("families", {})
    assert "OI_FLUSH" in families
    assert families["OI_FLUSH"]["templates"] == [
        "reversal_or_squeeze",
        "mean_reversion",
        "continuation",
        "exhaustion_reversal",
        "convexity_capture",
        "only_if_funding",
        "only_if_oi",
        "tail_risk_avoid",
    ]


def test_resolve_phase2_gate_params_liquidity_vacuum_tuned_override():
    gates_spec = p2._load_gates_spec()
    gates = p2._select_phase2_gate_spec(gates_spec, mode="research", gate_profile="auto")
    cfg = p2._resolve_phase2_gate_params(gates, "LIQUIDITY_VACUUM")
    assert cfg["max_q_value"] == 0.10
    assert cfg["min_after_cost_expectancy_bps"] == -2.0
    assert cfg["conservative_cost_multiplier"] == 1.0
    assert cfg["require_sign_stability"] is False


def test_promotion_profile_does_not_include_discovery_event_relaxation():
    gates_spec = p2._load_gates_spec()
    gates = p2._select_phase2_gate_spec(gates_spec, mode="production", gate_profile="auto")
    cfg = p2._resolve_phase2_gate_params(gates, "LIQUIDITY_VACUUM")
    assert cfg["max_q_value"] == 0.05
    assert cfg["min_after_cost_expectancy_bps"] == 0.1
    assert cfg["conservative_cost_multiplier"] == 1.5
    assert cfg["require_sign_stability"] is True


def test_load_family_spec_supports_conditioning_cols_override():
    spec = p2._load_family_spec()
    families = spec.get("families", {})
    assert families["LIQUIDITY_VACUUM"]["templates"] == [
        "mean_reversion",
        "stop_run_repair",
        "overshoot_repair",
        "continuation",
        "only_if_liquidity",
        "slippage_aware_filter",
    ]
    assert families["LIQUIDITY_VACUUM"]["horizons"] == ["15m"]
    assert families["LIQUIDITY_VACUUM"]["conditioning_cols"] == []
    assert families["LIQUIDATION_CASCADE"]["horizons"] == ["5m"]
    assert families["LIQUIDATION_CASCADE"]["conditioning_cols"] == []
