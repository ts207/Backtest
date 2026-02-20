"""Canary tests for Phase 2 fallback compilation semantics.

Canary A: A candidate with is_discovery=False but 2/3 gate passes (score=0.667)
          must be marked compile_eligible_phase2_fallback=True and
          promotion_track="fallback_only".

Canary B: A candidate compiled under fallback must be tagged promotion_track="fallback_only"
          and must NOT enter the bridge queue unless explicitly allowed.
"""
from __future__ import annotations

import json
import pytest


def _make_row(econ: bool, econ_cons: bool, stability: bool, n_events: int = 200, is_discovery: bool = False) -> dict:
    """Build a synthetic Phase 2 result row for canary testing.

    IMPORTANT: gate_phase2_final requires is_discovery=True (post-FDR invariant).
    promotion_track is "standard" only when gate_phase2_final=True.
    """
    score = (float(econ) + float(econ_cons) + float(stability)) / 3.0
    # Mirrors post-FDR pipeline logic: gate_phase2_final requires discovery
    gate_phase2_final = econ_cons and stability and is_discovery
    return {
        "candidate_id": "TEST_CANARY",
        "family_id": "TEST_CANARY_family",
        "event_type": "LIQUIDATION_CASCADE",
        "rule_template": "mean_reversion",
        "horizon": "15m",
        "symbol": "BTCUSDT",
        "conditioning": "all",
        "gate_economic": econ,
        "gate_economic_conservative": econ_cons,
        "gate_stability": stability,
        "gate_phase2_final": gate_phase2_final,
        "is_discovery": is_discovery,
        "phase2_quality_score": score,
        "phase2_quality_components": json.dumps({
            "econ": int(econ), "econ_cons": int(econ_cons), "stability": int(stability)
        }, sort_keys=True),
        "n_events": n_events,
        "robustness_score": score,
        "promotion_track": "standard" if gate_phase2_final else "fallback_only",
    }


def _evaluate_fallback_eligibility(row: dict, quality_floor: float = 0.66, min_events: int = 100) -> dict:
    """Apply the compile_eligible_phase2_fallback rule to a row."""
    eligible = (
        row["phase2_quality_score"] >= quality_floor
        and row["n_events"] >= min_events
    )
    return {**row, "compile_eligible_phase2_fallback": eligible}


# ─────────────────────────────────────────────
# Canary A: Fallback is Feasible
# ─────────────────────────────────────────────

class TestCanaryA:
    """Canary A: is_discovery=False, 2/3 gates pass → score=0.667 → fallback eligible."""

    def test_score_correct(self):
        row = _make_row(econ=True, econ_cons=True, stability=False)
        assert abs(row["phase2_quality_score"] - 2 / 3) < 1e-9, (
            f"Expected score 0.667, got {row['phase2_quality_score']}"
        )

    def test_promotion_track_is_fallback_only(self):
        row = _make_row(econ=True, econ_cons=True, stability=False)
        # is_discovery=False so gate_phase2_final=False → fallback_only
        assert row["promotion_track"] == "fallback_only", (
            f"Expected 'fallback_only', got {row['promotion_track']}"
        )

    def test_fallback_eligible_when_floor_met(self):
        row = _make_row(econ=True, econ_cons=True, stability=False, n_events=200)
        row = _evaluate_fallback_eligibility(row, quality_floor=0.66, min_events=100)
        assert row["compile_eligible_phase2_fallback"] is True, (
            f"Should be eligible: score={row['phase2_quality_score']:.3f} >= 0.66 and n_events={row['n_events']} >= 100"
        )

    def test_fallback_not_eligible_when_score_below_floor(self):
        row = _make_row(econ=True, econ_cons=False, stability=False, n_events=200)
        # 1/3 gates → score=0.333 < 0.66 floor
        row = _evaluate_fallback_eligibility(row, quality_floor=0.66, min_events=100)
        assert row["compile_eligible_phase2_fallback"] is False, (
            f"Should NOT be eligible: score={row['phase2_quality_score']:.3f} < 0.66"
        )

    def test_fallback_not_eligible_when_n_events_below_floor(self):
        row = _make_row(econ=True, econ_cons=True, stability=False, n_events=50)
        row = _evaluate_fallback_eligibility(row, quality_floor=0.66, min_events=100)
        assert row["compile_eligible_phase2_fallback"] is False, (
            f"Should NOT be eligible: n_events={row['n_events']} < 100"
        )

    def test_quality_components_schema(self):
        row = _make_row(econ=True, econ_cons=True, stability=False)
        components = json.loads(row["phase2_quality_components"])
        assert set(components.keys()) == {"econ", "econ_cons", "stability"}
        assert components["econ"] == 1
        assert components["econ_cons"] == 1
        assert components["stability"] == 0


# ─────────────────────────────────────────────
# Canary B: Fallback Cannot Promote to Bridge
# ─────────────────────────────────────────────

class TestCanaryB:
    """Canary B: Fallback-compiled blueprint must be tagged fallback_only and not enter bridge."""

    def test_no_discovery_always_fallback_track(self):
        row = _make_row(econ=True, econ_cons=True, stability=True)
        # Even all 3 gates pass, if is_discovery=False → promotion_track must be fallback_only
        # (gate_phase2_final requires discovery)
        assert row["is_discovery"] is False
        assert row["promotion_track"] == "fallback_only", (
            "Without is_discovery=True, promotion_track must be 'fallback_only' "
            "regardless of quality score."
        )

    def test_standard_track_requires_discovery(self):
        """Standard track must only be set after BH-FDR confirms is_discovery=True."""
        # Simulate post-FDR refresh: discovery granted
        row = _make_row(econ=True, econ_cons=True, stability=True)
        row["is_discovery"] = True
        row["gate_phase2_final"] = True
        row["promotion_track"] = "standard" if row["gate_phase2_final"] else "fallback_only"
        assert row["promotion_track"] == "standard"

    def test_fallback_blueprint_not_in_bridge_queue(self):
        """Simulate bridge queue: fallback_only blueprints must not appear unless explicitly allowed."""
        blueprints = [
            {"id": "bp_001", "promotion_track": "fallback_only"},
            {"id": "bp_002", "promotion_track": "standard"},
            {"id": "bp_003", "promotion_track": "fallback_only"},
        ]
        allow_fallback_into_bridge = False  # Default / strict mode

        bridge_queue = [
            bp for bp in blueprints
            if bp["promotion_track"] == "standard"
            or (allow_fallback_into_bridge and bp["promotion_track"] == "fallback_only")
        ]

        assert len(bridge_queue) == 1, (
            f"Only standard-track blueprints should enter bridge (got {len(bridge_queue)})"
        )
        assert bridge_queue[0]["id"] == "bp_002"

    def test_fallback_enters_bridge_when_explicitly_allowed(self):
        """With allow_fallback_into_bridge=True, fallback blueprints may enter bridge."""
        blueprints = [
            {"id": "bp_001", "promotion_track": "fallback_only"},
            {"id": "bp_002", "promotion_track": "standard"},
        ]
        allow_fallback_into_bridge = True

        bridge_queue = [
            bp for bp in blueprints
            if bp["promotion_track"] == "standard"
            or (allow_fallback_into_bridge and bp["promotion_track"] == "fallback_only")
        ]
        assert len(bridge_queue) == 2, "Both should enter bridge when explicitly allowed."
