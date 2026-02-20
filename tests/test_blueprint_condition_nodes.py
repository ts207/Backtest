"""
Regression tests: Blueprint condition node enforcement.

Verifies:
  1. Runtime-enforceable conditions (vol_regime_high, session_asia) produce >=1 ConditionNodeSpec.
  2. Severity-bucket candidates routed to 'all' produce 0 ConditionNodeSpecs.
  3. Compiled condition strings never contain '__' (legacy format).
  4. Compiled condition strings never equal a rule-template name.
  5. _condition_routing strict mode marks unknown non-bucket names as 'blocked'.
  6. _condition_routing permissive mode falls back to 'all' for unknowns.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1] / "project"
sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Part 1: normalize_entry_condition produces correct condition nodes
# ---------------------------------------------------------------------------

class TestConditionNodeProduction:
    """Verify that known executable conditions produce proper ConditionNodeSpecs."""

    def test_vol_regime_high_produces_one_node(self):
        from strategy_dsl.contract_v1 import normalize_entry_condition
        canonical, nodes, sym = normalize_entry_condition(
            "vol_regime_high",
            event_type="LIQUIDATION_CASCADE",
            candidate_id="test",
        )
        assert canonical == "vol_regime_high"
        assert len(nodes) == 1, f"Expected 1 node, got {len(nodes)}"
        assert nodes[0].feature == "vol_regime_code"
        assert nodes[0].operator == "=="
        assert nodes[0].value == pytest.approx(2.0)
        assert sym is None

    def test_session_asia_produces_one_node(self):
        from strategy_dsl.contract_v1 import normalize_entry_condition
        canonical, nodes, sym = normalize_entry_condition(
            "session_asia",
            event_type="LIQUIDATION_CASCADE",
            candidate_id="test",
        )
        assert canonical == "session_asia"
        assert len(nodes) == 1
        assert nodes[0].feature == "session_hour_utc"
        assert nodes[0].operator == "in_range"
        assert sym is None

    def test_all_produces_zero_nodes(self):
        from strategy_dsl.contract_v1 import normalize_entry_condition
        canonical, nodes, sym = normalize_entry_condition(
            "all",
            event_type="LIQUIDATION_CASCADE",
            candidate_id="test",
        )
        assert canonical == "all"
        assert nodes == []
        assert sym is None

    def test_severity_bucket_routed_to_all_produces_zero_nodes(self):
        """
        A candidate conditioned on severity_bucket_extreme_5pct must have its
        *condition* field set to 'all' by _condition_routing, so normalize
        receives 'all' and produces 0 nodes.  The research label is preserved
        in the *conditioning* column.
        """
        from strategy_dsl.contract_v1 import normalize_entry_condition
        from pipelines.research.phase2_candidate_discovery import _condition_routing

        # _condition_routing correctly routes severity_bucket → "all"
        condition_str, source = _condition_routing("severity_bucket_extreme_5pct")
        assert condition_str == "all", (
            f"severity_bucket must route to 'all'; got '{condition_str}'"
        )
        assert source == "bucket_non_runtime"

        # When normalize receives "all", produces 0 nodes
        canonical, nodes, sym = normalize_entry_condition(
            condition_str,
            event_type="LIQUIDATION_CASCADE",
            candidate_id="test",
        )
        assert canonical == "all"
        assert nodes == []


# ---------------------------------------------------------------------------
# Part 2: Compiled condition string invariants
# ---------------------------------------------------------------------------

class TestConditionStringInvariants:
    """Verify compile-time guards on condition string format."""

    _RULE_TEMPLATES = {"mean_reversion", "continuation", "carry", "breakout"}

    def test_no_double_underscore_in_executable_conditions(self):
        """
        'all__<name>' prefixed strings are technically accepted by normalize_entry_condition
        (they map to unconditional 'all' as a pass-through), but they MUST produce 0
        condition_nodes.  The contract is: if you want enforcement, don't use the prefix.
        """
        from strategy_dsl.contract_v1 import normalize_entry_condition

        # These should all produce canonical='all' and 0 nodes (enforcement silently dropped)
        bad = [
            "all__vol_regime_high",
            "all__session_asia",
            "all__severity_bucket_extreme_5pct",
        ]
        for s in bad:
            canonical, nodes, _ = normalize_entry_condition(s, event_type="_", candidate_id="_")
            assert canonical == "all" and nodes == [], (
                f"'{s}' must produce canonical='all' and 0 nodes (enforcement lost), "
                f"got canonical='{canonical}', nodes={nodes}"
            )

    def test_rule_template_names_are_not_executable_conditions(self):
        from strategy_dsl.contract_v1 import is_executable_condition
        for name in self._RULE_TEMPLATES:
            assert not is_executable_condition(name), (
                f"'{name}' is a rule template, must not be an executable condition"
            )

    def test_condition_routing_never_returns_legacy_all_prefix(self):
        """
        _condition_routing must never return an 'all__<name>' legacy format.
        Note: '__BLOCKED__' is the strict-mode sentinel (not a legacy format) and is excluded.
        """
        from pipelines.research.phase2_candidate_discovery import _condition_routing

        inputs = [
            "vol_regime_high", "vol_regime_low", "session_asia",
            "severity_bucket_extreme_5pct", "severity_bucket_top_10pct",
            "quantile_95", "all", "",
        ]
        for inp in inputs:
            condition_str, _ = _condition_routing(inp)  # strict=True by default
            assert not condition_str.startswith("all__"), (
                f"_condition_routing({inp!r}) returned '{condition_str}' "
                f"which uses the forbidden 'all__' legacy prefix"
            )
        # Also test permissive mode (unknown names become 'all', never 'all__<name>')
        for inp in ["some_unknown_xyz", "col_val", "unknown"]:
            condition_str, _ = _condition_routing(inp, strict=False)
            assert not condition_str.startswith("all__"), (
                f"_condition_routing({inp!r}, strict=False) returned '{condition_str}' "
                f"which uses the forbidden 'all__' legacy prefix"
            )

    def test_condition_routing_never_returns_rule_template_name(self):
        from pipelines.research.phase2_candidate_discovery import _condition_routing

        for name in self._RULE_TEMPLATES:
            condition_str, _ = _condition_routing(name)
            assert condition_str.lower() not in self._RULE_TEMPLATES, (
                f"_condition_routing({name!r}) returned a rule template name: '{condition_str}'"
            )


# ---------------------------------------------------------------------------
# Part 3: _condition_routing strict vs permissive mode
# ---------------------------------------------------------------------------

class TestConditionRoutingModes:
    """Verify strict and permissive mode semantics for _condition_routing."""

    def test_strict_runtime_condition_passes_unchanged(self):
        from pipelines.research.phase2_candidate_discovery import _condition_routing
        cond, source = _condition_routing("vol_regime_high", strict=True)
        assert cond == "vol_regime_high"
        assert source == "runtime"

    def test_strict_severity_bucket_routes_to_all_not_blocked(self):
        """Bucket prefixes are unconditionally allowed (non-runtime), never 'blocked'."""
        from pipelines.research.phase2_candidate_discovery import _condition_routing
        cond, source = _condition_routing("severity_bucket_extreme_5pct", strict=True)
        assert cond == "all"
        assert source == "bucket_non_runtime"

    def test_strict_unknown_name_returns_blocked(self):
        from pipelines.research.phase2_candidate_discovery import _condition_routing
        cond, source = _condition_routing("some_research_bucket_xyz_v2", strict=True)
        assert cond == "__BLOCKED__"
        assert source == "blocked"

    def test_permissive_unknown_name_returns_all(self):
        from pipelines.research.phase2_candidate_discovery import _condition_routing
        cond, source = _condition_routing("some_research_bucket_xyz_v2", strict=False)
        assert cond == "all"
        assert source == "permissive_fallback"

    def test_permissive_severity_bucket_still_non_runtime(self):
        """Bucket prefix early-exit overrides permissive mode — source is always 'bucket_non_runtime'."""
        from pipelines.research.phase2_candidate_discovery import _condition_routing
        cond, source = _condition_routing("severity_bucket_top_10pct", strict=False)
        assert cond == "all"
        assert source == "bucket_non_runtime"  # Not 'permissive_fallback'

    def test_empty_and_all_always_unconditional(self):
        from pipelines.research.phase2_candidate_discovery import _condition_routing
        for name in ("", "all"):
            for strict in (True, False):
                cond, source = _condition_routing(name, strict=strict)
                assert cond == "all"
                assert source == "unconditional"

    def test_blocked_condition_is_not_executable(self):
        """__BLOCKED__ sentinel must NOT be executable — prevents compilation."""
        from strategy_dsl.contract_v1 import is_executable_condition
        assert not is_executable_condition("__BLOCKED__"), (
            "'__BLOCKED__' must not be executable; it is a sentinel for rejected conditions"
        )

    def test_all_returned_values_are_executable_or_blocked(self):
        """All non-blocked outputs from _condition_routing must be executable."""
        from pipelines.research.phase2_candidate_discovery import _condition_routing
        from strategy_dsl.contract_v1 import is_executable_condition

        test_inputs = [
            ("vol_regime_high", True), ("session_asia", True), ("bull_bear_bull", True),
            ("severity_bucket_extreme_5pct", True), ("severity_bucket_top_10pct", False),
            ("quantile_95", True), ("some_unknown_xyz", True), ("some_unknown_xyz", False),
            ("all", True), ("", False),
        ]
        for name, strict in test_inputs:
            cond, source = _condition_routing(name, strict=strict)
            if source == "blocked":
                assert not is_executable_condition(cond), (
                    f"Blocked sentinel '{cond}' must not be executable"
                )
            else:
                assert is_executable_condition(cond), (
                    f"_condition_routing({name!r}, strict={strict}) returned '{cond}' (source='{source}') "
                    f"which is not executable"
                )
