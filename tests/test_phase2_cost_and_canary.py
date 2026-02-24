"""
Tests for Phase 2 spec-bound cost resolution and true label-shift canary.

Issue 1 – Cost resolution
--------------------------
Phase 2 must resolve execution costs from fees.yaml via resolve_execution_costs(),
not from a hardcoded --mock_cost_bps CLI float. The resolved coordinate (config_digest,
cost_bps, fee_bps_per_side, slippage_bps_per_fill) must be recorded in each candidate
row and in phase2_report.json.

Issue 2 – Label-shift canary
------------------------------
The label-shift canary must implement a *true* k-bar shift (future_pos = pos + horizon + k)
rather than replacing returns with np.random.randn()*0.001. A true shift:
  - Tests the specific "label misalignment" failure mode (not just random noise)
  - Is deterministic: calling twice with the same inputs yields identical results
  - Produces a mean return equal to what you'd compute manually with future_pos+k
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1] / "project"
sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_features(n_bars: int = 80, bar_minutes: int = 5,
                   start_ts: str = "2024-01-01 00:00:00") -> pd.DataFrame:
    """Deterministic features table: close[i] = i + 1.0 (1-indexed)."""
    ts = pd.date_range(start_ts, periods=n_bars, freq=f"{bar_minutes}min", tz="UTC")
    close = np.arange(1.0, float(n_bars) + 1.0)
    return pd.DataFrame({"timestamp": ts, "close": close})


def _make_events(features_df: pd.DataFrame, n_events: int = 40,
                 start_bar: int = 5) -> pd.DataFrame:
    """Events at exact feature bar timestamps (no sub-bar offset)."""
    ts = features_df["timestamp"].iloc[start_bar: start_bar + n_events].values
    return pd.DataFrame({"timestamp": ts})


# ===========================================================================
# Issue 2 – True label-shift canary
# ===========================================================================

class TestLabelShiftCanary:
    """calculate_expectancy must accept shift_labels_k (int), not shift_labels (bool)."""

    def test_shift_labels_k_accepted_as_kwarg(self):
        """Function signature must accept shift_labels_k keyword argument."""
        from pipelines.research.phase2_candidate_discovery import calculate_expectancy

        features = _make_features()
        events = _make_events(features)
        # With current code (shift_labels: bool), this raises TypeError.
        result = calculate_expectancy(events, features, "continuation", "5m",
                                      shift_labels_k=0)
        assert len(result) == 4

    def test_shift_labels_k0_matches_no_shift(self):
        """shift_labels_k=0 must give the same result as no shift at all."""
        from pipelines.research.phase2_candidate_discovery import calculate_expectancy

        features = _make_features()
        events = _make_events(features)
        result_plain = calculate_expectancy(events, features, "continuation", "5m")
        result_k0 = calculate_expectancy(events, features, "continuation", "5m",
                                         shift_labels_k=0)
        assert result_plain[0] == pytest.approx(result_k0[0], abs=1e-9)

    def test_shift_k5_differs_from_k0_on_deterministic_prices(self):
        """Shifting by 5 bars must produce a different mean return from k=0."""
        from pipelines.research.phase2_candidate_discovery import calculate_expectancy

        features = _make_features(n_bars=80)
        events = _make_events(features, n_events=30, start_bar=5)
        mean_k0, _, _, _ = calculate_expectancy(events, features, "continuation", "5m",
                                                 shift_labels_k=0)
        mean_k5, _, _, _ = calculate_expectancy(events, features, "continuation", "5m",
                                                 shift_labels_k=5)
        assert abs(mean_k0 - mean_k5) > 1e-6, (
            f"shift must change mean return; k0={mean_k0:.8f}, k5={mean_k5:.8f}"
        )

    def test_shift_k_uses_pos_plus_horizon_plus_k(self):
        """
        With shift_labels_k=K and horizon='5m' (1 bar), and default entry_lag_bars=1,
        the forward return for an event at feature bar P must use
        close[P + entry_lag_bars + 1 + K] / close[P + entry_lag_bars] - 1.

        close[i] = i+1, so return at (P, K) = (P+3+K)/(P+2) - 1.
        """
        from pipelines.research.phase2_candidate_discovery import calculate_expectancy

        n_bars = 80
        features = _make_features(n_bars=n_bars)
        events = _make_events(features, n_events=40, start_bar=5)

        shift_k = 5
        mean_k5, _, n, _ = calculate_expectancy(events, features, "continuation", "5m",
                                                  shift_labels_k=shift_k)

        # Compute expected mean manually using pos + horizon_bars + shift_k
        close = features["close"].values
        feat_ts = features["timestamp"].values
        event_ts = events["timestamp"].values
        expected_returns = []
        for ts in event_ts:
            pos = int(np.searchsorted(feat_ts, ts, side="left"))
            entry_pos = pos + 1  # entry_lag_bars=1 (default)
            future_pos = entry_pos + 1 + shift_k  # horizon_bars=1, shift_k=5
            if 0 <= pos < len(close) and future_pos < len(close):
                ret = close[future_pos] / close[entry_pos] - 1.0
                expected_returns.append(ret)  # direction=+1 for continuation
        expected_mean = float(np.mean(expected_returns)) if expected_returns else 0.0

        assert abs(mean_k5 - expected_mean) < 1e-9, (
            f"shift_labels_k=5 must use (pos+entry_lag)+horizon+5; got {mean_k5:.10f}, "
            f"expected {expected_mean:.10f}"
        )

    def test_shift_is_deterministic(self):
        """
        Regression: the old canary used np.random.randn()*0.001 (non-deterministic).
        The new shift_labels_k must be fully deterministic: two calls with the same
        input must return identical results.
        """
        from pipelines.research.phase2_candidate_discovery import calculate_expectancy

        features = _make_features(n_bars=80)
        events = _make_events(features, n_events=40, start_bar=5)

        run1 = calculate_expectancy(events, features, "continuation", "5m",
                                    shift_labels_k=3)
        run2 = calculate_expectancy(events, features, "continuation", "5m",
                                    shift_labels_k=3)

        assert run1[0] == pytest.approx(run2[0], abs=1e-12), (
            "shift_labels_k must be deterministic (no random noise)"
        )

    def test_shift_labels_old_bool_arg_removed(self):
        """
        The old shift_labels: bool parameter must be gone.
        Passing shift_labels=True must raise TypeError (unknown keyword).
        """
        from pipelines.research.phase2_candidate_discovery import calculate_expectancy

        features = _make_features()
        events = _make_events(features)
        with pytest.raises(TypeError):
            calculate_expectancy(events, features, "continuation", "5m",
                                  shift_labels=True)


# ===========================================================================
# Issue 1 – Spec-bound cost resolution
# ===========================================================================

class TestPhase2CostResolution:
    """Phase 2 must resolve costs from fees.yaml, not --mock_cost_bps."""

    def test_resolve_phase2_costs_function_exists(self):
        """_resolve_phase2_costs must be importable from phase2_candidate_discovery."""
        from pipelines.research import phase2_candidate_discovery

        assert hasattr(phase2_candidate_discovery, "_resolve_phase2_costs"), (
            "_resolve_phase2_costs() helper must exist"
        )

    def test_resolve_phase2_costs_reads_fees_yaml(self, tmp_path):
        """Cost must come from fees.yaml (fee+slippage), not a CLI default of 5 bps."""
        from pipelines.research.phase2_candidate_discovery import _resolve_phase2_costs

        configs = tmp_path / "configs"
        configs.mkdir()
        (configs / "fees.yaml").write_text("fee_bps_per_side: 3\nslippage_bps_per_fill: 1\n")
        (configs / "pipeline.yaml").write_text("{}\n")

        args = argparse.Namespace(fees_bps=None, slippage_bps=None, cost_bps=None)
        cost_bps, coordinate = _resolve_phase2_costs(args, project_root=tmp_path)

        assert cost_bps == pytest.approx(4.0, abs=1e-6), (
            f"cost_bps must be fee(3)+slippage(1)=4.0, got {cost_bps}"
        )

    def test_cost_coordinate_contains_required_fields(self, tmp_path):
        """Cost coordinate dict must include config_digest, cost_bps, fee/slippage fields."""
        from pipelines.research.phase2_candidate_discovery import _resolve_phase2_costs

        configs = tmp_path / "configs"
        configs.mkdir()
        (configs / "fees.yaml").write_text("fee_bps_per_side: 4\nslippage_bps_per_fill: 2\n")
        (configs / "pipeline.yaml").write_text("{}\n")

        args = argparse.Namespace(fees_bps=None, slippage_bps=None, cost_bps=None)
        _, coordinate = _resolve_phase2_costs(args, project_root=tmp_path)

        for field in ("config_digest", "cost_bps", "fee_bps_per_side", "slippage_bps_per_fill"):
            assert field in coordinate, f"cost_coordinate must include '{field}'"

    def test_fees_bps_override_arg_works(self, tmp_path):
        """CLI --fees_bps must override fees.yaml fee; slippage still from yaml."""
        from pipelines.research.phase2_candidate_discovery import _resolve_phase2_costs

        configs = tmp_path / "configs"
        configs.mkdir()
        (configs / "fees.yaml").write_text("fee_bps_per_side: 4\nslippage_bps_per_fill: 2\n")
        (configs / "pipeline.yaml").write_text("{}\n")

        # Override fee to 1; slippage stays at 2 from yaml → total = 3
        args = argparse.Namespace(fees_bps=1.0, slippage_bps=None, cost_bps=None)
        cost_bps, coordinate = _resolve_phase2_costs(args, project_root=tmp_path)

        assert cost_bps == pytest.approx(3.0, abs=1e-6), (
            f"fees_bps override=1, slippage=2 → cost must be 3.0, got {cost_bps}"
        )
        assert coordinate["fee_bps_per_side"] == pytest.approx(1.0, abs=1e-6)

    def test_parser_exposes_make_parser(self):
        """Module must expose _make_parser() so the parser can be inspected in tests."""
        from pipelines.research import phase2_candidate_discovery

        assert hasattr(phase2_candidate_discovery, "_make_parser"), (
            "_make_parser() must be exposed at module level"
        )

    def test_parser_has_no_mock_cost_bps(self):
        """Argument parser must NOT have --mock_cost_bps."""
        from pipelines.research.phase2_candidate_discovery import _make_parser

        parser = _make_parser()
        option_strings = [
            opt
            for action in parser._actions
            for opt in action.option_strings
        ]
        assert "--mock_cost_bps" not in option_strings, (
            "--mock_cost_bps must be removed; use --fees_bps / --slippage_bps / --cost_bps"
        )

    def test_parser_has_fees_bps_arg(self):
        """Argument parser must have --fees_bps, --slippage_bps, --cost_bps overrides."""
        from pipelines.research.phase2_candidate_discovery import _make_parser

        parser = _make_parser()
        option_strings = [
            opt
            for action in parser._actions
            for opt in action.option_strings
        ]
        for expected in ("--fees_bps", "--slippage_bps", "--cost_bps"):
            assert expected in option_strings, (
                f"Parser must expose {expected} override"
            )

    def test_parser_has_shift_labels_k_not_shift_labels(self):
        """Parser must use --shift_labels_k (int) instead of --shift_labels (bool int)."""
        from pipelines.research.phase2_candidate_discovery import _make_parser

        parser = _make_parser()
        option_strings = [
            opt
            for action in parser._actions
            for opt in action.option_strings
        ]
        assert "--shift_labels" not in option_strings, (
            "--shift_labels must be renamed to --shift_labels_k"
        )
        assert "--shift_labels_k" in option_strings, (
            "--shift_labels_k must be in parser"
        )

    def test_parser_has_cost_calibration_args(self):
        """Parser must expose ToB calibration flags for candidate-level economic gating."""
        from pipelines.research.phase2_candidate_discovery import _make_parser

        parser = _make_parser()
        option_strings = [
            opt
            for action in parser._actions
            for opt in action.option_strings
        ]
        for expected in (
            "--cost_calibration_mode",
            "--cost_min_tob_coverage",
            "--cost_tob_tolerance_minutes",
        ):
            assert expected in option_strings, f"Parser must expose {expected}"
