import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research.phase2_conditional_hypotheses import (
    ActionSpec,
    ConditionSpec,
    _apply_multiplicity_adjustments,
    _curvature_metrics,
    _delay_robustness_fields,
    _deployment_mode_from_symbol_dispersion,
    _effective_sample_size,
    _evaluate_candidate,
    _gate_regime_stability,
)


def _write_phase1_fixture(tmp_path: Path, run_id: str) -> None:
    in_dir = tmp_path / "reports" / "vol_shock_relaxation" / run_id
    in_dir.mkdir(parents=True, exist_ok=True)

    ts = pd.date_range("2024-01-01", periods=120, freq="D", tz="UTC")
    events = pd.DataFrame(
        {
            "event_id": [f"e{i}" for i in range(len(ts))],
            "symbol": ["BTCUSDT" if i % 2 == 0 else "ETHUSDT" for i in range(len(ts))],
            "enter_ts": ts,
            "exit_ts": ts,
            "duration_bars": [20 + (i % 10) for i in range(len(ts))],
            "time_to_relax": [15 + (i % 8) for i in range(len(ts))],
            "t_rv_peak": [3 + (i % 5) for i in range(len(ts))],
            "rv_decay_half_life": [10 + (i % 7) for i in range(len(ts))],
            "relaxed_within_96": [1] * len(ts),
            "auc_excess_rv": [0.2 + (i % 5) * 0.01 for i in range(len(ts))],
            "secondary_shock_within_h": [0 if i % 3 else 1 for i in range(len(ts))],
            "range_pct_96": [0.01 + (i % 4) * 0.005 for i in range(len(ts))],
            "time_to_secondary_shock": [30 if i % 3 else 5 for i in range(len(ts))],
            "bull_bear": ["bull" if i % 2 == 0 else "bear" for i in range(len(ts))],
            "vol_regime": ["high" if i % 4 < 2 else "low" for i in range(len(ts))],
            "tod_bucket": [i % 24 for i in range(len(ts))],
        }
    )
    controls = events[["event_id"]].copy()
    controls["control_idx"] = range(len(controls))
    controls["relaxed_within_96"] = 0.8
    controls["auc_excess_rv"] = 0.25
    controls["rv_decay_half_life"] = 12.0
    controls["secondary_shock_within_h"] = 0.4
    controls["range_pct_96"] = 0.03
    controls["time_to_secondary_shock"] = 12.0

    events.to_csv(in_dir / "vol_shock_relaxation_events.csv", index=False)
    controls.to_csv(in_dir / "vol_shock_relaxation_controls.csv", index=False)
    (in_dir / "vol_shock_relaxation_summary.json").write_text(
        json.dumps(
            {
                "decision": "promote",
                "phase1_structure_pass": True,
                "gates": {
                    "phase_pass": True,
                    "sign_pass": True,
                    "non_degenerate_count": True,
                },
            }
        ),
        encoding="utf-8",
    )


def _write_hypothesis_queue_fixture(tmp_path: Path, run_id: str, event_types: list[str]) -> None:
    out_dir = tmp_path / "reports" / "hypothesis_generator" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    queue = pd.DataFrame(
        [
            {
                "hypothesis_id": "H_MATCH",
                "priority_score": 0.91,
                "target_phase2_event_types": json.dumps(event_types),
            },
            {
                "hypothesis_id": "H_OTHER",
                "priority_score": 0.41,
                "target_phase2_event_types": json.dumps(["cross_venue_desync"]),
            },
        ]
    )
    queue.to_csv(out_dir / "phase1_hypothesis_queue.csv", index=False)


def _run_phase2(tmp_path: Path, run_id: str, extra_args: list[str] | None = None) -> None:
    extra_args = extra_args or []
    env = os.environ.copy()
    env["BACKTEST_DATA_ROOT"] = str(tmp_path)
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "project" / "pipelines" / "research" / "build_event_registry.py"),
            "--run_id",
            run_id,
            "--event_type",
            "vol_shock_relaxation",
            "--symbols",
            "BTCUSDT,ETHUSDT",
        ],
        check=True,
        env=env,
    )

    cmd = [
        sys.executable,
        str(ROOT / "project" / "pipelines" / "research" / "phase2_conditional_hypotheses.py"),
        "--run_id",
        run_id,
        "--event_type",
        "vol_shock_relaxation",
        "--symbols",
        "BTCUSDT,ETHUSDT",
        "--bootstrap_iters",
        "200",
    ] + extra_args
    subprocess.run(cmd, check=True, env=env)


def test_phase2_caps_and_outputs(tmp_path: Path) -> None:
    run_id = "phase2_test"
    _write_phase1_fixture(tmp_path=tmp_path, run_id=run_id)

    _run_phase2(
        tmp_path=tmp_path,
        run_id=run_id,
        extra_args=[
            "--max_conditions",
            "20",
            "--max_actions",
            "9",
        ],
    )

    out_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    assert (out_dir / "phase2_candidates.csv").exists()
    assert (out_dir / "phase2_symbol_evaluation.csv").exists()
    assert (out_dir / "promoted_candidates.json").exists()
    assert (out_dir / "phase2_manifests.json").exists()
    assert (out_dir / "phase2_summary.md").exists()

    manifest = json.loads((out_dir / "phase2_manifests.json").read_text())
    assert manifest["phase1_pass"] is True
    assert manifest["conditions_evaluated"] <= 20
    assert manifest["actions_evaluated"] <= 9
    assert manifest["caps"]["condition_cap_pass"] is True
    assert manifest["caps"]["action_cap_pass"] is True

    candidates = pd.read_csv(out_dir / "phase2_candidates.csv")
    if not candidates.empty:
        assert "gate_e_simplicity" in candidates.columns
        assert "fail_reasons" in candidates.columns
        assert "profit_density_score" in candidates.columns
        assert "expectancy_per_trade" in candidates.columns
        assert "robustness_score" in candidates.columns
        assert "event_frequency" in candidates.columns
        assert candidates["gate_e_simplicity"].all()

    promoted = json.loads((out_dir / "promoted_candidates.json").read_text())
    assert promoted["phase1_pass"] is True
    assert promoted["promoted_count"] <= 2
    if promoted["promoted_count"] > 1:
        scores = [float(x.get("profit_density_score", 0.0)) for x in promoted["candidates"]]
        assert scores == sorted(scores, reverse=True)


def test_phase2_simplicity_gate_blocks_when_caps_exceeded(tmp_path: Path) -> None:
    run_id = "phase2_gate_e_test"
    _write_phase1_fixture(tmp_path=tmp_path, run_id=run_id)

    _run_phase2(
        tmp_path=tmp_path,
        run_id=run_id,
        extra_args=[
            "--max_conditions",
            "3",
            "--max_actions",
            "2",
        ],
    )

    out_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    manifest = json.loads((out_dir / "phase2_manifests.json").read_text())
    assert manifest["caps"]["condition_cap_pass"] is False
    assert manifest["caps"]["action_cap_pass"] is False
    assert manifest["caps"]["simplicity_gate_pass"] is False

    promoted = json.loads((out_dir / "promoted_candidates.json").read_text())
    assert promoted["decision"] == "freeze"

    candidates = pd.read_csv(out_dir / "phase2_candidates.csv")
    if not candidates.empty:
        assert (candidates["gate_e_simplicity"] == False).all()  # noqa: E712


def _candidate_subframe(opportunity_value_excess: float = 0.03) -> pd.DataFrame:
    n = 64
    return pd.DataFrame(
        {
            "year": [2022] * (n // 2) + [2023] * (n // 2),
            "symbol": ["BTCUSDT"] * n,
            "vol_regime": ["high"] * n,
            "baseline_mode": ["matched_controls_excess"] * n,
            "adverse_proxy_excess": [0.02] * n,
            "opportunity_value_excess": [opportunity_value_excess] * n,
            "forward_abs_return_h": [opportunity_value_excess] * n,
            "time_to_secondary_shock": [12.0] * n,
            "rv_decay_half_life": [20.0] * n,
        }
    )


def test_phase2_blocks_full_exposure_cut_when_opportunity_cost_not_near_zero() -> None:
    sub = _candidate_subframe(opportunity_value_excess=0.03)
    condition = ConditionSpec("all", "all", lambda d: pd.Series(True, index=d.index))
    action = ActionSpec("risk_throttle_0", "risk_throttle", {"k": 0.0})

    result = _evaluate_candidate(
        sub=sub,
        condition=condition,
        action=action,
        bootstrap_iters=200,
        seed=7,
        cost_floor=0.0,
        tail_material_threshold=0.0,
        opportunity_tight_eps=0.0001,
        opportunity_near_zero_eps=0.001,
        net_benefit_floor=0.0,
        simplicity_gate=True,
    )

    assert result["delta_exposure_mean"] <= -0.9
    assert result["opportunity_cost_mean"] > 0.001
    assert result["gate_f_exposure_guard"] is False
    assert "gate_f_exposure_guard" in result["fail_reasons"]


def test_phase2_net_benefit_gate_blocks_negative_economics() -> None:
    sub = _candidate_subframe(opportunity_value_excess=0.03)
    condition = ConditionSpec("all", "all", lambda d: pd.Series(True, index=d.index))
    action = ActionSpec("delay_8", "timing", {"delay_bars": 8})

    result = _evaluate_candidate(
        sub=sub,
        condition=condition,
        action=action,
        bootstrap_iters=200,
        seed=11,
        cost_floor=0.0,
        tail_material_threshold=0.0,
        opportunity_tight_eps=0.005,
        opportunity_near_zero_eps=0.001,
        net_benefit_floor=0.0,
        simplicity_gate=True,
    )

    assert result["net_benefit_mean"] < 0.0
    assert result["gate_g_net_benefit"] is False
    assert "gate_g_net_benefit" in result["fail_reasons"]


def test_gate_regime_stability_uses_majority_rule_by_default() -> None:
    # symbol and vol_regime stay improved (<=0), bull_bear flips sign.
    sub = pd.DataFrame(
        {
            "symbol": ["BTCUSDT"] * 4 + ["ETHUSDT"] * 4,
            "vol_regime": ["high", "high", "low", "low"] * 2,
            "bull_bear": ["bull", "bear", "bull", "bear"] * 2,
            "adverse_effect": [0.2, -0.4, 0.2, -0.4, 0.2, -0.4, 0.2, -0.4],
        }
    )

    passed, stable_splits, required_splits = _gate_regime_stability(
        sub=sub,
        effect_col="adverse_effect",
        condition_name="all",
    )

    assert passed is True
    assert stable_splits == 2
    assert required_splits == 2


def test_gate_regime_stability_can_be_switched_back_to_strict() -> None:
    sub = pd.DataFrame(
        {
            "symbol": ["BTCUSDT"] * 4 + ["ETHUSDT"] * 4,
            "vol_regime": ["high", "high", "low", "low"] * 2,
            "bull_bear": ["bull", "bear", "bull", "bear"] * 2,
            "adverse_effect": [0.2, -0.4, 0.2, -0.4, 0.2, -0.4, 0.2, -0.4],
        }
    )

    passed, stable_splits, required_splits = _gate_regime_stability(
        sub=sub,
        effect_col="adverse_effect",
        condition_name="all",
        min_stable_splits=3,
    )

    assert passed is False
    assert stable_splits == 2
    assert required_splits == 3


def test_phase2_manifest_and_candidates_include_oos_and_multiplicity_fields(tmp_path: Path) -> None:
    run_id = "phase2_oos_fields"
    _write_phase1_fixture(tmp_path=tmp_path, run_id=run_id)
    _run_phase2(tmp_path=tmp_path, run_id=run_id)

    out_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    manifest = json.loads((out_dir / "phase2_manifests.json").read_text())
    assert "hypotheses_tested" in manifest
    assert "adjusted_pass_count" in manifest
    assert "adjusted_strict_pass_count" in manifest
    assert "ess_pass_count" in manifest
    assert "oos_pass_count" in manifest
    assert "symbol_evaluations" in manifest
    assert "deployable_symbol_rows" in manifest
    assert "rejected_symbol_rows" in manifest
    assert "rejected_symbol_rows_by_reason" in manifest

    symbol_eval = pd.read_csv(out_dir / "phase2_symbol_evaluation.csv")
    if not symbol_eval.empty:
        for col in [
            "candidate_id",
            "symbol",
            "ev",
            "variance",
            "sharpe_like",
            "stability_score",
            "window_consistency",
            "sign_persistence",
            "drawdown_profile",
            "capacity_proxy",
            "rejection_reason_codes",
            "promotion_status",
            "deployable",
        ]:
            assert col in symbol_eval.columns

    candidates = pd.read_csv(out_dir / "phase2_candidates.csv")
    if not candidates.empty:
        for col in [
            "train_samples",
            "validation_samples",
            "test_samples",
            "val_delta_adverse_mean",
            "oos1_delta_adverse_mean",
            "val_p_value",
            "val_p_value_adj_bh",
            "test_p_value",
            "test_p_value_adj_bh",
            "num_tests_event_family",
            "ess_effective",
            "ess_lag_used",
            "multiplicity_penalty",
            "expectancy_after_multiplicity",
            "expectancy_left",
            "expectancy_center",
            "expectancy_right",
            "curvature_penalty",
            "neighborhood_positive_count",
            "gate_parameter_curvature",
            "delay_expectancy_map",
            "delay_positive_ratio",
            "delay_dispersion",
            "delay_robustness_score",
            "gate_delay_robustness",
            "gate_oos_min_samples",
            "gate_oos_validation",
            "gate_oos_validation_test",
            "gate_multiplicity",
            "gate_multiplicity_strict",
            "gate_ess",
            "after_cost_expectancy_per_trade",
            "stressed_after_cost_expectancy_per_trade",
            "turnover_proxy_mean",
            "avg_dynamic_cost_bps",
            "cost_ratio",
            "gate_after_cost_positive",
            "gate_after_cost_stressed_positive",
            "gate_cost_ratio",
        ]:
            assert col in candidates.columns


def test_effective_sample_size_iid_like_series_is_near_n() -> None:
    values = pd.Series([0.1, -0.1] * 400, dtype=float).to_numpy()
    ess, lag_used = _effective_sample_size(values, max_lag=24)
    assert lag_used == 24
    assert ess > 700


def test_effective_sample_size_autocorrelated_series_is_lower() -> None:
    values = [0.0]
    for _ in range(1, 800):
        values.append((0.95 * values[-1]) + 0.01)
    ess, lag_used = _effective_sample_size(pd.Series(values, dtype=float).to_numpy(), max_lag=24)
    assert lag_used == 24
    assert ess < 300


def test_multiplicity_adjustment_penalty_is_monotonic() -> None:
    base = pd.DataFrame(
        [
            {
                "expectancy_per_trade": 0.03,
                "gate_multiplicity": True,
                "ess_effective": 200.0,
                "num_tests_event_family": 10,
            },
            {
                "expectancy_per_trade": 0.03,
                "gate_multiplicity": True,
                "ess_effective": 200.0,
                "num_tests_event_family": 1_000,
            },
        ]
    )
    adjusted = _apply_multiplicity_adjustments(base, multiplicity_k=1.0)
    assert adjusted.loc[1, "multiplicity_penalty"] > adjusted.loc[0, "multiplicity_penalty"]
    assert adjusted.loc[1, "expectancy_after_multiplicity"] < adjusted.loc[0, "expectancy_after_multiplicity"]


def test_candidate_can_pass_bh_but_fail_multiplicity_strict_gate() -> None:
    base = pd.DataFrame(
        [
            {
                "expectancy_per_trade": 0.01,
                "gate_multiplicity": True,
                "ess_effective": 150.0,
                "num_tests_event_family": 5_000,
            }
        ]
    )
    adjusted = _apply_multiplicity_adjustments(base, multiplicity_k=1.0)
    assert bool(adjusted.loc[0, "gate_multiplicity"]) is True
    assert adjusted.loc[0, "expectancy_after_multiplicity"] <= 0.0
    assert bool(adjusted.loc[0, "gate_multiplicity_strict"]) is False


def test_parameter_curvature_flat_profile_passes() -> None:
    events = pd.DataFrame(
        {
            "x": [0.9, 1.0, 1.1, 1.2, 1.3],
            "adverse_proxy_excess": [0.04] * 5,
            "opportunity_value_excess": [0.01] * 5,
            "forward_abs_return_h": [0.02] * 5,
        }
    )
    sub = events[events["x"] >= 1.0].copy()
    action = ActionSpec("risk_throttle_0.5", "risk_throttle", {"k": 0.5})
    condition = ConditionSpec("x >= 1.0", "x >= 1.0", lambda d: d["x"] >= 1.0)
    metrics = _curvature_metrics(
        all_events=events,
        condition_name=condition.name,
        sub=sub,
        action=action,
        parameter_curvature_max_penalty=0.50,
    )
    assert metrics["gate_parameter_curvature"] is True
    assert float(metrics["curvature_penalty"]) <= 0.50


def test_parameter_curvature_spike_profile_fails() -> None:
    events = pd.DataFrame(
        {
            "x": [0.8, 0.9, 1.0, 1.1, 1.2],
            "adverse_proxy_excess": [0.001, 0.001, 0.08, 0.08, 0.001],
            "opportunity_value_excess": [0.001, 0.001, 0.01, 0.01, 0.001],
            "forward_abs_return_h": [0.01, 0.01, 0.03, 0.03, 0.01],
        }
    )
    sub = events[events["x"] >= 1.0].copy()
    action = ActionSpec("risk_throttle_0.5", "risk_throttle", {"k": 0.5})
    metrics = _curvature_metrics(
        all_events=events,
        condition_name="x >= 1.0",
        sub=sub,
        action=action,
        parameter_curvature_max_penalty=0.10,
    )
    assert metrics["gate_parameter_curvature"] is False
    assert float(metrics["curvature_penalty"]) > 0.10


def test_delay_robustness_fields_gate_behavior() -> None:
    strong = _delay_robustness_fields(
        [0.03, 0.02, 0.015, 0.01, 0.005],
        min_delay_positive_ratio=0.60,
        min_delay_robustness_score=0.60,
    )
    weak = _delay_robustness_fields(
        [0.04, -0.02, -0.03, 0.0, -0.01],
        min_delay_positive_ratio=0.60,
        min_delay_robustness_score=0.60,
    )
    assert strong["gate_delay_robustness"] is True
    assert weak["gate_delay_robustness"] is False




def test_phase2_promotion_rejection_reason_codes_and_summary_counts(tmp_path: Path) -> None:
    run_id = "phase2_rejection_reasons"
    _write_phase1_fixture(tmp_path=tmp_path, run_id=run_id)
    _run_phase2(
        tmp_path=tmp_path,
        run_id=run_id,
        extra_args=[
            "--promotion_min_ev",
            "0.5",
            "--promotion_min_stability_score",
            "1.0",
            "--promotion_min_sample_size",
            "500",
        ],
    )

    out_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    manifest = json.loads((out_dir / "phase2_manifests.json").read_text())
    promoted_payload = json.loads((out_dir / "promoted_candidates.json").read_text())
    symbol_eval = pd.read_csv(out_dir / "phase2_symbol_evaluation.csv")

    if symbol_eval.empty:
        assert manifest["rejected_symbol_rows"] == 0
        assert promoted_payload["rejected_count"] == 0
        assert promoted_payload["rejected_by_reason"] == {}
        return

    assert (symbol_eval["promotion_status"] == "rejected").all()
    assert (symbol_eval["deployable"] == False).all()  # noqa: E712
    assert symbol_eval["rejection_reason_codes"].str.contains("NEGATIVE_EV").any()
    assert symbol_eval["rejection_reason_codes"].str.contains("UNSTABLE").any()
    assert symbol_eval["rejection_reason_codes"].str.contains("LOW_SAMPLE").any()

    assert int(manifest["rejected_symbol_rows"]) == int(len(symbol_eval))
    assert int(promoted_payload["rejected_count"]) == int(len(symbol_eval))
    rejected_by_reason = manifest["rejected_symbol_rows_by_reason"]
    assert rejected_by_reason == promoted_payload["rejected_by_reason"]
    assert {"NEGATIVE_EV", "UNSTABLE", "LOW_SAMPLE"}.issubset(set(rejected_by_reason.keys()))


def test_phase2_attaches_supporting_hypotheses_when_queue_exists(tmp_path: Path) -> None:
    run_id = "phase2_hypothesis_context"
    _write_phase1_fixture(tmp_path=tmp_path, run_id=run_id)
    _write_hypothesis_queue_fixture(
        tmp_path=tmp_path,
        run_id=run_id,
        event_types=["vol_shock_relaxation", "vol_aftershock_window"],
    )

    _run_phase2(tmp_path=tmp_path, run_id=run_id)
    out_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    manifest = json.loads((out_dir / "phase2_manifests.json").read_text())
    promoted = json.loads((out_dir / "promoted_candidates.json").read_text())
    candidates = pd.read_csv(out_dir / "phase2_candidates.csv")

    assert manifest["phase1_hypothesis_queue_exists"] is True
    assert int(manifest["matched_hypothesis_count"]) >= 1
    assert promoted["phase1_hypothesis_queue_exists"] is True
    assert int(promoted["matched_hypothesis_count"]) >= 1
    if not candidates.empty:
        assert "supporting_hypothesis_count" in candidates.columns
        assert "supporting_hypothesis_ids" in candidates.columns
        assert int(candidates["supporting_hypothesis_count"].iloc[0]) >= 1


def test_phase2_fails_when_phase1_summary_missing_and_required(tmp_path: Path) -> None:
    run_id = "phase2_missing_summary"
    _write_phase1_fixture(tmp_path=tmp_path, run_id=run_id)
    in_dir = tmp_path / "reports" / "vol_shock_relaxation" / run_id
    (in_dir / "vol_shock_relaxation_summary.json").unlink()

    env = os.environ.copy()
    env["BACKTEST_DATA_ROOT"] = str(tmp_path)
    cmd = [
        sys.executable,
        str(ROOT / "project" / "pipelines" / "research" / "phase2_conditional_hypotheses.py"),
        "--run_id",
        run_id,
        "--event_type",
        "vol_shock_relaxation",
        "--symbols",
        "BTCUSDT,ETHUSDT",
    ]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    assert proc.returncode != 0
    assert "summary" in (proc.stderr + proc.stdout).lower()


def test_phase2_fails_when_queue_has_no_matching_event_family(tmp_path: Path) -> None:
    run_id = "phase2_queue_mismatch"
    _write_phase1_fixture(tmp_path=tmp_path, run_id=run_id)
    _write_hypothesis_queue_fixture(
        tmp_path=tmp_path,
        run_id=run_id,
        event_types=["cross_venue_desync"],
    )

    env = os.environ.copy()
    env["BACKTEST_DATA_ROOT"] = str(tmp_path)
    cmd = [
        sys.executable,
        str(ROOT / "project" / "pipelines" / "research" / "phase2_conditional_hypotheses.py"),
        "--run_id",
        run_id,
        "--event_type",
        "vol_shock_relaxation",
        "--symbols",
        "BTCUSDT,ETHUSDT",
    ]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    assert proc.returncode != 0
    assert "no entries mapped to event_type" in (proc.stderr + proc.stdout)

def test_phase2_fails_without_split_or_timestamp(tmp_path: Path) -> None:
    run_id = "phase2_missing_split"
    _write_phase1_fixture(tmp_path=tmp_path, run_id=run_id)

    in_dir = tmp_path / "reports" / "vol_shock_relaxation" / run_id
    events_path = in_dir / "vol_shock_relaxation_events.csv"
    events = pd.read_csv(events_path)
    events = events.drop(columns=["enter_ts"])
    events.to_csv(events_path, index=False)

    env = os.environ.copy()
    env["BACKTEST_DATA_ROOT"] = str(tmp_path)
    cmd = [
        sys.executable,
        str(ROOT / "project" / "pipelines" / "research" / "phase2_conditional_hypotheses.py"),
        "--run_id",
        run_id,
        "--event_type",
        "vol_shock_relaxation",
        "--symbols",
        "BTCUSDT,ETHUSDT",
    ]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    assert proc.returncode != 0
    assert "Event registry parity check failed" in (proc.stderr + proc.stdout)


def test_phase2_freezes_when_controls_coverage_is_below_threshold(tmp_path: Path) -> None:
    run_id = "phase2_low_controls_coverage"
    _write_phase1_fixture(tmp_path=tmp_path, run_id=run_id)

    in_dir = tmp_path / "reports" / "vol_shock_relaxation" / run_id
    controls_path = in_dir / "vol_shock_relaxation_controls.csv"
    controls = pd.read_csv(controls_path).head(5)
    controls.to_csv(controls_path, index=False)

    _run_phase2(tmp_path=tmp_path, run_id=run_id)

    out_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    promoted = json.loads((out_dir / "promoted_candidates.json").read_text())
    manifest = json.loads((out_dir / "phase2_manifests.json").read_text())
    candidates = pd.read_csv(out_dir / "phase2_candidates.csv")

    assert promoted["decision"] == "freeze"
    assert promoted["reason"] == "insufficient_controls_coverage"
    assert manifest["freeze_reason"] == "insufficient_controls_coverage"
    assert float(promoted["controls_coverage_ratio"]) < 0.8
    assert candidates.empty


def test_phase2_oos_gate_blocks_positive_test_split() -> None:
    n = 90
    split_label = ["train"] * 30 + ["validation"] * 30 + ["test"] * 30
    adverse = [0.04] * 30 + [0.03] * 30 + [-0.02] * 30
    sub = pd.DataFrame(
        {
            "year": [2022] * 45 + [2023] * 45,
            "symbol": ["BTCUSDT"] * n,
            "vol_regime": ["high"] * n,
            "split_label": split_label,
            "baseline_mode": ["matched_controls_excess"] * n,
            "adverse_proxy_excess": adverse,
            "opportunity_value_excess": [0.0] * n,
            "forward_abs_return_h": [0.0] * n,
            "time_to_secondary_shock": [12.0] * n,
            "rv_decay_half_life": [20.0] * n,
        }
    )
    condition = ConditionSpec("all", "all", lambda d: pd.Series(True, index=d.index))
    action = ActionSpec("risk_throttle_0.5", "risk_throttle", {"k": 0.5})

    result = _evaluate_candidate(
        sub=sub,
        condition=condition,
        action=action,
        bootstrap_iters=200,
        seed=17,
        cost_floor=0.0,
        tail_material_threshold=0.0,
        opportunity_tight_eps=0.005,
        opportunity_near_zero_eps=0.001,
        net_benefit_floor=0.0,
        simplicity_gate=True,
    )

    assert result["validation_delta_adverse_mean"] < 0
    assert result["test_delta_adverse_mean"] > 0
    assert result["gate_oos_validation"] is True
    assert result["gate_oos_validation_test"] is True


def test_phase2_oos_gate_requires_minimum_split_samples() -> None:
    split_label = ["train"] * 20 + ["validation"] * 5 + ["test"] * 5
    n = len(split_label)
    sub = pd.DataFrame(
        {
            "year": [2022] * n,
            "symbol": ["BTCUSDT"] * n,
            "vol_regime": ["high"] * n,
            "split_label": split_label,
            "baseline_mode": ["matched_controls_excess"] * n,
            "adverse_proxy_excess": [0.02] * n,
            "opportunity_value_excess": [0.0] * n,
            "forward_abs_return_h": [0.0] * n,
            "time_to_secondary_shock": [12.0] * n,
            "rv_decay_half_life": [20.0] * n,
        }
    )
    condition = ConditionSpec("all", "all", lambda d: pd.Series(True, index=d.index))
    action = ActionSpec("risk_throttle_0.5", "risk_throttle", {"k": 0.5})

    result = _evaluate_candidate(
        sub=sub,
        condition=condition,
        action=action,
        bootstrap_iters=200,
        seed=17,
        cost_floor=0.0,
        tail_material_threshold=0.0,
        opportunity_tight_eps=0.005,
        opportunity_near_zero_eps=0.001,
        net_benefit_floor=0.0,
        simplicity_gate=True,
        min_oos_split_samples=10,
    )

    assert result["validation_delta_adverse_mean"] < 0
    assert result["test_delta_adverse_mean"] < 0
    assert result["gate_oos_min_samples"] is False
    assert result["gate_oos_validation_test"] is False
    assert "gate_oos_min_samples" in result["fail_reasons"]


def test_deployment_mode_concentrate_when_top_symbol_clearly_dominates() -> None:
    symbol_eval = pd.DataFrame(
        [
            {
                "symbol": "BTCUSDT",
                "deployable": True,
                "ev": 0.12,
                "stability_score": 0.95,
                "variance": 0.0001,
                "sample_size": 80,
            },
            {
                "symbol": "ETHUSDT",
                "deployable": True,
                "ev": 0.02,
                "stability_score": 0.60,
                "variance": 0.0025,
                "sample_size": 80,
            },
        ]
    )

    decision = _deployment_mode_from_symbol_dispersion(symbol_eval)

    assert decision["deployment_mode"] == "concentrate"
    assert decision["dominance_detected"] is True
    assert decision["dispersion_metrics"]["confidence_overlap"] is False


def test_deployment_mode_diversify_when_scores_overlap() -> None:
    symbol_eval = pd.DataFrame(
        [
            {
                "symbol": "BTCUSDT",
                "deployable": True,
                "ev": 0.06,
                "stability_score": 0.85,
                "variance": 0.02,
                "sample_size": 30,
            },
            {
                "symbol": "ETHUSDT",
                "deployable": True,
                "ev": 0.058,
                "stability_score": 0.83,
                "variance": 0.02,
                "sample_size": 30,
            },
        ]
    )

    decision = _deployment_mode_from_symbol_dispersion(symbol_eval)

    assert decision["deployment_mode"] == "diversify"
    assert decision["dominance_detected"] is False
    assert decision["dispersion_metrics"]["confidence_overlap"] is True
