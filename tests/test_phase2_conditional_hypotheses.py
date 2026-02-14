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


def _run_phase2(tmp_path: Path, run_id: str, extra_args: list[str] | None = None) -> None:
    extra_args = extra_args or []
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
    assert "oos_pass_count" in manifest

    candidates = pd.read_csv(out_dir / "phase2_candidates.csv")
    if not candidates.empty:
        for col in [
            "train_samples",
            "validation_samples",
            "test_samples",
            "test_p_value",
            "test_p_value_adj_bh",
            "gate_oos_validation_test",
            "gate_multiplicity",
        ]:
            assert col in candidates.columns


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
    assert "split_label" in (proc.stderr + proc.stdout)


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
    assert result["gate_oos_validation_test"] is False
