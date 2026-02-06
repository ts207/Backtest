import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


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
        assert candidates["gate_e_simplicity"].all()

    promoted = json.loads((out_dir / "promoted_candidates.json").read_text())
    assert promoted["phase1_pass"] is True
    assert promoted["promoted_count"] <= 2


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
