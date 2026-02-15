import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import export_edge_candidates


def test_collects_unified_multi_event_phase2_candidates(tmp_path: Path, monkeypatch) -> None:
    run_id = "edge_universe_run"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(export_edge_candidates, "DATA_ROOT", tmp_path)

    phase2_root = tmp_path / "reports" / "phase2" / run_id
    vsr_dir = phase2_root / "vol_shock_relaxation"
    liq_dir = phase2_root / "liquidity_absence_window"
    vsr_dir.mkdir(parents=True, exist_ok=True)
    liq_dir.mkdir(parents=True, exist_ok=True)

    (vsr_dir / "promoted_candidates.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "event_type": "vol_shock_relaxation",
                "decision": "promote",
                "phase1_pass": True,
                "candidates": [
                    {
                        "condition": "all",
                        "action": "risk_throttle_0.5",
                        "sample_size": 42,
                        "delta_adverse_mean": -0.05,
                        "delta_opportunity_mean": 0.02,
                        "expectancy_per_trade": 0.02,
                        "robustness_score": 0.9,
                        "event_frequency": 0.3,
                        "capacity_proxy": 1.2,
                        "profit_density_score": 0.0054,
                        "gate_a_ci_separated": True,
                        "gate_b_time_stable": True,
                        "gate_c_regime_stable": True,
                        "gate_d_friction_floor": True,
                        "gate_e_simplicity": True,
                        "gate_f_exposure_guard": True,
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    pd.DataFrame(
        [
            {
                "candidate_id": "all__risk_throttle_0.5",
                "event_type": "vol_shock_relaxation",
                "condition": "all",
                "action": "risk_throttle_0.5",
                "symbol": "BTCUSDT",
                "sample_size": 42,
                "ev": 0.03,
                "variance": 0.0004,
                "sharpe_like": 1.5,
                "stability_score": 0.8,
                "window_consistency": 1.0,
                "sign_persistence": 1.0,
                "drawdown_profile": -0.01,
                "capacity_proxy": 1.8,
                "deployable": True,
            }
        ]
    ).to_csv(vsr_dir / "phase2_symbol_evaluation.csv", index=False)

    pd.DataFrame(
        [
            {
                "condition": "session_us",
                "action": "delay_8",
                "sample_size": 30,
                "delta_adverse_mean": -0.03,
                "delta_opportunity_mean": -0.01,
                "expectancy_per_trade": 0.0,
                "robustness_score": 0.8,
                "event_frequency": 0.2,
                "profit_density_score": 0.0,
                "gate_all": True,
                "gate_a_ci_separated": True,
                "gate_b_time_stable": True,
                "gate_c_regime_stable": True,
                "gate_d_friction_floor": True,
                "gate_e_simplicity": True,
                "gate_f_exposure_guard": True,
            }
        ]
    ).to_csv(liq_dir / "phase2_candidates.csv", index=False)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "export_edge_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--execute",
            "0",
        ],
    )
    assert export_edge_candidates.main() == 0

    out_csv = tmp_path / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.csv"
    out_df = pd.read_csv(out_csv)
    assert not out_df.empty
    assert set(out_df["event"].tolist()) >= {
        "vol_shock_relaxation",
        "liquidity_absence_window",
    }

    promoted = out_df[out_df["event"] == "vol_shock_relaxation"].iloc[0]
    assert promoted["status"] == "PROMOTED"
    assert promoted["n_events"] == 42
    assert promoted["edge_score"] > 0.0
    assert promoted["profit_density_score"] > 0.0
    assert promoted["expectancy_per_trade"] == 0.03
    assert promoted["variance"] == 0.0004
    assert promoted["capacity_proxy"] == 1.8
    assert promoted["candidate_symbol"] == "BTCUSDT"

    liq_row = out_df[out_df["event"] == "liquidity_absence_window"].iloc[0]
    assert liq_row["n_events"] == 30
    assert liq_row["status"] == "DRAFT"
    assert liq_row["candidate_symbol"] == "ALL"


def test_run_research_chain_executes_phase1_and_phase2_for_all_events(monkeypatch) -> None:
    captured_cmds: list[list[str]] = []

    def _fake_run(cmd: list[str]) -> SimpleNamespace:
        captured_cmds.append(list(cmd))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(export_edge_candidates.subprocess, "run", _fake_run)

    export_edge_candidates._run_research_chain(
        run_id="r_chain",
        symbols="BTCUSDT,ETHUSDT",
        run_hypothesis_generator=True,
        hypothesis_datasets="auto",
        hypothesis_max_fused=24,
    )

    assert captured_cmds
    assert captured_cmds[0][1].endswith("generate_hypothesis_queue.py")

    for event_type, script_name, _ in export_edge_candidates.PHASE2_EVENT_CHAIN:
        analyzer_matches = [c for c in captured_cmds if c[1].endswith(script_name)]
        assert analyzer_matches, f"missing analyzer call for {event_type}"
        phase2_matches = [
            c
            for c in captured_cmds
            if c[1].endswith("phase2_conditional_hypotheses.py") and "--event_type" in c and event_type in c
        ]
        assert phase2_matches, f"missing phase2 call for {event_type}"

    phase2_calls = [c for c in captured_cmds if c[1].endswith("phase2_conditional_hypotheses.py")]
    assert len(phase2_calls) == len(export_edge_candidates.PHASE2_EVENT_CHAIN)


def test_collect_phase2_candidates_infers_symbol_from_condition(tmp_path: Path, monkeypatch) -> None:
    run_id = "edge_symbol_scope"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(export_edge_candidates, "DATA_ROOT", tmp_path)

    event_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    event_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([
        {
            "condition": "symbol_BTCUSDT",
            "action": "delay_8",
            "sample_size": 10,
            "delta_adverse_mean": -0.01,
            "delta_opportunity_mean": 0.01,
            "gate_all": True,
        }
    ]).to_csv(event_dir / "phase2_candidates.csv", index=False)

    rows = export_edge_candidates._collect_phase2_candidates(run_id, run_symbols=["BTCUSDT", "ETHUSDT"])
    assert rows and rows[0]["candidate_symbol"] == "BTCUSDT"


def test_collect_phase2_candidates_adds_symbol_scores_and_rollout_gate(tmp_path: Path, monkeypatch) -> None:
    run_id = "edge_rollout_gate"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(export_edge_candidates, "DATA_ROOT", tmp_path)

    event_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    event_dir.mkdir(parents=True, exist_ok=True)
    (event_dir / "promoted_candidates.json").write_text(
        json.dumps({"run_id": run_id, "event_type": "vol_shock_relaxation", "candidates": [{"candidate_id": "c0"}]}),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "candidate_id": "c0",
                "symbol": "BTCUSDT",
                "ev": 0.03,
                "variance": 0.1,
                "sharpe_like": 1.0,
                "stability_score": 0.8,
                "capacity_proxy": 1.0,
                "deployable": True,
            },
            {
                "candidate_id": "c0",
                "symbol": "ETHUSDT",
                "ev": 0.005,
                "variance": 0.1,
                "sharpe_like": 1.0,
                "stability_score": 0.8,
                "capacity_proxy": 1.0,
                "deployable": True,
            },
        ]
    ).to_csv(event_dir / "phase2_symbol_evaluation.csv", index=False)

    rows = export_edge_candidates._collect_phase2_candidates(run_id, run_symbols=["BTCUSDT", "ETHUSDT"])
    assert rows
    row = rows[0]
    assert row["candidate_symbol"] == "BTCUSDT"
    assert row["rollout_eligible"] is False
    symbol_scores = json.loads(row["symbol_scores"])
    assert set(symbol_scores.keys()) == {"BTCUSDT", "ETHUSDT"}
