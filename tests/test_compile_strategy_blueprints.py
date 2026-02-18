from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import compile_strategy_blueprints

ALLOW_NAIVE_ENTRY_FAIL_ARGS = ["--allow_naive_entry_fail", "1", "--strict_cost_fields", "0"]


def _write_inputs(tmp_path: Path, run_id: str) -> None:
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    edge_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "all__delay_8",
                "status": "PROMOTED",
                "candidate_symbol": "BTCUSDT",
                "edge_score": 0.4,
                "expectancy_per_trade": 0.02,
                "robustness_score": 0.9,
                "event_frequency": 0.3,
                "capacity_proxy": 1.0,
                "profit_density_score": 0.0054,
                "n_events": 120,
                "source_path": str(tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation" / "phase2_candidates.csv"),
            },
            {
                "run_id": run_id,
                "event": "liquidity_absence_window",
                "candidate_id": "all__no_action",
                "status": "PROMOTED",
                "candidate_symbol": "ALL",
                "edge_score": 0.1,
                "expectancy_per_trade": 0.005,
                "robustness_score": 0.6,
                "event_frequency": 0.2,
                "capacity_proxy": 0.2,
                "profit_density_score": 0.0006,
                "n_events": 120,
                "source_path": str(tmp_path / "reports" / "phase2" / run_id / "liquidity_absence_window" / "phase2_candidates.csv"),
            },
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)

    phase2_vsr = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    phase2_vsr.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "all__delay_8",
                "condition": "all",
                "action": "delay_8",
                "sample_size": 120,
                "gate_oos_validation_test": True,
                "gate_multiplicity": True,
                "gate_c_regime_stable": True,
                "gate_all": True,
                "delta_adverse_mean": -0.02,
                "delta_opportunity_mean": 0.03,
                "robustness_score": 0.9,
                "profit_density_score": 0.0054,
            }
        ]
    ).to_csv(phase2_vsr / "phase2_candidates.csv", index=False)

    phase2_law = tmp_path / "reports" / "phase2" / run_id / "liquidity_absence_window"
    phase2_law.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "all__no_action",
                "condition": "session_eu",
                "action": "no_action",
                "sample_size": 120,
                "gate_oos_validation_test": False,
                "gate_multiplicity": False,
                "gate_c_regime_stable": True,
                "gate_all": False,
                "delta_adverse_mean": -0.004,
                "delta_opportunity_mean": 0.005,
                "robustness_score": 0.6,
                "profit_density_score": 0.0006,
            }
        ]
    ).to_csv(phase2_law / "phase2_candidates.csv", index=False)

    report_vsr = tmp_path / "reports" / "vol_shock_relaxation" / run_id
    report_vsr.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "rv_decay_half_life": [12, 18, 24],
            "range_pct_96": [0.01, 0.02, 0.03],
            "forward_abs_return_h": [0.02, 0.03, 0.05],
        }
    ).to_csv(report_vsr / "vol_shock_relaxation_events.csv", index=False)


def test_compiler_emits_per_event_blueprints_with_required_fields(monkeypatch, tmp_path: Path) -> None:
    run_id = "dsl_compile_fields"
    _write_inputs(tmp_path=tmp_path, run_id=run_id)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compile_strategy_blueprints.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--max_per_event",
            "2",
            "--ignore_checklist",
            "1",
        ]
        + ALLOW_NAIVE_ENTRY_FAIL_ARGS,
    )
    assert compile_strategy_blueprints.main() == 0

    out_path = tmp_path / "reports" / "strategy_blueprints" / run_id / "blueprints.jsonl"
    assert out_path.exists()
    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert rows
    events = {row["event_type"] for row in rows}
    assert "vol_shock_relaxation" in events
    assert "liquidity_absence_window" in events
    for row in rows:
        assert row["id"]
        assert row["entry"]["triggers"]
        assert "time_stop_bars" in row["exit"]
        assert "trailing_stop_type" in row["exit"]
        assert "break_even_r" in row["exit"]
        assert "condition_logic" in row["entry"]
        assert "condition_nodes" in row["entry"]
        assert "min_trades" in row["evaluation"]
        assert row["entry"]["conditions"] and all(isinstance(cond, str) for cond in row["entry"]["conditions"])
    liquidity_absence_row = next(row for row in rows if row["event_type"] == "liquidity_absence_window")
    assert liquidity_absence_row["entry"]["conditions"] == ["session_eu"]
    assert liquidity_absence_row["entry"]["condition_nodes"]
    assert liquidity_absence_row["entry"]["condition_nodes"][0]["feature"] == "session_hour_utc"
    assert liquidity_absence_row["entry"]["condition_nodes"][0]["operator"] == "in_range"


def test_compiler_is_deterministic_under_rerun(monkeypatch, tmp_path: Path) -> None:
    run_id = "dsl_compile_deterministic"
    _write_inputs(tmp_path=tmp_path, run_id=run_id)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)

    argv = [
        "compile_strategy_blueprints.py",
        "--run_id",
        run_id,
        "--symbols",
        "BTCUSDT,ETHUSDT",
        "--ignore_checklist",
        "1",
    ] + ALLOW_NAIVE_ENTRY_FAIL_ARGS

    monkeypatch.setattr(sys, "argv", argv)
    assert compile_strategy_blueprints.main() == 0
    out_path = tmp_path / "reports" / "strategy_blueprints" / run_id / "blueprints.jsonl"
    first = out_path.read_bytes()

    monkeypatch.setattr(sys, "argv", argv)
    assert compile_strategy_blueprints.main() == 0
    second = out_path.read_bytes()

    assert first == second


def test_compiler_quality_floor_prefers_high_quality_promoted(monkeypatch, tmp_path: Path) -> None:
    run_id = "dsl_compile_quality_floor"
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    edge_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "all__delay_8",
                "status": "PROMOTED",
                "candidate_symbol": "BTCUSDT",
                "edge_score": 0.2,
                "expectancy_per_trade": 0.02,
                "robustness_score": 0.80,
                "event_frequency": 0.3,
                "capacity_proxy": 0.5,
                "profit_density_score": 0.02,
                "n_events": 120,
                "source_path": "x",
            },
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "all__delay_30",
                "status": "PROMOTED",
                "candidate_symbol": "BTCUSDT",
                "edge_score": 0.3,
                "expectancy_per_trade": 0.03,
                "robustness_score": 0.40,
                "event_frequency": 0.3,
                "capacity_proxy": 0.5,
                "profit_density_score": 0.03,
                "n_events": 200,
                "source_path": "x",
            },
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)

    phase2_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    phase2_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"candidate_id": "all__delay_8", "condition": "all", "action": "delay_8", "sample_size": 120},
            {"candidate_id": "all__delay_30", "condition": "all", "action": "delay_30", "sample_size": 200},
        ]
    ).to_csv(phase2_dir / "phase2_candidates.csv", index=False)

    report_vsr = tmp_path / "reports" / "vol_shock_relaxation" / run_id
    report_vsr.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"rv_decay_half_life": [12], "range_pct_96": [0.01], "forward_abs_return_h": [0.02]}).to_csv(
        report_vsr / "vol_shock_relaxation_events.csv", index=False
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compile_strategy_blueprints.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--max_per_event",
            "2",
            "--ignore_checklist",
            "1",
        ]
        + ALLOW_NAIVE_ENTRY_FAIL_ARGS,
    )
    assert compile_strategy_blueprints.main() == 0

    out_path = tmp_path / "reports" / "strategy_blueprints" / run_id / "blueprints.jsonl"
    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 1
    assert rows[0]["candidate_id"] == "all__delay_8"


def test_compiler_trims_zero_trade_blueprint_from_walkforward(monkeypatch, tmp_path: Path) -> None:
    run_id = "dsl_compile_trim_wf"
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    edge_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "all__delay_8",
                "status": "PROMOTED",
                "candidate_symbol": "BTCUSDT",
                "edge_score": 0.2,
                "expectancy_per_trade": 0.02,
                "robustness_score": 0.80,
                "event_frequency": 0.3,
                "capacity_proxy": 0.5,
                "profit_density_score": 0.02,
                "n_events": 120,
                "source_path": "x",
            },
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "all__delay_30",
                "status": "PROMOTED",
                "candidate_symbol": "BTCUSDT",
                "edge_score": 0.19,
                "expectancy_per_trade": 0.02,
                "robustness_score": 0.79,
                "event_frequency": 0.3,
                "capacity_proxy": 0.5,
                "profit_density_score": 0.019,
                "n_events": 120,
                "source_path": "x",
            },
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)

    phase2_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    phase2_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"candidate_id": "all__delay_8", "condition": "all", "action": "delay_8", "sample_size": 120},
            {"candidate_id": "all__delay_30", "condition": "all", "action": "delay_30", "sample_size": 120},
        ]
    ).to_csv(phase2_dir / "phase2_candidates.csv", index=False)

    report_vsr = tmp_path / "reports" / "vol_shock_relaxation" / run_id
    report_vsr.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"rv_decay_half_life": [12], "range_pct_96": [0.01], "forward_abs_return_h": [0.02]}).to_csv(
        report_vsr / "vol_shock_relaxation_events.csv", index=False
    )

    eval_dir = tmp_path / "reports" / "eval" / run_id
    eval_dir.mkdir(parents=True, exist_ok=True)
    wf = {
        "per_strategy_split_metrics": {
            "dsl_interpreter_v1__bp_dsl_compile_trim_wf_vol_shock_relaxation_all__delay_8_single_symbol": {
                "validation": {"total_trades": 0, "stressed_net_pnl": 0.0}
            }
        }
    }
    (eval_dir / "walkforward_summary.json").write_text(json.dumps(wf), encoding="utf-8")

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compile_strategy_blueprints.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--max_per_event",
            "2",
            "--ignore_checklist",
            "1",
        ]
        + ALLOW_NAIVE_ENTRY_FAIL_ARGS,
    )
    assert compile_strategy_blueprints.main() == 0

    out_path = tmp_path / "reports" / "strategy_blueprints" / run_id / "blueprints.jsonl"
    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    ids = {row["candidate_id"] for row in rows}
    assert "all__delay_8" not in ids
    assert "all__delay_30" in ids


def test_compiler_rejects_negative_after_cost_expectancy_under_strict_defaults(monkeypatch, tmp_path: Path) -> None:
    run_id = "dsl_compile_strict_after_cost"
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    edge_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "c_bad_cost",
                "status": "PROMOTED",
                "candidate_symbol": "BTCUSDT",
                "expectancy_per_trade": 0.02,
                "after_cost_expectancy_per_trade": -0.001,
                "stressed_after_cost_expectancy_per_trade": -0.002,
                "cost_ratio": 0.8,
                "robustness_score": 0.9,
                "event_frequency": 0.3,
                "capacity_proxy": 1.0,
                "profit_density_score": 0.01,
                "n_events": 120,
                "source_path": "x",
            }
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)

    phase2_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    phase2_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "c_bad_cost",
                "condition": "all",
                "action": "delay_8",
                "sample_size": 120,
                "after_cost_expectancy_per_trade": -0.001,
                "stressed_after_cost_expectancy_per_trade": -0.002,
                "cost_ratio": 0.8,
            }
        ]
    ).to_csv(phase2_dir / "phase2_candidates.csv", index=False)

    report_dir = tmp_path / "reports" / "vol_shock_relaxation" / run_id
    report_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"rv_decay_half_life": [12], "range_pct_96": [0.01], "forward_abs_return_h": [0.02]}).to_csv(
        report_dir / "vol_shock_relaxation_events.csv", index=False
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compile_strategy_blueprints.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--ignore_checklist",
            "1",
            "--allow_naive_entry_fail",
            "1",
        ],
    )
    assert compile_strategy_blueprints.main() == 1


def test_compiler_walkforward_trim_uses_validation_split_only(monkeypatch, tmp_path: Path) -> None:
    run_id = "dsl_compile_validation_trim_only"
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    edge_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "c1",
                "status": "PROMOTED",
                "candidate_symbol": "BTCUSDT",
                "expectancy_per_trade": 0.03,
                "after_cost_expectancy_per_trade": 0.02,
                "stressed_after_cost_expectancy_per_trade": 0.01,
                "cost_ratio": 0.2,
                "robustness_score": 0.9,
                "event_frequency": 0.3,
                "capacity_proxy": 0.8,
                "profit_density_score": 0.01,
                "n_events": 150,
                "source_path": "x",
            },
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "c2",
                "status": "PROMOTED",
                "candidate_symbol": "BTCUSDT",
                "expectancy_per_trade": 0.03,
                "after_cost_expectancy_per_trade": 0.02,
                "stressed_after_cost_expectancy_per_trade": 0.01,
                "cost_ratio": 0.2,
                "robustness_score": 0.9,
                "event_frequency": 0.3,
                "capacity_proxy": 0.8,
                "profit_density_score": 0.01,
                "n_events": 150,
                "source_path": "x",
            },
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)

    phase2_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    phase2_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"candidate_id": "c1", "condition": "all", "action": "delay_8", "sample_size": 150},
            {"candidate_id": "c2", "condition": "all", "action": "delay_12", "sample_size": 150},
        ]
    ).to_csv(phase2_dir / "phase2_candidates.csv", index=False)

    report_dir = tmp_path / "reports" / "vol_shock_relaxation" / run_id
    report_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"rv_decay_half_life": [12], "range_pct_96": [0.01], "forward_abs_return_h": [0.02]}).to_csv(
        report_dir / "vol_shock_relaxation_events.csv", index=False
    )

    eval_dir = tmp_path / "reports" / "eval" / run_id
    eval_dir.mkdir(parents=True, exist_ok=True)
    wf = {
        "per_strategy_split_metrics": {
            "dsl_interpreter_v1__bp_dsl_compile_validation_trim_only_vol_shock_relaxation_c1_single_symbol": {
                "validation": {"total_trades": 10, "stressed_net_pnl": -1.0},
                "test": {"total_trades": 10, "stressed_net_pnl": 5.0},
            },
            "dsl_interpreter_v1__bp_dsl_compile_validation_trim_only_vol_shock_relaxation_c2_single_symbol": {
                "validation": {"total_trades": 10, "stressed_net_pnl": 1.0},
                "test": {"total_trades": 10, "stressed_net_pnl": -5.0},
            },
        }
    }
    (eval_dir / "walkforward_summary.json").write_text(json.dumps(wf), encoding="utf-8")

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compile_strategy_blueprints.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--max_per_event",
            "2",
            "--ignore_checklist",
            "1",
            "--allow_naive_entry_fail",
            "1",
        ],
    )
    assert compile_strategy_blueprints.main() == 0
    out_path = tmp_path / "reports" / "strategy_blueprints" / run_id / "blueprints.jsonl"
    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    ids = {row["candidate_id"] for row in rows}
    assert "c1" not in ids
    assert "c2" in ids


def test_compile_blueprint_lineage_source_path_is_run_scoped() -> None:
    run_id = "run_scoped_lineage"
    rows, _ = compile_strategy_blueprints._choose_event_rows(
        run_id=run_id,
        event_type="vol_shock_relaxation",
        edge_rows=[
            {
                "candidate_id": "c1",
                "status": "PROMOTED",
                "expectancy_per_trade": 0.02,
                "robustness_score": 0.9,
                "n_events": 120,
            }
        ],
        phase2_df=pd.DataFrame(),
        max_per_event=1,
        allow_fallback_blueprints=False,
        strict_cost_fields=False,
    )
    assert rows
    expected_suffix = f"/phase2/{run_id}/vol_shock_relaxation/phase2_candidates.csv"
    assert str(rows[0]["source_path"]).endswith(expected_suffix)


def test_compiler_applies_action_semantics_to_overlays() -> None:
    stats = {
        "half_life": np.array([12.0], dtype=float),
        "adverse": np.array([0.01], dtype=float),
        "favorable": np.array([0.02], dtype=float),
    }
    bp_skip = compile_strategy_blueprints._build_blueprint(
        run_id="run_action_overlay",
        run_symbols=["BTCUSDT"],
        event_type="range_compression_breakout_window",
        row={
            "candidate_id": "c_skip",
            "condition": "all",
            "action": "entry_gate_skip",
            "sample_size": 200,
            "n_events": 200,
            "robustness_score": 0.9,
            "source_path": "x",
        },
        phase2_lookup={},
        stats=stats,
        fees_bps=4.0,
        slippage_bps=2.0,
    )
    bp_throttle = compile_strategy_blueprints._build_blueprint(
        run_id="run_action_overlay",
        run_symbols=["BTCUSDT"],
        event_type="range_compression_breakout_window",
        row={
            "candidate_id": "c_throttle",
            "condition": "all",
            "action": "risk_throttle_0.5",
            "sample_size": 200,
            "n_events": 200,
            "robustness_score": 0.9,
            "source_path": "x",
        },
        phase2_lookup={},
        stats=stats,
        fees_bps=4.0,
        slippage_bps=2.0,
    )

    skip_overlay = next((ov for ov in bp_skip.overlays if ov.name == "risk_throttle"), None)
    throttle_overlay = next((ov for ov in bp_throttle.overlays if ov.name == "risk_throttle"), None)
    assert skip_overlay is not None
    assert throttle_overlay is not None
    assert float(skip_overlay.params.get("size_scale", 1.0)) == 0.0
    assert float(throttle_overlay.params.get("size_scale", 1.0)) == 0.5


def test_compiler_drops_behavior_duplicates_and_keeps_best_ranked() -> None:
    stats = {
        "half_life": np.array([12.0], dtype=float),
        "adverse": np.array([0.01], dtype=float),
        "favorable": np.array([0.02], dtype=float),
    }
    bp_a = compile_strategy_blueprints._build_blueprint(
        run_id="run_dedupe",
        run_symbols=["BTCUSDT"],
        event_type="range_compression_breakout_window",
        row={
            "candidate_id": "all__entry_gate_skip",
            "condition": "all",
            "action": "entry_gate_skip",
            "sample_size": 200,
            "n_events": 200,
            "robustness_score": 0.9,
            "source_path": "x",
        },
        phase2_lookup={},
        stats=stats,
        fees_bps=4.0,
        slippage_bps=2.0,
    )
    bp_b = compile_strategy_blueprints._build_blueprint(
        run_id="run_dedupe",
        run_symbols=["BTCUSDT"],
        event_type="range_compression_breakout_window",
        row={
            "candidate_id": "all__risk_throttle_0",
            "condition": "all",
            "action": "risk_throttle_0",
            "sample_size": 200,
            "n_events": 200,
            "robustness_score": 0.9,
            "source_path": "x",
        },
        phase2_lookup={},
        stats=stats,
        fees_bps=4.0,
        slippage_bps=2.0,
    )

    deduped, diag = compile_strategy_blueprints._dedupe_blueprints_by_behavior([bp_a, bp_b])
    assert len(deduped) == 1
    assert deduped[0].id == bp_a.id
    assert int(diag["behavior_duplicate_count"]) == 1
    assert bp_b.id in diag["behavior_duplicate_dropped_ids"]
    keep_map = dict(diag["behavior_duplicate_keep_map"])
    assert bp_a.id in keep_map
    assert bp_b.id in keep_map[bp_a.id]


def test_compiler_summary_records_duplicate_drop_diagnostics(monkeypatch, tmp_path: Path) -> None:
    run_id = "dsl_compile_duplicate_summary"
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    edge_dir.mkdir(parents=True, exist_ok=True)
    phase2_dir = tmp_path / "reports" / "phase2" / run_id / "range_compression_breakout_window"
    phase2_dir.mkdir(parents=True, exist_ok=True)
    report_dir = tmp_path / "reports" / "range_compression_breakout_window" / run_id
    report_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "event": "range_compression_breakout_window",
                "candidate_id": "all__entry_gate_skip",
                "status": "PROMOTED",
                "candidate_symbol": "BTCUSDT",
                "expectancy_per_trade": 0.03,
                "robustness_score": 0.9,
                "event_frequency": 0.3,
                "capacity_proxy": 0.8,
                "profit_density_score": 0.03,
                "n_events": 200,
                "source_path": "x",
            },
                {
                    "run_id": run_id,
                    "event": "range_compression_breakout_window",
                    "candidate_id": "all__risk_throttle_0",
                    "status": "PROMOTED",
                    "candidate_symbol": "BTCUSDT",
                    "expectancy_per_trade": 0.02,
                    "robustness_score": 0.9,
                    "event_frequency": 0.3,
                    "capacity_proxy": 0.8,
                    "profit_density_score": 0.02,
                    "n_events": 200,
                    "source_path": "x",
            },
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)

    pd.DataFrame(
        [
            {"candidate_id": "all__entry_gate_skip", "condition": "all", "action": "entry_gate_skip", "sample_size": 200},
            {"candidate_id": "all__risk_throttle_0", "condition": "all", "action": "risk_throttle_0", "sample_size": 200},
        ]
    ).to_csv(phase2_dir / "phase2_candidates.csv", index=False)

    pd.DataFrame({"rv_decay_half_life": [12], "range_pct_96": [0.01], "forward_abs_return_h": [0.02]}).to_csv(
        report_dir / "range_compression_breakout_window_events.csv", index=False
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compile_strategy_blueprints.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--max_per_event",
            "2",
            "--ignore_checklist",
            "1",
        ]
        + ALLOW_NAIVE_ENTRY_FAIL_ARGS,
    )
    assert compile_strategy_blueprints.main() == 0

    summary_path = tmp_path / "reports" / "strategy_blueprints" / run_id / "blueprints_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert int(summary["behavior_duplicate_count"]) == 1
    assert len(summary["behavior_duplicate_dropped_ids"]) == 1
    assert summary["behavior_duplicate_keep_map"]


def test_compiler_requires_promote_checklist_by_default(monkeypatch, tmp_path: Path) -> None:
    run_id = "dsl_compile_checklist_gate"
    _write_inputs(tmp_path=tmp_path, run_id=run_id)
    checklist_dir = tmp_path / "runs" / run_id / "research_checklist"
    checklist_dir.mkdir(parents=True, exist_ok=True)
    (checklist_dir / "checklist.json").write_text(json.dumps({"run_id": run_id, "decision": "KEEP_RESEARCH"}), encoding="utf-8")

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compile_strategy_blueprints.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--max_per_event",
            "2",
        ]
        + ALLOW_NAIVE_ENTRY_FAIL_ARGS,
    )
    assert compile_strategy_blueprints.main() == 1


def test_compiler_fails_on_non_executable_symbolic_condition_by_default(monkeypatch, tmp_path: Path) -> None:
    run_id = "dsl_compile_non_executable_condition"
    _write_inputs(tmp_path=tmp_path, run_id=run_id)
    phase2_law = tmp_path / "reports" / "phase2" / run_id / "liquidity_absence_window" / "phase2_candidates.csv"
    phase2_df = pd.read_csv(phase2_law)
    phase2_df["condition"] = ["age_bucket_0_8"]
    phase2_df.to_csv(phase2_law, index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compile_strategy_blueprints.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--ignore_checklist",
            "1",
        ]
        + ALLOW_NAIVE_ENTRY_FAIL_ARGS,
    )
    assert compile_strategy_blueprints.main() == 1


def test_compiler_skips_non_executable_when_other_selected_candidate_is_valid(monkeypatch, tmp_path: Path) -> None:
    run_id = "dsl_compile_partial_non_executable"
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    edge_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "near_half_life__delay_30",
                "status": "PROMOTED",
                "candidate_symbol": "BTCUSDT",
                "edge_score": 0.5,
                "expectancy_per_trade": 0.03,
                "robustness_score": 0.95,
                "event_frequency": 0.3,
                "capacity_proxy": 1.0,
                "profit_density_score": 0.006,
                "n_events": 120,
                "source_path": "x",
            },
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "all__delay_8",
                "status": "PROMOTED",
                "candidate_symbol": "BTCUSDT",
                "edge_score": 0.4,
                "expectancy_per_trade": 0.02,
                "robustness_score": 0.9,
                "event_frequency": 0.3,
                "capacity_proxy": 1.0,
                "profit_density_score": 0.0054,
                "n_events": 120,
                "source_path": "x",
            },
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)

    phase2_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    phase2_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "near_half_life__delay_30",
                "condition": "near_half_life",
                "action": "delay_30",
                "sample_size": 120,
                "gate_oos_validation_test": True,
                "gate_multiplicity": True,
                "gate_c_regime_stable": True,
                "gate_all": True,
                "delta_adverse_mean": -0.02,
                "delta_opportunity_mean": 0.03,
                "robustness_score": 0.95,
                "profit_density_score": 0.006,
            },
            {
                "candidate_id": "all__delay_8",
                "condition": "all",
                "action": "delay_8",
                "sample_size": 120,
                "gate_oos_validation_test": True,
                "gate_multiplicity": True,
                "gate_c_regime_stable": True,
                "gate_all": True,
                "delta_adverse_mean": -0.02,
                "delta_opportunity_mean": 0.03,
                "robustness_score": 0.9,
                "profit_density_score": 0.0054,
            },
        ]
    ).to_csv(phase2_dir / "phase2_candidates.csv", index=False)

    report_dir = tmp_path / "reports" / "vol_shock_relaxation" / run_id
    report_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "rv_decay_half_life": [12, 18, 24],
            "range_pct_96": [0.01, 0.02, 0.03],
            "forward_abs_return_h": [0.02, 0.03, 0.05],
        }
    ).to_csv(report_dir / "vol_shock_relaxation_events.csv", index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compile_strategy_blueprints.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--ignore_checklist",
            "1",
            "--max_per_event",
            "2",
        ]
        + ALLOW_NAIVE_ENTRY_FAIL_ARGS,
    )
    assert compile_strategy_blueprints.main() == 0
    out_path = tmp_path / "reports" / "strategy_blueprints" / run_id / "blueprints.jsonl"
    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    ids = {row["candidate_id"] for row in rows}
    assert "all__delay_8" in ids
    assert "near_half_life__delay_30" not in ids


def test_compiler_does_not_emit_weak_promoted_fallback(monkeypatch, tmp_path: Path) -> None:
    run_id = "dsl_compile_no_weak_promoted_fallback"
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    edge_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "weak_promoted",
                "status": "PROMOTED",
                "candidate_symbol": "BTCUSDT",
                "edge_score": 0.1,
                "expectancy_per_trade": -0.01,
                "robustness_score": 0.3,
                "event_frequency": 0.1,
                "capacity_proxy": 0.2,
                "profit_density_score": 0.0,
                "n_events": 10,
                "source_path": "x",
            }
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)

    phase2_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    phase2_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [{"candidate_id": "weak_promoted", "condition": "all", "action": "delay_8", "sample_size": 10}]
    ).to_csv(phase2_dir / "phase2_candidates.csv", index=False)

    report_vsr = tmp_path / "reports" / "vol_shock_relaxation" / run_id
    report_vsr.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"rv_decay_half_life": [12], "range_pct_96": [0.01], "forward_abs_return_h": [0.02]}).to_csv(
        report_vsr / "vol_shock_relaxation_events.csv", index=False
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compile_strategy_blueprints.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--ignore_checklist",
            "1",
        ]
        + ALLOW_NAIVE_ENTRY_FAIL_ARGS,
    )
    assert compile_strategy_blueprints.main() == 1
    out_path = tmp_path / "reports" / "strategy_blueprints" / run_id / "blueprints.jsonl"
    assert not out_path.exists()


def test_compiler_walkforward_trim_all_fails_closed(monkeypatch, tmp_path: Path) -> None:
    run_id = "dsl_compile_trim_all"
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    edge_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "c1",
                "status": "PROMOTED",
                "candidate_symbol": "BTCUSDT",
                "edge_score": 0.3,
                "expectancy_per_trade": 0.03,
                "robustness_score": 0.9,
                "event_frequency": 0.5,
                "capacity_proxy": 1.0,
                "profit_density_score": 0.02,
                "n_events": 120,
                "source_path": "x",
            },
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "c2",
                "status": "PROMOTED",
                "candidate_symbol": "BTCUSDT",
                "edge_score": 0.25,
                "expectancy_per_trade": 0.02,
                "robustness_score": 0.8,
                "event_frequency": 0.4,
                "capacity_proxy": 0.8,
                "profit_density_score": 0.015,
                "n_events": 100,
                "source_path": "x",
            },
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)

    phase2_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    phase2_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"candidate_id": "c1", "condition": "all", "action": "delay_8", "sample_size": 120},
            {"candidate_id": "c2", "condition": "all", "action": "delay_12", "sample_size": 120},
        ]
    ).to_csv(phase2_dir / "phase2_candidates.csv", index=False)

    report_vsr = tmp_path / "reports" / "vol_shock_relaxation" / run_id
    report_vsr.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"rv_decay_half_life": [12], "range_pct_96": [0.01], "forward_abs_return_h": [0.02]}).to_csv(
        report_vsr / "vol_shock_relaxation_events.csv", index=False
    )

    eval_dir = tmp_path / "reports" / "eval" / run_id
    eval_dir.mkdir(parents=True, exist_ok=True)
    wf = {
        "per_strategy_split_metrics": {
            "dsl_interpreter_v1__bp_dsl_compile_trim_all_vol_shock_relaxation_c1_single_symbol": {
                "validation": {"total_trades": 0, "stressed_net_pnl": 0.0}
            },
            "dsl_interpreter_v1__bp_dsl_compile_trim_all_vol_shock_relaxation_c2_single_symbol": {
                "validation": {"total_trades": 0, "stressed_net_pnl": 0.0}
            },
        }
    }
    (eval_dir / "walkforward_summary.json").write_text(json.dumps(wf), encoding="utf-8")

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compile_strategy_blueprints.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--max_per_event",
            "2",
            "--ignore_checklist",
            "1",
        ]
        + ALLOW_NAIVE_ENTRY_FAIL_ARGS,
    )
    assert compile_strategy_blueprints.main() == 1


def test_compiler_requires_naive_entry_validation_by_default(monkeypatch, tmp_path: Path) -> None:
    run_id = "dsl_compile_requires_naive_validation"
    _write_inputs(tmp_path=tmp_path, run_id=run_id)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compile_strategy_blueprints.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--ignore_checklist",
            "1",
        ],
    )
    assert compile_strategy_blueprints.main() == 1
    manifest_path = tmp_path / "runs" / run_id / "compile_strategy_blueprints.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert "Missing naive-entry validation artifact" in str(payload.get("error", ""))


def test_compiler_allows_naive_entry_fail_override(monkeypatch, tmp_path: Path) -> None:
    run_id = "dsl_compile_naive_override"
    _write_inputs(tmp_path=tmp_path, run_id=run_id)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compile_strategy_blueprints.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--ignore_checklist",
            "1",
        ]
        + ALLOW_NAIVE_ENTRY_FAIL_ARGS,
    )
    assert compile_strategy_blueprints.main() == 0
