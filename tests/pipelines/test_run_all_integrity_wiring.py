from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.run_all as run_all


def _arg_value(args: list[str], flag: str) -> str:
    idx = args.index(flag)
    return str(args[idx + 1])


def test_run_all_ohlcv_ingest_respects_force_flag(monkeypatch, tmp_path):
    captured: list[tuple[str, list[str]]] = []

    def fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, list(base_args)))
        return False

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(run_all, "_git_commit", lambda _project_root: "test-sha")
    monkeypatch.setattr(run_all, "_run_stage", fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--force",
            "0",
        ],
    )

    rc = run_all.main()
    assert rc == 1
    assert captured
    stage, args = captured[0]
    assert stage == "ingest_binance_um_ohlcv_5m"
    assert _arg_value(args, "--force") == "0"


def test_run_all_bridge_stage_default_embargo_is_nonzero(monkeypatch, tmp_path):
    captured: list[tuple[str, list[str]]] = []

    def fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, list(base_args)))
        return True

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(run_all, "_git_commit", lambda _project_root: "test-sha")
    monkeypatch.setattr(run_all, "_run_stage", fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--skip_ingest_ohlcv",
            "1",
            "--skip_ingest_funding",
            "1",
            "--skip_ingest_spot_ohlcv",
            "1",
            "--run_hypothesis_generator",
            "0",
            "--run_phase2_conditional",
            "1",
            "--phase2_event_type",
            "all",
            "--run_bridge_eval_phase2",
            "1",
            "--run_strategy_blueprint_compiler",
            "0",
            "--run_strategy_builder",
            "0",
            "--run_recommendations_checklist",
            "0",
        ],
    )

    rc = run_all.main()
    assert rc == 0
    bridge = [row for row in captured if row[0].startswith("bridge_evaluate_phase2")]
    assert bridge
    _, bridge_args = bridge[0]
    assert _arg_value(bridge_args, "--embargo_days") == "1"


def test_run_all_progress_logs_elapsed_and_eta(monkeypatch, tmp_path, capsys):
    def fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        return False

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(run_all, "_git_commit", lambda _project_root: "test-sha")
    monkeypatch.setattr(run_all, "_run_stage", fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
        ],
    )

    rc = run_all.main()
    assert rc == 1
    captured = capsys.readouterr()
    assert "pipeline_elapsed=" in captured.out
    assert "eta~" in captured.out


def test_run_all_declared_subtype_families_are_not_noop():
    expected_scripts = {
        "FUNDING_EXTREME_ONSET": "analyze_funding_episode_events.py",
        "FUNDING_PERSISTENCE_TRIGGER": "analyze_funding_episode_events.py",
        "FUNDING_NORMALIZATION_TRIGGER": "analyze_funding_episode_events.py",
        "OI_SPIKE_POSITIVE": "analyze_oi_shock_events.py",
        "OI_SPIKE_NEGATIVE": "analyze_oi_shock_events.py",
        "OI_FLUSH": "analyze_oi_shock_events.py",
        "DEPTH_COLLAPSE": "analyze_liquidity_dislocation_events.py",
        "SPREAD_BLOWOUT": "analyze_liquidity_dislocation_events.py",
        "ORDERFLOW_IMBALANCE_SHOCK": "analyze_liquidity_dislocation_events.py",
        "SWEEP_STOPRUN": "analyze_liquidity_dislocation_events.py",
        "ABSORPTION_EVENT": "analyze_liquidity_dislocation_events.py",
        "LIQUIDITY_GAP_PRINT": "analyze_liquidity_dislocation_events.py",
        "VOL_SPIKE": "analyze_volatility_transition_events.py",
        "VOL_RELAXATION_START": "analyze_volatility_transition_events.py",
        "VOL_CLUSTER_SHIFT": "analyze_volatility_transition_events.py",
        "RANGE_COMPRESSION_END": "analyze_volatility_transition_events.py",
        "BREAKOUT_TRIGGER": "analyze_volatility_transition_events.py",
        "FUNDING_FLIP": "analyze_positioning_extremes_events.py",
        "DELEVERAGING_WAVE": "analyze_positioning_extremes_events.py",
        "TREND_EXHAUSTION_TRIGGER": "analyze_forced_flow_and_exhaustion_events.py",
        "MOMENTUM_DIVERGENCE_TRIGGER": "analyze_forced_flow_and_exhaustion_events.py",
        "CLIMAX_VOLUME_BAR": "analyze_forced_flow_and_exhaustion_events.py",
        "FAILED_CONTINUATION": "analyze_forced_flow_and_exhaustion_events.py",
        "RANGE_BREAKOUT": "analyze_trend_structure_events.py",
        "FALSE_BREAKOUT": "analyze_trend_structure_events.py",
        "TREND_ACCELERATION": "analyze_trend_structure_events.py",
        "TREND_DECELERATION": "analyze_trend_structure_events.py",
        "PULLBACK_PIVOT": "analyze_trend_structure_events.py",
        "SUPPORT_RESISTANCE_BREAK": "analyze_trend_structure_events.py",
        "ZSCORE_STRETCH": "analyze_statistical_dislocation_events.py",
        "BAND_BREAK": "analyze_statistical_dislocation_events.py",
        "OVERSHOOT_AFTER_SHOCK": "analyze_statistical_dislocation_events.py",
        "GAP_OVERSHOOT": "analyze_statistical_dislocation_events.py",
        "VOL_REGIME_SHIFT_EVENT": "analyze_regime_transition_events.py",
        "TREND_TO_CHOP_SHIFT": "analyze_regime_transition_events.py",
        "CHOP_TO_TREND_SHIFT": "analyze_regime_transition_events.py",
        "CORRELATION_BREAKDOWN_EVENT": "analyze_regime_transition_events.py",
        "BETA_SPIKE_EVENT": "analyze_regime_transition_events.py",
        "INDEX_COMPONENT_DIVERGENCE": "analyze_information_desync_events.py",
        "SPOT_PERP_BASIS_SHOCK": "analyze_information_desync_events.py",
        "LEAD_LAG_BREAK": "analyze_information_desync_events.py",
        "SESSION_OPEN_EVENT": "analyze_temporal_structure_events.py",
        "SESSION_CLOSE_EVENT": "analyze_temporal_structure_events.py",
        "FUNDING_TIMESTAMP_EVENT": "analyze_temporal_structure_events.py",
        "SCHEDULED_NEWS_WINDOW_EVENT": "analyze_temporal_structure_events.py",
        "SPREAD_REGIME_WIDENING_EVENT": "analyze_execution_friction_events.py",
        "SLIPPAGE_SPIKE_EVENT": "analyze_execution_friction_events.py",
        "FEE_REGIME_CHANGE_EVENT": "analyze_execution_friction_events.py",
    }
    chain_map = {event: script for event, script, _ in run_all.PHASE2_EVENT_CHAIN}
    for event_type, expected_script in expected_scripts.items():
        assert chain_map.get(event_type) == expected_script


def test_run_all_target_families_are_not_routed_to_canonical_analyzer():
    target_reports_dirs = {
        "liquidity_dislocation",
        "volatility_transition",
        "positioning_extremes",
        "forced_flow_and_exhaustion",
        "trend_structure",
        "statistical_dislocation",
        "regime_transition",
        "information_desync",
        "temporal_structure",
        "execution_friction",
    }
    for event_type, script_name, _ in run_all.PHASE2_EVENT_CHAIN:
        spec = run_all.EVENT_REGISTRY_SPECS[event_type]
        if str(spec.reports_dir).strip().lower() in target_reports_dirs:
            assert script_name != "analyze_canonical_events.py", f"event_type={event_type} still routes to canonical"


def test_run_all_phase2_chain_has_registry_and_scripts():
    assert run_all._validate_phase2_event_chain() == []


def test_run_all_phase2_chain_validation_reports_missing_entries(monkeypatch):
    monkeypatch.setattr(
        run_all,
        "PHASE2_EVENT_CHAIN",
        [("unknown_event_type", "missing_script.py", [])],
    )
    issues = run_all._validate_phase2_event_chain()
    assert any("Missing event spec/registry entry" in issue for issue in issues)
    assert any("Missing phase2 analyzer script" in issue for issue in issues)


def test_run_all_research_gate_profile_wiring(monkeypatch, tmp_path):
    captured: list[tuple[str, list[str]]] = []

    def fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, list(base_args)))
        return True

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(run_all, "_git_commit", lambda _project_root: "test-sha")
    monkeypatch.setattr(run_all, "_run_stage", fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--run_expectancy_robustness",
            "1",
            "--run_recommendations_checklist",
            "1",
            "--run_strategy_blueprint_compiler",
            "0",
            "--run_strategy_builder",
            "0",
            "--run_phase2_conditional",
            "0",
            "--run_hypothesis_generator",
            "0",
            "--mode",
            "research",
        ],
    )

    rc = run_all.main()
    assert rc == 0
    stage_map = {stage: args for stage, args in captured}
    robust_args = stage_map["validate_expectancy_traps"]
    checklist_args = stage_map["generate_recommendations_checklist"]
    assert _arg_value(robust_args, "--gate_profile") == "discovery"
    assert _arg_value(checklist_args, "--gate_profile") == "discovery"


def test_run_all_passes_mode_to_phase2_and_bridge(monkeypatch, tmp_path):
    captured: list[tuple[str, list[str]]] = []

    def fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, list(base_args)))
        return True

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(run_all, "_git_commit", lambda _project_root: "test-sha")
    monkeypatch.setattr(run_all, "_run_stage", fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--run_phase2_conditional",
            "1",
            "--phase2_event_type",
            "LIQUIDITY_VACUUM",
            "--run_bridge_eval_phase2",
            "1",
            "--run_strategy_blueprint_compiler",
            "0",
            "--run_strategy_builder",
            "0",
            "--run_recommendations_checklist",
            "0",
            "--run_hypothesis_generator",
            "0",
            "--skip_ingest_ohlcv",
            "1",
            "--skip_ingest_funding",
            "1",
            "--skip_ingest_spot_ohlcv",
            "1",
            "--mode",
            "research",
        ],
    )

    rc = run_all.main()
    assert rc == 0
    stage_map = {stage: args for stage, args in captured}
    phase2_args = stage_map["phase2_conditional_hypotheses"]
    bridge_args = stage_map["bridge_evaluate_phase2"]
    assert _arg_value(phase2_args, "--mode") == "research"
    assert _arg_value(bridge_args, "--mode") == "research"


def test_run_all_wires_candidate_promotion_into_compiler(monkeypatch, tmp_path):
    captured: list[tuple[str, list[str]]] = []

    def fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, list(base_args)))
        return True

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(run_all, "_git_commit", lambda _project_root: "test-sha")
    monkeypatch.setattr(run_all, "_run_stage", fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            "r_promote",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--run_phase2_conditional",
            "1",
            "--phase2_event_type",
            "LIQUIDITY_VACUUM",
            "--run_bridge_eval_phase2",
            "0",
            "--run_hypothesis_generator",
            "0",
            "--run_recommendations_checklist",
            "0",
            "--run_strategy_builder",
            "0",
            "--run_candidate_promotion",
            "1",
            "--skip_ingest_ohlcv",
            "1",
            "--skip_ingest_funding",
            "1",
            "--skip_ingest_spot_ohlcv",
            "1",
        ],
    )

    rc = run_all.main()
    assert rc == 0

    stage_names = [stage for stage, _ in captured]
    assert "promote_candidates" in stage_names
    assert "update_edge_registry" in stage_names
    assert "compile_strategy_blueprints" in stage_names
    assert stage_names.index("promote_candidates") < stage_names.index("compile_strategy_blueprints")
    assert stage_names.index("promote_candidates") < stage_names.index("update_edge_registry")
    assert stage_names.index("update_edge_registry") < stage_names.index("compile_strategy_blueprints")

    stage_map = {stage: args for stage, args in captured}
    compiler_args = stage_map["compile_strategy_blueprints"]
    expected_path = str(tmp_path / "data" / "reports" / "promotions" / "r_promote" / "promoted_candidates.parquet")
    assert _arg_value(compiler_args, "--candidates_file") == expected_path


def test_run_all_passes_phase2_cost_calibration_overrides(monkeypatch, tmp_path):
    captured: list[tuple[str, list[str]]] = []

    def fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, list(base_args)))
        return True

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(run_all, "_git_commit", lambda _project_root: "test-sha")
    monkeypatch.setattr(run_all, "_run_stage", fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--run_phase2_conditional",
            "1",
            "--phase2_event_type",
            "LIQUIDITY_VACUUM",
            "--run_bridge_eval_phase2",
            "0",
            "--run_strategy_blueprint_compiler",
            "0",
            "--run_strategy_builder",
            "0",
            "--run_recommendations_checklist",
            "0",
            "--run_hypothesis_generator",
            "0",
            "--skip_ingest_ohlcv",
            "1",
            "--skip_ingest_funding",
            "1",
            "--skip_ingest_spot_ohlcv",
            "1",
            "--phase2_cost_calibration_mode",
            "tob_regime",
            "--phase2_cost_min_tob_coverage",
            "0.75",
            "--phase2_cost_tob_tolerance_minutes",
            "15",
        ],
    )

    rc = run_all.main()
    assert rc == 0
    stage_map = {stage: args for stage, args in captured}
    phase2_args = stage_map["phase2_conditional_hypotheses"]
    assert _arg_value(phase2_args, "--cost_calibration_mode") == "tob_regime"
    assert _arg_value(phase2_args, "--cost_min_tob_coverage") == "0.75"
    assert _arg_value(phase2_args, "--cost_tob_tolerance_minutes") == "15"


def test_run_all_always_wires_candidate_plan_when_hypothesis_generator_enabled(monkeypatch, tmp_path):
    captured: list[tuple[str, list[str]]] = []
    run_id = "r_plan_wiring"

    def fake_run_stage(stage: str, script_path: Path, base_args: list[str], _run_id: str) -> bool:
        captured.append((stage, list(base_args)))
        if stage == "generate_candidate_plan":
            plan_path = (
                tmp_path
                / "data"
                / "reports"
                / "hypothesis_generator"
                / run_id
                / "candidate_plan.jsonl"
            )
            plan_path.parent.mkdir(parents=True, exist_ok=True)
            plan_path.write_text(json.dumps({"event_type": "LIQUIDITY_VACUUM"}) + "\n", encoding="utf-8")
        return True

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(run_all, "_git_commit", lambda _project_root: "test-sha")
    monkeypatch.setattr(run_all, "_run_stage", fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--skip_ingest_ohlcv",
            "1",
            "--skip_ingest_funding",
            "1",
            "--skip_ingest_spot_ohlcv",
            "1",
            "--run_hypothesis_generator",
            "1",
            "--run_phase2_conditional",
            "1",
            "--phase2_event_type",
            "LIQUIDITY_VACUUM",
            "--run_bridge_eval_phase2",
            "0",
            "--run_strategy_blueprint_compiler",
            "0",
            "--run_strategy_builder",
            "0",
            "--run_recommendations_checklist",
            "0",
        ],
    )

    rc = run_all.main()
    assert rc == 0

    stage_map = {stage: args for stage, args in captured}
    phase2_args = stage_map["phase2_conditional_hypotheses"]
    expected_plan_path = str(
        tmp_path / "data" / "reports" / "hypothesis_generator" / run_id / "candidate_plan.jsonl"
    )
    assert _arg_value(phase2_args, "--candidate_plan") == expected_plan_path


def test_run_all_production_gate_profile_wiring(monkeypatch, tmp_path):
    captured: list[tuple[str, list[str]]] = []

    def fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, list(base_args)))
        return True

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(run_all, "_git_commit", lambda _project_root: "test-sha")
    monkeypatch.setattr(run_all, "_run_stage", fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--run_expectancy_robustness",
            "1",
            "--run_recommendations_checklist",
            "1",
            "--run_strategy_blueprint_compiler",
            "0",
            "--run_strategy_builder",
            "0",
            "--run_phase2_conditional",
            "0",
            "--run_hypothesis_generator",
            "0",
            "--mode",
            "production",
        ],
    )

    rc = run_all.main()
    assert rc == 0
    stage_map = {stage: args for stage, args in captured}
    robust_args = stage_map["validate_expectancy_traps"]
    checklist_args = stage_map["generate_recommendations_checklist"]
    assert _arg_value(robust_args, "--gate_profile") == "promotion"
    assert _arg_value(checklist_args, "--gate_profile") == "promotion"


def test_run_all_unconditionally_blocks_strategy_blueprint_fallback(monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--strategy_blueprint_allow_fallback",
            "1",
        ],
    )

    rc = run_all.main()
    captured = capsys.readouterr()
    assert rc == 1
    assert "INV_NO_FALLBACK_IN_MEASUREMENT" in captured.err


def test_run_all_blocks_override_flags_in_production_mode(monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--mode",
            "production",
            "--strategy_blueprint_allow_naive_entry_fail",
            "1",
        ],
    )

    rc = run_all.main()
    captured = capsys.readouterr()
    assert rc == 1
    assert "strictly forbidden in production mode" in captured.err


def test_run_all_keep_research_blocks_execution_stages(monkeypatch, tmp_path, capsys):
    manifests: list[dict] = []

    def fake_write_run_manifest(_run_id: str, payload: dict) -> None:
        manifests.append(copy.deepcopy(payload))

    def fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        if stage == "generate_recommendations_checklist":
            checklist_path = tmp_path / "data" / "runs" / run_id / "research_checklist" / "checklist.json"
            checklist_path.parent.mkdir(parents=True, exist_ok=True)
            checklist_path.write_text(json.dumps({"decision": "KEEP_RESEARCH"}), encoding="utf-8")
        return True

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(run_all, "_git_commit", lambda _project_root: "test-sha")
    monkeypatch.setattr(run_all, "_run_stage", fake_run_stage)
    monkeypatch.setattr(run_all, "_write_run_manifest", fake_write_run_manifest)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--run_backtest",
            "1",
            "--auto_continue_on_keep_research",
            "0",
        ],
    )

    rc = run_all.main()
    captured = capsys.readouterr()

    assert rc == 1
    assert "Checklist gate blocked execution because decision=KEEP_RESEARCH" in captured.err
    assert manifests
    final_manifest = manifests[-1]
    assert final_manifest["status"] == "failed"
    assert final_manifest["failed_stage"] == "checklist_gate"
    assert final_manifest["execution_blocked_by_checklist"] is True


def test_run_all_keep_research_auto_continue_guard_is_fail_closed(monkeypatch, tmp_path, capsys):
    manifests: list[dict] = []

    def fake_write_run_manifest(_run_id: str, payload: dict) -> None:
        manifests.append(copy.deepcopy(payload))

    def fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        if stage == "generate_recommendations_checklist":
            checklist_path = tmp_path / "data" / "runs" / run_id / "research_checklist" / "checklist.json"
            checklist_path.parent.mkdir(parents=True, exist_ok=True)
            checklist_path.write_text(json.dumps({"decision": "KEEP_RESEARCH"}), encoding="utf-8")
        return True

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(run_all, "_git_commit", lambda _project_root: "test-sha")
    monkeypatch.setattr(run_all, "_run_stage", fake_run_stage)
    monkeypatch.setattr(run_all, "_write_run_manifest", fake_write_run_manifest)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--run_backtest",
            "1",
            "--auto_continue_on_keep_research",
            "1",
        ],
    )

    rc = run_all.main()
    captured = capsys.readouterr()

    assert rc == 1
    assert "INV_NO_FALLBACK_IN_MEASUREMENT" in captured.err
    assert manifests
    final_manifest = manifests[-1]
    assert final_manifest["status"] == "failed"
    assert final_manifest["failed_stage"] == "checklist_gate"
    assert "evaluation_guard_violation" in final_manifest


def test_run_all_skips_event_stages_when_hypothesis_queue_empty(monkeypatch, tmp_path):
    captured_stages: list[str] = []
    manifests: list[dict] = []

    def fake_write_run_manifest(_run_id: str, payload: dict) -> None:
        manifests.append(copy.deepcopy(payload))

    def fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured_stages.append(stage)
        return True

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(run_all, "_git_commit", lambda _project_root: "test-sha")
    monkeypatch.setattr(run_all, "_run_stage", fake_run_stage)
    monkeypatch.setattr(run_all, "_write_run_manifest", fake_write_run_manifest)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--skip_ingest_ohlcv",
            "1",
            "--skip_ingest_funding",
            "1",
            "--skip_ingest_spot_ohlcv",
            "1",
            "--run_hypothesis_generator",
            "1",
            "--run_phase2_conditional",
            "1",
            "--phase2_event_type",
            "all",
            "--run_bridge_eval_phase2",
            "1",
            "--run_strategy_blueprint_compiler",
            "0",
            "--run_strategy_builder",
            "0",
            "--run_recommendations_checklist",
            "0",
            "--run_blueprint_promotion",
            "0",
            "--run_make_report",
            "0",
        ],
    )

    rc = run_all.main()
    assert rc == 0

    skipped_prefixes = (
        "phase2_conditional_hypotheses_",
        "bridge_evaluate_phase2_",
        "build_event_registry_",
    )
    assert not any(stage.startswith(skipped_prefixes) for stage in captured_stages)
    assert manifests
    final_manifest = manifests[-1]
    timings = final_manifest.get("stage_timings_sec", {})
    skipped_timing_keys = [k for k in timings.keys() if k.startswith(skipped_prefixes)]
    assert skipped_timing_keys
    assert all(float(timings[k]) == 0.0 for k in skipped_timing_keys)


def test_run_all_records_stage_instance_timings_for_single_event(monkeypatch, tmp_path):
    manifests: list[dict] = []

    def fake_write_run_manifest(_run_id: str, payload: dict) -> None:
        manifests.append(copy.deepcopy(payload))

    def fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        return True

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(run_all, "_git_commit", lambda _project_root: "test-sha")
    monkeypatch.setattr(run_all, "_run_stage", fake_run_stage)
    monkeypatch.setattr(run_all, "_write_run_manifest", fake_write_run_manifest)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--skip_ingest_ohlcv",
            "1",
            "--skip_ingest_funding",
            "1",
            "--skip_ingest_spot_ohlcv",
            "1",
            "--run_hypothesis_generator",
            "0",
            "--run_phase2_conditional",
            "1",
            "--phase2_event_type",
            "LIQUIDITY_VACUUM",
            "--run_bridge_eval_phase2",
            "1",
            "--run_strategy_blueprint_compiler",
            "0",
            "--run_strategy_builder",
            "0",
            "--run_recommendations_checklist",
            "0",
            "--run_backtest",
            "0",
            "--run_make_report",
            "0",
        ],
    )

    rc = run_all.main()
    assert rc == 0
    assert manifests
    final_manifest = manifests[-1]

    planned_instances = final_manifest.get("planned_stage_instances", [])
    assert "build_event_registry_LIQUIDITY_VACUUM" in planned_instances
    assert "phase2_conditional_hypotheses_LIQUIDITY_VACUUM" in planned_instances
    assert "bridge_evaluate_phase2_LIQUIDITY_VACUUM" in planned_instances

    instance_timings = final_manifest.get("stage_instance_timings_sec", {})
    assert "build_event_registry_LIQUIDITY_VACUUM" in instance_timings
    assert "phase2_conditional_hypotheses_LIQUIDITY_VACUUM" in instance_timings
    assert "bridge_evaluate_phase2_LIQUIDITY_VACUUM" in instance_timings


def test_run_all_terminal_manifest_guard_blocks_subsequent_stages(monkeypatch, tmp_path, capsys):
    call_count = 0

    def fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            manifest_path = tmp_path / "data" / "runs" / run_id / "run_manifest.json"
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            payload["status"] = "success"
            payload["finished_at"] = "2024-01-01T00:00:00+00:00"
            payload["ended_at"] = "2024-01-01T00:00:00+00:00"
            manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return True

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(run_all, "_git_commit", lambda _project_root: "test-sha")
    monkeypatch.setattr(run_all, "_run_stage", fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--skip_ingest_ohlcv",
            "1",
            "--skip_ingest_funding",
            "1",
            "--skip_ingest_spot_ohlcv",
            "1",
            "--run_phase2_conditional",
            "0",
            "--run_hypothesis_generator",
            "0",
            "--run_strategy_blueprint_compiler",
            "0",
            "--run_strategy_builder",
            "0",
            "--run_recommendations_checklist",
            "0",
            "--run_backtest",
            "0",
            "--run_make_report",
            "0",
        ],
    )

    rc = run_all.main()
    captured = capsys.readouterr()
    assert rc == 1
    assert call_count == 1
    assert "Terminal run manifest detected" in captured.err
