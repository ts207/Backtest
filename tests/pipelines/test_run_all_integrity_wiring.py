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
        "funding_extreme_onset": "analyze_funding_episode_events.py",
        "funding_persistence_window": "analyze_funding_episode_events.py",
        "funding_normalization": "analyze_funding_episode_events.py",
        "oi_spike_positive": "analyze_oi_shock_events.py",
        "oi_spike_negative": "analyze_oi_shock_events.py",
        "oi_flush": "analyze_oi_shock_events.py",
    }
    chain_map = {event: script for event, script, _ in run_all.PHASE2_EVENT_CHAIN}
    for event_type, expected_script in expected_scripts.items():
        assert chain_map.get(event_type) == expected_script


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
            "liquidity_vacuum",
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
