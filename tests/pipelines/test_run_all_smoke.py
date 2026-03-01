import json
import sys
from pathlib import Path

import pytest
import pipelines.run_all as run_all

def test_run_all_synthetic_e2e_smoke(monkeypatch, tmp_path):
    """
    Synthetic end-to-end smoke test that runs the orchestrator (run_all.py)
    with a complete set of stages enabled. It mocks _run_stage to simulate
    successful completion of all stages, thus verifying stage gating and
    manifest invariants across the full pipeline lifecycle.
    """
    executed_stages = []

    def fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        executed_stages.append(stage)
        # If the stage generates something that later stages need to exist (like a manifest or checklist)
        # we can mock its existence here.
        if stage == "generate_recommendations_checklist":
            checklist_path = tmp_path / "data" / "runs" / run_id / "research_checklist" / "checklist.json"
            checklist_path.parent.mkdir(parents=True, exist_ok=True)
            checklist_path.write_text(json.dumps({"decision": "APPROVE"}), encoding="utf-8")
        elif stage == "phase1_validate_spec":
            pass
        return True

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(run_all, "_git_commit", lambda _project_root: "mock-sha")
    monkeypatch.setattr(run_all, "_run_stage", fake_run_stage)

    # We provide a comprehensive set of arguments to enable all major paths
    test_args = [
        "run_all.py",
        "--run_id", "smoke_test_run",
        "--symbols", "BTCUSDT",
        "--start", "2024-01-01",
        "--end", "2024-01-02",
        "--run_hypothesis_generator", "1",
        "--run_phase2_conditional", "1",
        "--phase2_event_type", "LIQUIDITY_VACUUM",
        "--run_bridge_eval_phase2", "1",
        "--run_strategy_blueprint_compiler", "1",
        "--run_strategy_builder", "1",
        "--run_recommendations_checklist", "1",
        "--run_candidate_promotion", "1",
        "--run_blueprint_promotion", "1",
        "--run_backtest", "1",
        "--run_make_report", "1",
        "--skip_ingest_ohlcv", "1",
        "--skip_ingest_funding", "1",
        "--skip_ingest_spot_ohlcv", "1",
    ]
    monkeypatch.setattr(sys, "argv", test_args)

    rc = run_all.main()
    
    # Assert successful orchestration
    assert rc == 0

    # Verify that the expected stages were called in roughly correct order
    print("\nEXECUTED:", executed_stages)
    assert "generate_candidate_plan" in executed_stages
    assert "build_event_registry" in executed_stages
    assert "phase2_conditional_hypotheses" in executed_stages
    assert "bridge_evaluate_phase2" in executed_stages
    assert "compile_strategy_blueprints" in executed_stages
    assert "generate_recommendations_checklist" in executed_stages

    # Verify manifest invariants at the end of the run
    manifest_path = tmp_path / "data" / "runs" / "smoke_test_run" / "run_manifest.json"
    assert manifest_path.exists(), "Manifest should be fully written"
    
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
        
    assert manifest["status"] == "success"
    assert "started_at" in manifest
    assert "ended_at" in manifest
    assert "planned_stage_instances" in manifest
    assert "stage_instance_timings_sec" in manifest
    assert manifest["run_id"] == "smoke_test_run"
