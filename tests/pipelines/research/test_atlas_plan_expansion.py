import json
import os
import sys
from pathlib import Path
from unittest.mock import patch
import pandas as pd
import pytest
from pipelines.research.generate_candidate_plan import main as planner_main

@pytest.fixture
def mock_atlas_env(tmp_path):
    atlas_dir = tmp_path / "atlas"
    atlas_dir.mkdir()
    
    # Mock template with new conditioning
    template = {
        "template_id": "CL_001@VOL_SHOCK",
        "source_claim_id": "CL_001",
        "concept_id": "C_1",
        "object_type": "event",
        "runtime_event_type": "VOL_SHOCK",
        "event_type": "VOL_SHOCK",
        "canonical_event_type": "VOL_SHOCK",
        "canonical_family": "VOLATILITY_TRANSITION",
        "ontology_in_taxonomy": True,
        "ontology_in_canonical_registry": True,
        "ontology_unknown_templates": [],
        "ontology_source_states": [],
        "ontology_family_states": [],
        "ontology_all_states": [],
        "ontology_spec_hash": "sha256:test",
        "target_spec_path": "spec/events/VOL_SHOCK.yaml",
        "rule_templates": ["mean_reversion"],
        "horizons": ["5m"],
        "conditioning": {
            "vol_regime": ["high"],
            "funding_bps": ["extreme_pos"]
        },
        "assets_filter": "*",
        "min_events": 50
    }
    pd.DataFrame([template]).to_parquet(atlas_dir / "candidate_templates.parquet")
    
    # Create required spec file — must be valid YAML so FeasibilityGuard passes it
    spec_dir = tmp_path / "spec" / "events"
    spec_dir.mkdir(parents=True)
    (spec_dir / "VOL_SHOCK.yaml").write_text(
        "event_type: VOL_SHOCK\n"
        "reports_dir: vol_shock_relaxation\n"
        "events_file: vol_shock_relaxation_events.csv\n"
        "signal_column: vol_shock_relaxation_event\n"
    )
    
    # Mock lake structure
    lake_dir = tmp_path / "data" / "lake" / "cleaned" / "perp" / "BTCUSDT" / "bars_5m"
    lake_dir.mkdir(parents=True)
    pd.DataFrame({"timestamp": [pd.Timestamp.now()]}).to_parquet(lake_dir / "bars.parquet")
    
    return tmp_path

def test_plan_supports_new_conditioning_keys(mock_atlas_env):
    """Verify that the planner processes the new conditioning keys and checks market context."""
    run_id = "test_run"
    out_dir = mock_atlas_env / "reports"
    
    # Case 1: Market context missing - should block
    test_args = [
        "generate_candidate_plan.py",
        "--run_id", run_id,
        "--symbols", "BTCUSDT",
        "--atlas_dir", str(mock_atlas_env / "atlas"),
        "--out_dir", str(out_dir)
    ]
    
    with patch.dict(os.environ, {"BACKTEST_DATA_ROOT": str(mock_atlas_env / "data")}):
        with patch.object(sys, "argv", test_args):
            import pipelines.research.generate_candidate_plan as gcp
            # PROJECT_ROOT.parent must be mock_atlas_env
            mock_project_root = mock_atlas_env / "project"
            with patch.object(gcp, "PROJECT_ROOT", mock_project_root):
                with patch.object(gcp, "DATA_ROOT", mock_atlas_env / "data"):
                    with patch.object(gcp, "ontology_spec_hash", return_value="sha256:test"):
                        res = planner_main()
            assert res == 0
    
    report_path = out_dir / "plan_feasibility_report.parquet"
    if report_path.exists():
        report = pd.read_parquet(report_path)
        # Should have blocked due to missing market context for vol_regime and funding_bps
        blocks = report[report["status"] == "blocked_missing_dataset"]
        assert len(blocks) > 0
        assert any("market_context missing" in r for r in blocks["reason"])
    else:
        pytest.fail("Feasibility report missing")

def test_composite_regime_feasibility_check(mock_atlas_env):
    """Verify that the planner accepts candidates when market context is present."""
    # Add market context
    ctx_dir = mock_atlas_env / "data" / "lake" / "context" / "market_state" / "BTCUSDT"
    ctx_dir.mkdir(parents=True)
    pd.DataFrame({"timestamp": [pd.Timestamp.now()]}).to_parquet(ctx_dir / "5m.parquet")
    
    run_id = "test_run_success"
    out_dir = mock_atlas_env / "reports_success"
    
    test_args = [
        "generate_candidate_plan.py",
        "--run_id", run_id,
        "--symbols", "BTCUSDT",
        "--atlas_dir", str(mock_atlas_env / "atlas"),
        "--out_dir", str(out_dir)
    ]
    
    with patch.dict(os.environ, {"BACKTEST_DATA_ROOT": str(mock_atlas_env / "data")}):
        with patch.object(sys, "argv", test_args):
            import pipelines.research.generate_candidate_plan as gcp
            # PROJECT_ROOT.parent must be mock_atlas_env
            mock_project_root = mock_atlas_env / "project"
            with patch.object(gcp, "PROJECT_ROOT", mock_project_root):
                with patch.object(gcp, "DATA_ROOT", mock_atlas_env / "data"):
                    with patch.object(gcp, "ontology_spec_hash", return_value="sha256:test"):
                        res = planner_main()
            assert res == 0
            
    # Check plan file
    plan_path = out_dir / "candidate_plan.jsonl"
    with open(plan_path, "r") as f:
        lines = f.readlines()
        assert len(lines) > 0
        plan_data = [json.loads(l) for l in lines]
        # Should have found conditioning variants
        cond_keys = []
        for p in plan_data:
            if p["conditioning"]:
                cond_keys.extend(p["conditioning"].keys())
        assert "vol_regime" in cond_keys
        assert "funding_bps" in cond_keys
        assert all(str(p.get("candidate_id", "")).strip() for p in plan_data)
        assert all(str(p.get("hypothesis_id", "")).strip() for p in plan_data)
        assert all(str(p.get("template_id", "")).strip() for p in plan_data)
        assert all("condition_signature" in p for p in plan_data)
