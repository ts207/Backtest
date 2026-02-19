import pytest
import pandas as pd
import json
import yaml
from pathlib import Path
import hashlib

PROJECT_ROOT = Path(__file__).resolve().parents[1]

def test_golden_artifacts_exist():
    """Verify that all golden artifacts are present."""
    golden_dir = PROJECT_ROOT / "golden"
    assert (golden_dir / "e1_report.json").exists()
    assert (golden_dir / "phase2_fdr.parquet").exists()
    assert (golden_dir / "claim_verification_log.parquet").exists()

def test_run_manifest_structure():
    """Verify run manifest contains required audit fields."""
    # Using the certification batch run manifest
    manifest_path = PROJECT_ROOT / "data" / "runs" / "certification_batch" / "run_manifest.json"
    if not manifest_path.exists():
        pytest.skip("Certification batch manifest not found")
        
    with open(manifest_path) as f:
        manifest = json.load(f)
        
    required_keys = {
        "run_id", "git_commit", "config_digest", "data_hash", 
        "spec_hashes", "feature_schema_hash"
    }
    assert required_keys.issubset(manifest.keys())
    assert isinstance(manifest["spec_hashes"], dict)
    assert len(manifest["spec_hashes"]) > 0

def test_phase2_report_spec_binding():
    """Verify Phase 2 report links back to spec hashes."""
    report_path = PROJECT_ROOT / "data" / "reports" / "phase2" / "certification_batch" / "liquidity_vacuum" / "phase2_report.json"
    if not report_path.exists():
        pytest.skip("Phase 2 report not found")
        
    with open(report_path) as f:
        report = json.load(f)
        
    assert "spec_hashes" in report
    assert "thresholds" in report
    
    # Check if thresholds match spec
    spec_path = PROJECT_ROOT / "spec" / "gates.yaml"
    with open(spec_path) as f:
        spec = yaml.safe_load(f)
        
    gate_v1 = spec.get("gate_v1_phase2", {})
    report_thresholds = report.get("thresholds", {})
    
    assert report_thresholds["max_q_value"] == gate_v1["max_q_value"]
    assert report_thresholds["min_after_cost_expectancy_bps"] == gate_v1["min_after_cost_expectancy_bps"]

def test_e1_report_structure():
    """Verify E-1 report structure and pass/fail logic."""
    report_path = PROJECT_ROOT / "golden" / "e1_report.json"
    with open(report_path) as f:
        reports = json.load(f)
        
    assert isinstance(reports, list)
    for r in reports:
        assert "gate_e1_pass" in r
        assert "thresholds" in r
        assert "fail_reasons" in r
