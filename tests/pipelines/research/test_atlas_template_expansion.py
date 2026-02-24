import json
import os
from pathlib import Path
import pandas as pd
import pytest
from pipelines.research.generate_candidate_templates import DEFAULT_RULE_TEMPLATES, DEFAULT_CONDITIONING

def test_new_templates_defined():
    """Verify that the new archetypes are included in the default rule templates."""
    assert "trend_continuation" in DEFAULT_RULE_TEMPLATES
    assert "pullback_entry" in DEFAULT_RULE_TEMPLATES

def test_granular_conditioning_defined():
    """Verify that the new conditioning keys are included in the default configuration."""
    assert "funding_bps" in DEFAULT_CONDITIONING
    assert "vpin" in DEFAULT_CONDITIONING
    assert "regime_vol_liquidity" in DEFAULT_CONDITIONING

def test_lineage_metadata_present(tmp_path):
    """Verify that generated templates include lineage metadata and attribution scores."""
    backlog_path = tmp_path / "backlog.csv"
    pd.DataFrame([{
        "claim_id": "CL_001",
        "concept_id": "C_1",
        "operationalizable": "Y",
        "status": "unverified",
        "candidate_type": "event",
        "statement_summary": '""event_type"": ""VOL_SHOCK""',
        "next_artifact": "spec/events/{event_type}.yaml",
        "priority_score": 10,
        "assets": "*"
    }]).to_csv(backlog_path, index=False)
    
    atlas_dir = tmp_path / "atlas"
    atlas_dir.mkdir()
    
    import sys
    from unittest.mock import patch
    from pipelines.research.generate_candidate_templates import main as templates_main
    
    test_args = [
        "generate_candidate_templates.py",
        "--backlog", str(backlog_path),
        "--atlas_dir", str(atlas_dir)
    ]
    
    with patch.object(sys, "argv", test_args):
        res = templates_main()
        assert res == 0
        
    templates_df = pd.read_parquet(atlas_dir / "candidate_templates.parquet")
    assert "regime_attribution_score" in templates_df.columns
    assert "concept_id" in templates_df.columns
    assert templates_df.iloc[0]["concept_id"] == "C_1"
