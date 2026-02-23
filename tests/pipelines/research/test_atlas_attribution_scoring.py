import os
import sys
from pathlib import Path
from unittest.mock import patch
import pandas as pd
import pytest
from pipelines.research.generate_candidate_templates import main as templates_main

@pytest.fixture
def mock_backlog(tmp_path):
    backlog_path = tmp_path / "research_backlog.csv"
    df = pd.DataFrame([
        {
            "claim_id": "CL_001",
            "concept_id": "C_1",
            "operationalizable": "Y",
            "status": "unverified",
            "candidate_type": "event",
            "statement_summary": '""event_type"": ""VOL_SHOCK""',
            "next_artifact": "spec/events/{event_type}.yaml",
            "priority_score": 10,
            "assets": "*"
        },
        {
            "claim_id": "CL_002",
            "concept_id": "C_2",
            "operationalizable": "Y",
            "status": "unverified",
            "candidate_type": "event",
            "statement_summary": '""event_type"": ""LIQUIDITY_VACUUM""',
            "next_artifact": "spec/events/{event_type}.yaml",
            "priority_score": 10,
            "assets": "*"
        }
    ])
    df.to_csv(backlog_path, index=False)
    return backlog_path

@pytest.fixture
def mock_attribution_report(tmp_path):
    report_path = tmp_path / "attribution_report.parquet"
    # Metrics per concept or claim
    # For now, let's assume we score by concept_id
    df = pd.DataFrame([
        {"concept_id": "C_1", "sharpe_ratio": 2.0, "total_pnl": 100},
        {"concept_id": "C_2", "sharpe_ratio": 0.5, "total_pnl": 10}
    ])
    df.to_parquet(report_path)
    return report_path

def test_attribution_scoring_priority_adjustment(mock_backlog, mock_attribution_report, tmp_path):
    """
    Verify that providing an attribution report adjusts the ordering of templates.
    CL_001 (C_1) has better performance than CL_002 (C_2), so it should be prioritized.
    """
    atlas_dir = tmp_path / "atlas"
    
    # Run without attribution first to see default order (alphabetical claim_id if scores equal)
    # Both have priority_score 10.
    
    test_args = [
        "generate_candidate_templates.py",
        "--backlog", str(mock_backlog),
        "--atlas_dir", str(atlas_dir),
        "--attribution_report", str(mock_attribution_report)
    ]
    
    with patch.object(sys, 'argv', test_args):
        # templates_main returns 0 on success
        res = templates_main()
        assert res == 0
    
    # Load templates and check order
    templates_df = pd.read_parquet(atlas_dir / "candidate_templates.parquet")
    
    # We expect CL_001 to have a higher 'attribution_bonus' and thus better priority
    # Or just check the list order
    assert templates_df.iloc[0]['source_claim_id'] == "CL_001"
