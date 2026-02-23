import json
import os
from pathlib import Path
import pandas as pd
import pytest
from pipelines.research.generate_candidate_templates import DEFAULT_RULE_TEMPLATES, DEFAULT_CONDITIONING

def test_new_templates_defined():
    """Verify that the new archetypes are included in the default rule templates."""
    assert "trend_continuation" in DEFAULT_RULE_TEMPLATES
    assert "liquidity_reversion_v2" in DEFAULT_RULE_TEMPLATES

def test_granular_conditioning_defined():
    """Verify that the new conditioning keys are included in the default configuration."""
    assert "funding_bps" in DEFAULT_CONDITIONING
    assert "vpin" in DEFAULT_CONDITIONING
    assert "regime_vol_liquidity" in DEFAULT_CONDITIONING

def test_lineage_metadata_present(tmp_path):
    """
    Verify that generated templates include lineage metadata.
    This test will need mock data for the backlog.
    """
    # This is a placeholder for a more complex integration test
    # that would run the actual generator and check the parquet output.
    pass
