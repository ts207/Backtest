import json
import os
from pathlib import Path
import pandas as pd
import pytest
from pipelines.research.verify_atlas_claims import _LOOKUP_MAP, main as verify_main

def test_registry_presence_lookup_defined():
    """Verify that the new registry presence lookup is defined in the lookup map."""
    assert "T_EVENT_REGISTRY_PRESENCE" in _LOOKUP_MAP

def test_registry_presence_lookup_logic(tmp_path, monkeypatch):
    """
    Verify that the registry presence lookup correctly identifies existing event types.
    """
    # Patch DATA_ROOT in the module
    import pipelines.research.verify_atlas_claims as vac
    monkeypatch.setattr(vac, "DATA_ROOT", tmp_path)
    
    run_id = "test_run"
    event_type = "LIQUIDATION_CASCADE"
    
    # Create mock registry output
    events_dir = tmp_path / "events" / run_id
    events_dir.mkdir(parents=True)
    
    df = pd.DataFrame([
        {
            "run_id": run_id,
            "event_type": event_type,
            "signal_column": "liquidation_cascade_event",
            "timestamp": "2026-01-01 00:00:00",
            "symbol": "BTCUSDT",
            "event_id": "LC_001",
            "features_at_event": "{}"
        }
    ])
    df.to_parquet(events_dir / "events.parquet")
    
    # The lookup function we'll implement
    lookup_fn = vac._LOOKUP_MAP.get("T_EVENT_REGISTRY_PRESENCE")
    if lookup_fn:
        metric, threshold, passed = lookup_fn(run_id, event_type)
        assert passed is True
        assert metric == 1.0
    else:
        pytest.fail("T_EVENT_REGISTRY_PRESENCE not in _LOOKUP_MAP")

def test_registry_presence_lookup_logic_missing(tmp_path, monkeypatch):
    """
    Verify that the registry presence lookup correctly identifies missing event types.
    """
    import pipelines.research.verify_atlas_claims as vac
    monkeypatch.setattr(vac, "DATA_ROOT", tmp_path)
    run_id = "test_run"
    
    events_dir = tmp_path / "events" / run_id
    events_dir.mkdir(parents=True)
    
    # Empty registry
    pd.DataFrame(columns=["event_type"]).to_parquet(events_dir / "events.parquet")
    
    lookup_fn = vac._LOOKUP_MAP.get("T_EVENT_REGISTRY_PRESENCE")
    if lookup_fn:
        metric, threshold, passed = lookup_fn(run_id, "NON_EXISTENT_EVENT")
        assert passed is False
        assert metric == 0.0
