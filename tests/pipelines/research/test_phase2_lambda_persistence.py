"""Round-trip test for lambda state persistence.

Verifies that _build_lambda_snapshot → save_lambda_state_json → reload JSON
produces a consistent representation of the lambda state.
"""
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research.phase2_lambda_persistence import (
    _build_lambda_snapshot,
    save_lambda_state_json,
)


def _make_fdr_df() -> pd.DataFrame:
    """Minimal FDR DataFrame with lambda columns for all 3 levels."""
    return pd.DataFrame(
        {
            "template_verb": ["mean_reversion", "continuation"],
            "horizon": ["5m", "15m"],
            "canonical_family": ["LIQUIDITY_DISLOCATION", "TREND_STRUCTURE"],
            "canonical_event_type": ["EXTREME_SPREAD_SPIKE", "MOMENTUM_BREAKOUT"],
            "lambda_family": [120.0, 250.0],
            "lambda_family_status": ["adaptive", "adaptive"],
            "lambda_event": [80.0, 350.0],
            "lambda_event_status": ["adaptive", "fixed"],
            "lambda_state": [45.0, 100.0],
            "lambda_state_status": ["adaptive_smoothed", "insufficient_data"],
        }
    )


def test_build_lambda_snapshot_schema():
    """Snapshot DataFrame has the expected columns."""
    df = _make_fdr_df()
    snapshot = _build_lambda_snapshot(df)
    expected_cols = {"level", "template_verb", "horizon", "canonical_family",
                     "canonical_event_type", "lambda_value", "lambda_status"}
    assert set(snapshot.columns) == expected_cols
    assert set(snapshot["level"].unique()) == {"family", "event", "state"}


def test_save_lambda_state_json_roundtrip(tmp_path: Path):
    """JSON output contains all levels and can be parsed back to dict."""
    df = _make_fdr_df()
    json_path = tmp_path / "lambda_state.json"
    save_lambda_state_json(df, json_path, run_id="test_run", event_type="TEST_EVENT")

    assert json_path.exists()
    state = json.loads(json_path.read_text())

    assert "_meta" in state
    assert state["_meta"]["run_id"] == "test_run"
    assert state["_meta"]["event_type"] == "TEST_EVENT"

    # Check all levels present
    assert len(state["family"]) > 0
    assert len(state["event"]) > 0
    assert len(state["state"]) > 0

    # Verify family entries have correct keys
    for entry in state["family"]:
        assert "template_verb" in entry
        assert "horizon" in entry
        assert "lambda_value" in entry
        assert "lambda_status" in entry
        assert "canonical_family" not in entry  # family level has no family key
    
    # Verify event entries have canonical_family
    for entry in state["event"]:
        assert "canonical_family" in entry
        assert "canonical_event_type" not in entry  # event level doesn't have event type

    # Verify state entries have both
    for entry in state["state"]:
        assert "canonical_family" in entry
        assert "canonical_event_type" in entry


def test_save_lambda_state_json_empty(tmp_path: Path):
    """Empty FDR DataFrame produces valid JSON with empty levels."""
    df = pd.DataFrame()
    json_path = tmp_path / "lambda_state_empty.json"
    save_lambda_state_json(df, json_path)

    state = json.loads(json_path.read_text())
    assert state["family"] == []
    assert state["event"] == []
    assert state["state"] == []
    assert state["_meta"]["total_rows"] == 0
