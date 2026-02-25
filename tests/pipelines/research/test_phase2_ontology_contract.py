from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research import phase2_candidate_discovery as p2


def _valid_event_plan_row() -> dict:
    return {
        "plan_row_id": "CL_1:VOL_SHOCK:mean_reversion:5m:all:BTCUSDT",
        "object_type": "event",
        "runtime_event_type": "vol_shock_relaxation",
        "canonical_event_type": "VOL_SHOCK",
        "event_type": "VOL_SHOCK",
        "rule_template": "mean_reversion",
        "horizon": "5m",
        "symbol": "BTCUSDT",
        "conditioning": {},
        "min_events": 50,
        "ontology_spec_hash": "sha256:hash_a",
        "ontology_in_canonical_registry": True,
        "ontology_unknown_templates": [],
    }


def test_validate_candidate_plan_ontology_rejects_drift_without_override():
    rows = [_valid_event_plan_row()]
    errors = p2._validate_candidate_plan_ontology(
        rows,
        run_manifest_ontology_hash="sha256:hash_a",
        current_ontology_hash="sha256:hash_b",
        allow_hash_mismatch=False,
    )
    assert errors
    assert any("ontology hash mismatch" in msg for msg in errors)


def test_validate_candidate_plan_ontology_allows_drift_with_override():
    rows = [_valid_event_plan_row()]
    errors = p2._validate_candidate_plan_ontology(
        rows,
        run_manifest_ontology_hash="sha256:hash_a",
        current_ontology_hash="sha256:hash_b",
        allow_hash_mismatch=True,
    )
    assert errors == []


def test_phase2_state_context_column_resolution_uses_canonical_mapping():
    cols = pd.Index(["timestamp", "low_liquidity_state", "vol_regime"])
    resolved = p2._resolve_state_context_column(cols, "LOW_LIQUIDITY_STATE")
    assert resolved == "low_liquidity_state"


def test_phase2_bool_mask_from_series_accepts_numeric_and_text_flags():
    numeric = p2._bool_mask_from_series(pd.Series([1, 0, 2, None]))
    text = p2._bool_mask_from_series(pd.Series(["true", "false", "yes", "off"]))
    assert numeric.tolist() == [True, False, True, False]
    assert text.tolist() == [True, False, True, False]


def test_optional_token_normalizes_null_markers():
    assert p2._optional_token(None) is None
    assert p2._optional_token(float("nan")) is None
    assert p2._optional_token("None") is None
    assert p2._optional_token(" null ") is None
    assert p2._optional_token("NaN") is None
    assert p2._optional_token("LOW_LIQUIDITY_STATE") == "LOW_LIQUIDITY_STATE"
