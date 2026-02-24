from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from events.registry import EVENT_REGISTRY_SPECS, normalize_phase1_events


def _base_events() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_type": [
                "FUNDING_EXTREME_ONSET",
                "FUNDING_PERSISTENCE_TRIGGER",
                "FUNDING_NORMALIZATION_TRIGGER",
                "OI_SPIKE_POSITIVE",
            ],
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                    "2026-01-01T00:15:00Z",
                ],
                utc=True,
            ),
            "symbol": ["BTCUSDT"] * 4,
            "event_id": ["e0", "e1", "e2", "e3"],
        }
    )


def test_normalize_phase1_events_subtype_filters_to_exact_event_type():
    events = _base_events()
    spec = EVENT_REGISTRY_SPECS["FUNDING_EXTREME_ONSET"]
    normalized = normalize_phase1_events(events=events, spec=spec, run_id="r1")
    assert len(normalized) == 1
    assert normalized["event_type"].iloc[0] == "FUNDING_EXTREME_ONSET"
    assert normalized["event_id"].iloc[0] == "e0"


def test_normalize_phase1_events_without_event_type_column_keeps_rows():
    events = _base_events().drop(columns=["event_type"])
    spec = EVENT_REGISTRY_SPECS["FUNDING_EXTREME_ONSET"]
    normalized = normalize_phase1_events(events=events, spec=spec, run_id="r1")
    assert len(normalized) == len(events)


def test_registry_uses_canonical_funding_specs():
    assert "FUNDING_PERSISTENCE_TRIGGER" in EVENT_REGISTRY_SPECS
