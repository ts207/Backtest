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
                "funding_extreme_onset",
                "funding_acceleration",
                "funding_persistence_window",
                "funding_normalization",
                "oi_spike_positive",
            ],
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                    "2026-01-01T00:15:00Z",
                    "2026-01-01T00:20:00Z",
                ],
                utc=True,
            ),
            "symbol": ["BTCUSDT"] * 5,
            "event_id": ["e0", "e1", "e2", "e3", "e4"],
        }
    )


def test_normalize_phase1_events_subtype_filters_to_exact_event_type():
    events = _base_events()
    spec = EVENT_REGISTRY_SPECS["funding_extreme_onset"]
    normalized = normalize_phase1_events(events=events, spec=spec, run_id="r1")
    assert len(normalized) == 1
    assert normalized["event_type"].iloc[0] == "funding_extreme_onset"
    assert normalized["event_id"].iloc[0] == "e0"


def test_normalize_phase1_events_aggregate_funding_union_filters_children():
    events = _base_events()
    spec = EVENT_REGISTRY_SPECS["funding_episodes"]
    normalized = normalize_phase1_events(events=events, spec=spec, run_id="r1")
    assert len(normalized) == 4
    assert set(normalized["event_id"].tolist()) == {"e0", "e1", "e2", "e3"}


def test_normalize_phase1_events_without_event_type_column_keeps_rows():
    events = _base_events().drop(columns=["event_type"])
    spec = EVENT_REGISTRY_SPECS["funding_extreme_onset"]
    normalized = normalize_phase1_events(events=events, spec=spec, run_id="r1")
    assert len(normalized) == len(events)
