from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.research.export_edge_candidates as export_edge_candidates


def test_export_chain_includes_declared_subtype_families():
    expected_scripts = {
        "funding_extreme_onset": "analyze_funding_episode_events.py",
        "funding_persistence_window": "analyze_funding_episode_events.py",
        "funding_normalization": "analyze_funding_episode_events.py",
        "oi_spike_positive": "analyze_oi_shock_events.py",
        "oi_spike_negative": "analyze_oi_shock_events.py",
        "oi_flush": "analyze_oi_shock_events.py",
    }
    chain_map = {event: script for event, script, _ in export_edge_candidates.PHASE2_EVENT_CHAIN}
    for event_type, expected_script in expected_scripts.items():
        assert chain_map.get(event_type) == expected_script
