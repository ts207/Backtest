from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research import phase2_candidate_discovery


def test_sample_size_uses_joined_candidate_count_not_symbol_total():
    observed = phase2_candidate_discovery._resolved_sample_size(
        joined_event_count=27,
        symbol_event_count=913,
    )
    assert observed == 27


def test_sample_size_never_negative():
    observed = phase2_candidate_discovery._resolved_sample_size(
        joined_event_count=-3,
        symbol_event_count=100,
    )
    assert observed == 0
