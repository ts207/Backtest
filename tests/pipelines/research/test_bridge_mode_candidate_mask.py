from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research.bridge_evaluate_phase2 import _select_bridge_candidates


def test_bridge_select_candidates_research_mode_uses_gate_phase2_research():
    df = pd.DataFrame(
        [
            {"candidate_id": "c1", "gate_phase2_research": True, "gate_phase2_final": False, "is_discovery": False},
            {"candidate_id": "c2", "gate_phase2_research": False, "gate_phase2_final": True, "is_discovery": True},
        ]
    )
    out = _select_bridge_candidates(full_candidates=df, mode="research")
    assert out["candidate_id"].tolist() == ["c1"]


def test_bridge_select_candidates_production_mode_preserves_strict_discovery_gate():
    df = pd.DataFrame(
        [
            {"candidate_id": "c1", "gate_phase2_research": True, "gate_phase2_final": True, "is_discovery": False},
            {"candidate_id": "c2", "gate_phase2_research": False, "gate_phase2_final": True, "is_discovery": True},
        ]
    )
    out = _select_bridge_candidates(full_candidates=df, mode="production")
    assert out["candidate_id"].tolist() == ["c2"]
