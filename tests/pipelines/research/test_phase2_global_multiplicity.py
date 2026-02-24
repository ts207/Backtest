from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research import phase2_candidate_discovery


def test_apply_multiplicity_controls_applies_global_bh_over_family_q():
    rows = [
        {"candidate_id": "low_signal", "family_id": "fam_a", "p_value": 0.01},
        {"candidate_id": "noise_a", "family_id": "fam_a", "p_value": 0.90},
    ]
    for idx in range(1, 10):
        rows.append({"candidate_id": f"noise_{idx}", "family_id": f"fam_{idx+1}", "p_value": 0.90})
    raw_df = pd.DataFrame(rows)

    out = phase2_candidate_discovery._apply_multiplicity_controls(raw_df=raw_df, max_q=0.05)
    low = out[out["candidate_id"] == "low_signal"].iloc[0]

    assert bool(low["is_discovery_family"]) is True
    assert bool(low["is_discovery"]) is False
    assert float(low["q_value_family"]) <= 0.05
    assert float(low["q_value"]) > 0.05
