from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research import phase2_candidate_discovery


def test_hierarchical_fdr_simes_and_within_family_bh():
    rows = [
        {"candidate_id": "strong_signal", "family_id": "fam_a", "p_value": 0.001},
        {"candidate_id": "weak_signal", "family_id": "fam_a", "p_value": 0.20},
    ]
    for idx in range(1, 10):
        rows.append({"candidate_id": f"noise_{idx}", "family_id": f"fam_{idx+1}", "p_value": 0.90})
    raw_df = pd.DataFrame(rows)

    out = phase2_candidate_discovery._apply_multiplicity_controls(raw_df=raw_df, max_q=0.05)
    strong = out[out["candidate_id"] == "strong_signal"].iloc[0]
    weak = out[out["candidate_id"] == "weak_signal"].iloc[0]
    noise = out[out["candidate_id"] == "noise_1"].iloc[0]

    # fam_a has Simes p-val = min(0.001*2/1, 0.20*2/2) = 0.002.
    # Global BH across 10 families => q_family = 0.002 * 10 / 1 = 0.02 <= 0.05.
    assert bool(strong["is_discovery_family"]) is True
    assert bool(weak["is_discovery_family"]) is True

    # Within fam_a: strong q = 0.001 * 2 / 1 = 0.002 <= 0.05
    assert bool(strong["is_discovery"]) is True
    # Within fam_a: weak q = 0.20 * 2 / 2 = 0.20 > 0.05
    assert bool(weak["is_discovery"]) is False
    
    # Noise families are completely discarded
    assert bool(noise["is_discovery_family"]) is False
    assert bool(noise["is_discovery"]) is False


def test_apply_multiplicity_controls_research_excludes_low_sample_rows():
    raw_df = pd.DataFrame(
        [
            {"candidate_id": "low_sample", "family_id": "fam_a", "p_value": 1e-8, "sample_size": 10},
            {"candidate_id": "eligible", "family_id": "fam_b", "p_value": 1e-3, "sample_size": 120},
            {"candidate_id": "noise", "family_id": "fam_c", "p_value": 0.9, "sample_size": 150},
        ]
    )

    out = phase2_candidate_discovery._apply_multiplicity_controls(
        raw_df=raw_df,
        max_q=0.05,
        mode="research",
        min_sample_size=50,
    )

    low = out[out["candidate_id"] == "low_sample"].iloc[0]
    eligible = out[out["candidate_id"] == "eligible"].iloc[0]

    assert bool(low["is_discovery"]) is False
    assert float(low["q_value"]) == 1.0
    assert bool(eligible["is_discovery"]) is True
