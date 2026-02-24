from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.research.summarize_discovery_quality as summarize_discovery_quality


def test_gate_pass_series_prefers_gate_phase2_final_when_present():
    frame = pd.DataFrame(
        {
            "gate_all": [1, 1, 0],
            "gate_phase2_final": [0, 1, 0],
        }
    )

    observed = summarize_discovery_quality._gate_pass_series(frame)

    assert observed.tolist() == [False, True, False]


def test_build_summary_uses_phase2_final_for_family_and_global_pass_counts(tmp_path, monkeypatch):
    run_id = "test_run"
    phase2_root = tmp_path / "reports" / "phase2" / run_id
    event_dir = phase2_root / "LIQUIDITY_VACUUM"
    event_dir.mkdir(parents=True, exist_ok=True)

    frame = pd.DataFrame(
        [
            {"candidate_id": "c1", "gate_all": 1, "gate_phase2_final": 1, "fail_reasons": ""},
            {"candidate_id": "c2", "gate_all": 1, "gate_phase2_final": 0, "fail_reasons": "bridge_cost"},
            {"candidate_id": "c3", "gate_all": 1, "gate_phase2_final": 0, "fail_reasons": "stability"},
        ]
    )
    frame.to_csv(event_dir / "phase2_candidates.csv", index=False)

    monkeypatch.setattr(summarize_discovery_quality, "DATA_ROOT", tmp_path)
    payload = summarize_discovery_quality.build_summary(
        run_id=run_id,
        phase2_root=phase2_root,
        top_fail_reasons=5,
    )

    family = payload["by_event_family"]["LIQUIDITY_VACUUM"]
    assert family["phase2_candidates"] == 3
    assert family["gate_pass_count"] == 1
    assert family["phase2_gate_all_pass"] == 1
    assert payload["gate_pass_count"] == 1
