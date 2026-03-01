from __future__ import annotations

import sys
import pytest
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.research.summarize_discovery_quality as summarize_discovery_quality


def test_overlay_kill_count_from_bridge_fail_reasons(tmp_path, monkeypatch):
    """overlay_kill_by_missing_base_count is populated when bridge_fail_reasons
    contains the gate_bridge_missing_overlay_base token."""
    run_id = "test_run"
    phase2_root = tmp_path / "reports" / "phase2" / run_id
    event_dir = phase2_root / "LIQUIDITY_VACUUM"
    event_dir.mkdir(parents=True, exist_ok=True)

    # Minimal phase2 candidates CSV (no bridge info in this file)
    pd.DataFrame(
        [{"candidate_id": "c1", "gate_phase2_final": 1, "fail_reasons": ""}]
    ).to_csv(event_dir / "phase2_candidates.csv", index=False)

    # Bridge metrics with one overlay killed by missing base
    bridge_dir = tmp_path / "reports" / "bridge_eval" / run_id / "LIQUIDITY_VACUUM"
    bridge_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "bridge_eval_status": "tradable",
                "gate_bridge_tradable": 1,
                "bridge_fail_reasons": "",
            },
            {
                "candidate_id": "c2",
                "bridge_eval_status": "rejected:missing_overlay_base",
                "gate_bridge_tradable": 0,
                "bridge_fail_reasons": "gate_bridge_missing_overlay_base",
            },
            {
                "candidate_id": "c3",
                "bridge_eval_status": "rejected:missing_overlay_base",
                "gate_bridge_tradable": 0,
                "bridge_fail_reasons": "gate_bridge_missing_overlay_base",
            },
        ]
    ).to_csv(bridge_dir / "bridge_candidate_metrics.csv", index=False)

    monkeypatch.setattr(summarize_discovery_quality, "DATA_ROOT", tmp_path)
    payload = summarize_discovery_quality.build_summary(
        run_id=run_id,
        phase2_root=phase2_root,
        top_fail_reasons=5,
    )

    family = payload["by_event_family"]["LIQUIDITY_VACUUM"]
    assert family["overlay_kill_by_missing_base_count"] == 2


def test_overlay_kill_count_from_bridge_eval_status(tmp_path, monkeypatch):
    """Falls back to bridge_eval_status column when bridge_fail_reasons is absent."""
    run_id = "test_run_b"
    phase2_root = tmp_path / "reports" / "phase2" / run_id
    event_dir = phase2_root / "VOL_SHOCK"
    event_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [{"candidate_id": "c1", "gate_phase2_final": 1, "fail_reasons": ""}]
    ).to_csv(event_dir / "phase2_candidates.csv", index=False)

    bridge_dir = tmp_path / "reports" / "bridge_eval" / run_id / "VOL_SHOCK"
    bridge_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "bridge_eval_status": "rejected:missing_overlay_base",
                "gate_bridge_tradable": 0,
                # Note: no bridge_fail_reasons column
            },
        ]
    ).to_csv(bridge_dir / "bridge_candidate_metrics.csv", index=False)

    monkeypatch.setattr(summarize_discovery_quality, "DATA_ROOT", tmp_path)
    payload = summarize_discovery_quality.build_summary(
        run_id=run_id,
        phase2_root=phase2_root,
        top_fail_reasons=5,
    )

    family = payload["by_event_family"]["VOL_SHOCK"]
    assert family["overlay_kill_by_missing_base_count"] == 1


def test_overlay_kill_count_zero_when_none(tmp_path, monkeypatch):
    """overlay_kill_by_missing_base_count is 0 when no overlay kills occurred."""
    run_id = "test_run_c"
    phase2_root = tmp_path / "reports" / "phase2" / run_id
    event_dir = phase2_root / "VOL_SHOCK"
    event_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [{"candidate_id": "c1", "gate_phase2_final": 1, "fail_reasons": ""}]
    ).to_csv(event_dir / "phase2_candidates.csv", index=False)

    bridge_dir = tmp_path / "reports" / "bridge_eval" / run_id / "VOL_SHOCK"
    bridge_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "bridge_eval_status": "tradable",
                "gate_bridge_tradable": 1,
                "bridge_fail_reasons": "",
            },
        ]
    ).to_csv(bridge_dir / "bridge_candidate_metrics.csv", index=False)

    monkeypatch.setattr(summarize_discovery_quality, "DATA_ROOT", tmp_path)
    payload = summarize_discovery_quality.build_summary(
        run_id=run_id,
        phase2_root=phase2_root,
        top_fail_reasons=5,
    )

    family = payload["by_event_family"]["VOL_SHOCK"]
    assert family["overlay_kill_by_missing_base_count"] == 0
