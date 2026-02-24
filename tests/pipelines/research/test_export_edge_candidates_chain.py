from __future__ import annotations

from types import SimpleNamespace
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.research.export_edge_candidates as export_edge_candidates


def test_export_chain_includes_declared_subtype_families():
    expected_scripts = {
        "FUNDING_EXTREME_ONSET": "analyze_funding_episode_events.py",
        "FUNDING_PERSISTENCE_TRIGGER": "analyze_funding_episode_events.py",
        "FUNDING_NORMALIZATION_TRIGGER": "analyze_funding_episode_events.py",
        "OI_SPIKE_POSITIVE": "analyze_oi_shock_events.py",
        "OI_SPIKE_NEGATIVE": "analyze_oi_shock_events.py",
        "OI_FLUSH": "analyze_oi_shock_events.py",
    }
    chain_map = {event: script for event, script, _ in export_edge_candidates.PHASE2_EVENT_CHAIN}
    for event_type, expected_script in expected_scripts.items():
        assert chain_map.get(event_type) == expected_script


def test_export_execute_chain_uses_phase2_candidate_discovery(monkeypatch, tmp_path):
    fake_root = tmp_path / "project"
    research_dir = fake_root / "pipelines" / "research"
    research_dir.mkdir(parents=True, exist_ok=True)
    for name in (
        "analyze_liquidity_vacuum.py",
        "build_event_registry.py",
        "phase2_candidate_discovery.py",
        "bridge_evaluate_phase2.py",
    ):
        (research_dir / name).write_text("#!/usr/bin/env python3\n", encoding="utf-8")

    captured: list[list[str]] = []

    def fake_run(cmd, *args, **kwargs):
        captured.append([str(x) for x in cmd])
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(export_edge_candidates, "PROJECT_ROOT", fake_root)
    monkeypatch.setattr(
        export_edge_candidates,
        "PHASE2_EVENT_CHAIN",
        [("LIQUIDITY_VACUUM", "analyze_liquidity_vacuum.py", [])],
    )
    monkeypatch.setattr(export_edge_candidates.subprocess, "run", fake_run)

    export_edge_candidates._run_research_chain(
        run_id="r_test",
        symbols="BTCUSDT",
        run_hypothesis_generator=False,
        hypothesis_datasets="auto",
        hypothesis_max_fused=4,
    )

    phase2_cmds = [cmd for cmd in captured if any("phase2_candidate_discovery.py" in token for token in cmd)]
    legacy_cmds = [cmd for cmd in captured if any("phase2_conditional_hypotheses.py" in token for token in cmd)]
    assert phase2_cmds
    assert not legacy_cmds
