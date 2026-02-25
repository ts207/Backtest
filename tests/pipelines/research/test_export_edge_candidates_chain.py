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
        "DEPTH_COLLAPSE": "analyze_liquidity_dislocation_events.py",
        "SPREAD_BLOWOUT": "analyze_liquidity_dislocation_events.py",
        "ORDERFLOW_IMBALANCE_SHOCK": "analyze_liquidity_dislocation_events.py",
        "SWEEP_STOPRUN": "analyze_liquidity_dislocation_events.py",
        "ABSORPTION_EVENT": "analyze_liquidity_dislocation_events.py",
        "LIQUIDITY_GAP_PRINT": "analyze_liquidity_dislocation_events.py",
        "VOL_SPIKE": "analyze_volatility_transition_events.py",
        "VOL_RELAXATION_START": "analyze_volatility_transition_events.py",
        "VOL_CLUSTER_SHIFT": "analyze_volatility_transition_events.py",
        "RANGE_COMPRESSION_END": "analyze_volatility_transition_events.py",
        "BREAKOUT_TRIGGER": "analyze_volatility_transition_events.py",
        "FUNDING_FLIP": "analyze_positioning_extremes_events.py",
        "DELEVERAGING_WAVE": "analyze_positioning_extremes_events.py",
        "TREND_EXHAUSTION_TRIGGER": "analyze_forced_flow_and_exhaustion_events.py",
        "MOMENTUM_DIVERGENCE_TRIGGER": "analyze_forced_flow_and_exhaustion_events.py",
        "CLIMAX_VOLUME_BAR": "analyze_forced_flow_and_exhaustion_events.py",
        "FAILED_CONTINUATION": "analyze_forced_flow_and_exhaustion_events.py",
        "RANGE_BREAKOUT": "analyze_trend_structure_events.py",
        "FALSE_BREAKOUT": "analyze_trend_structure_events.py",
        "TREND_ACCELERATION": "analyze_trend_structure_events.py",
        "TREND_DECELERATION": "analyze_trend_structure_events.py",
        "PULLBACK_PIVOT": "analyze_trend_structure_events.py",
        "SUPPORT_RESISTANCE_BREAK": "analyze_trend_structure_events.py",
        "ZSCORE_STRETCH": "analyze_statistical_dislocation_events.py",
        "BAND_BREAK": "analyze_statistical_dislocation_events.py",
        "OVERSHOOT_AFTER_SHOCK": "analyze_statistical_dislocation_events.py",
        "GAP_OVERSHOOT": "analyze_statistical_dislocation_events.py",
        "VOL_REGIME_SHIFT_EVENT": "analyze_regime_transition_events.py",
        "TREND_TO_CHOP_SHIFT": "analyze_regime_transition_events.py",
        "CHOP_TO_TREND_SHIFT": "analyze_regime_transition_events.py",
        "CORRELATION_BREAKDOWN_EVENT": "analyze_regime_transition_events.py",
        "BETA_SPIKE_EVENT": "analyze_regime_transition_events.py",
        "INDEX_COMPONENT_DIVERGENCE": "analyze_information_desync_events.py",
        "SPOT_PERP_BASIS_SHOCK": "analyze_information_desync_events.py",
        "LEAD_LAG_BREAK": "analyze_information_desync_events.py",
        "SESSION_OPEN_EVENT": "analyze_temporal_structure_events.py",
        "SESSION_CLOSE_EVENT": "analyze_temporal_structure_events.py",
        "FUNDING_TIMESTAMP_EVENT": "analyze_temporal_structure_events.py",
        "SCHEDULED_NEWS_WINDOW_EVENT": "analyze_temporal_structure_events.py",
        "SPREAD_REGIME_WIDENING_EVENT": "analyze_execution_friction_events.py",
        "SLIPPAGE_SPIKE_EVENT": "analyze_execution_friction_events.py",
        "FEE_REGIME_CHANGE_EVENT": "analyze_execution_friction_events.py",
    }
    chain_map = {event: script for event, script, _ in export_edge_candidates.PHASE2_EVENT_CHAIN}
    for event_type, expected_script in expected_scripts.items():
        assert chain_map.get(event_type) == expected_script


def test_export_chain_has_no_canonical_analyzer_routes():
    assert all(script != "analyze_canonical_events.py" for _, script, _ in export_edge_candidates.PHASE2_EVENT_CHAIN)


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
