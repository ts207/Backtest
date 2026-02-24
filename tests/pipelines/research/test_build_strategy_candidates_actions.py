from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.research.build_strategy_candidates as build_strategy_candidates


def test_edge_candidate_market_entry_action_is_executable_and_backtest_ready(monkeypatch):
    monkeypatch.setattr(build_strategy_candidates, "BACKTEST_READY_BASE_STRATEGIES", {"liquidity_vacuum_v1"})

    row = {
        "event": "liquidity_vacuum",
        "candidate_id": "cand_001",
        "status": "PROMOTED",
        "edge_score": 0.25,
        "expectancy_per_trade": 0.2,
        "expectancy_after_multiplicity": 0.18,
        "stability_proxy": 0.7,
        "robustness_score": 0.7,
        "event_frequency": 0.1,
        "capacity_proxy": 0.2,
        "profit_density_score": 0.05,
        "n_events": 120,
        "gate_oos_consistency_strict": True,
        "gate_bridge_tradable": True,
    }
    detail = {
        "condition": "all",
        "action": "enter_short_market",
        "gate_oos_consistency_strict": True,
        "gate_bridge_tradable": True,
    }

    candidate = build_strategy_candidates._build_edge_strategy_candidate(
        row=row,
        detail=detail,
        symbols=["BTCUSDT"],
    )

    assert candidate["executable_action"] is True
    assert candidate["backtest_ready"] is True
    assert candidate["backtest_ready_reason"] == ""
