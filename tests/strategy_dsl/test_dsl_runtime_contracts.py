from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from strategies.dsl_interpreter_v1 import DslInterpreterV1, _build_blueprint


def _base_blueprint() -> dict:
    return {
        "id": "bp_test",
        "run_id": "r1",
        "event_type": "vol_shock_relaxation",
        "candidate_id": "cand_1",
        "symbol_scope": {"mode": "single_symbol", "symbols": ["BTCUSDT"], "candidate_symbol": "BTCUSDT"},
        "direction": "long",
        "entry": {
            "triggers": ["event_detected"],
            "conditions": ["all"],
            "confirmations": ["oos_validation_pass"],
            "delay_bars": 0,
            "cooldown_bars": 0,
            "condition_logic": "all",
            "condition_nodes": [],
            "arm_bars": 0,
            "reentry_lockout_bars": 0,
        },
        "exit": {
            "time_stop_bars": 10,
            "invalidation": {"metric": "close", "operator": ">", "value": 10_000.0},
            "stop_type": "percent",
            "stop_value": 0.01,
            "target_type": "percent",
            "target_value": 0.02,
            "trailing_stop_type": "none",
            "trailing_stop_value": 0.0,
            "break_even_r": 0.0,
        },
        "sizing": {
            "mode": "fixed_risk",
            "risk_per_trade": 0.01,
            "target_vol": None,
            "max_gross_leverage": 1.0,
            "max_position_scale": 1.0,
            "portfolio_risk_budget": 1.0,
            "symbol_risk_budget": 1.0,
        },
        "overlays": [],
        "evaluation": {
            "min_trades": 1,
            "cost_model": {"fees_bps": 2.0, "slippage_bps": 2.0, "funding_included": True},
            "robustness_flags": {
                "oos_required": True,
                "multiplicity_required": True,
                "regime_stability_required": True,
            },
        },
        "lineage": {
            "source_path": "dummy",
            "compiler_version": "v1",
            "generated_at_utc": "2026-01-01T00:00:00Z",
        },
    }


def test_blueprint_feature_reference_contract_rejects_disallowed_condition_feature():
    bp = _base_blueprint()
    bp["entry"]["condition_nodes"] = [
        {
            "feature": "forward_return_h",
            "operator": ">",
            "value": 0.0,
            "lookback_bars": 0,
            "window_bars": 0,
        }
    ]
    with pytest.raises(ValueError, match="Disallowed feature reference"):
        _build_blueprint(bp)


def test_dsl_enforces_minimum_one_bar_decision_lag():
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                ],
                utc=True,
            ),
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.0, 101.0, 102.0],
            "quote_volume": [1_000_000.0, 1_000_000.0, 1_000_000.0],
        }
    )
    features = bars[["timestamp", "close", "quote_volume"]].copy()

    strategy = DslInterpreterV1()
    positions = strategy.generate_positions(
        bars=bars,
        features=features,
        params={
            "strategy_symbol": "BTCUSDT",
            "dsl_blueprint": _base_blueprint(),
        },
    )

    assert int(positions.iloc[0]) == 0
    assert int(positions.iloc[1]) in {0, 1}
    assert int(positions.iloc[1]) == 1
