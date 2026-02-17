import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from strategies.dsl_interpreter_v1 import DslInterpreterV1


def _bars() -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=8, freq="15min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "open": [100, 101, 102, 103, 104, 105, 106, 107],
            "high": [101, 102, 103, 104, 105, 106, 107, 108],
            "low": [99, 100, 101, 102, 103, 104, 105, 106],
            "close": [100, 101, 102, 103, 104, 105, 106, 107],
        }
    )


def _features() -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=8, freq="15min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "high_96": [101.0] * 8,
            "low_96": [99.0] * 8,
            "adverse_proxy": [0.0, 0.0, 0.0, 0.03, 0.0, 0.0, 0.0, 0.0],
            "quote_volume": [1000.0] * 8,
            "spread_bps": [2.0] * 8,
            "funding_rate_scaled": [0.0] * 8,
            "forward_abs_return_h": [0.01] * 8,
        }
    )


def _blueprint(overlay_name: str = "liquidity_guard", condition: str = "all") -> dict:
    return {
        "id": "bp_interp",
        "run_id": "r1",
        "event_type": "vol_shock_relaxation",
        "candidate_id": "c1",
        "symbol_scope": {"mode": "single_symbol", "symbols": ["BTCUSDT"], "candidate_symbol": "BTCUSDT"},
        "direction": "long",
        "entry": {
            "triggers": ["t"],
            "conditions": [condition],
            "confirmations": ["c"],
            "delay_bars": 0,
            "cooldown_bars": 2,
            "condition_logic": "all",
            "condition_nodes": [],
            "arm_bars": 0,
            "reentry_lockout_bars": 0,
        },
        "exit": {
            "time_stop_bars": 3,
            "invalidation": {"metric": "adverse_proxy", "operator": ">", "value": 0.02},
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
            "risk_per_trade": 0.004,
            "target_vol": None,
            "max_gross_leverage": 1.0,
            "max_position_scale": 1.0,
            "portfolio_risk_budget": 1.0,
            "symbol_risk_budget": 1.0,
        },
        "overlays": [{"name": overlay_name, "params": {"min_notional": 0.0}}],
        "evaluation": {
            "min_trades": 20,
            "cost_model": {"fees_bps": 3.0, "slippage_bps": 1.0, "funding_included": True},
            "robustness_flags": {"oos_required": True, "multiplicity_required": True, "regime_stability_required": True},
        },
        "lineage": {"source_path": "x", "compiler_version": "v1", "generated_at_utc": "1970-01-01T00:00:00Z"},
    }


def test_interpreter_generates_positions() -> None:
    strat = DslInterpreterV1()
    pos = strat.generate_positions(_bars(), _features(), {"dsl_blueprint": _blueprint(), "strategy_symbol": "BTCUSDT"})
    assert set(pos.unique()).issubset({-1, 0, 1})
    assert (pos != 0).any()


def test_interpreter_respects_symbol_scope() -> None:
    strat = DslInterpreterV1()
    pos = strat.generate_positions(_bars(), _features(), {"dsl_blueprint": _blueprint(), "strategy_symbol": "ETHUSDT"})
    assert (pos == 0).all()


def test_interpreter_rejects_unknown_overlay() -> None:
    strat = DslInterpreterV1()
    with pytest.raises(ValueError, match="Unknown overlay"):
        strat.generate_positions(
            _bars(),
            _features(),
            {"dsl_blueprint": _blueprint(overlay_name="not_a_real_overlay"), "strategy_symbol": "BTCUSDT"},
        )


def test_interpreter_rejects_unknown_condition_column() -> None:
    strat = DslInterpreterV1()
    with pytest.raises(ValueError, match="Unknown condition column"):
        strat.generate_positions(
            _bars(),
            _features(),
            {"dsl_blueprint": _blueprint(condition="missing_col > 1"), "strategy_symbol": "BTCUSDT"},
        )


def test_interpreter_supports_condition_nodes_and_any_logic() -> None:
    strat = DslInterpreterV1()
    bp = _blueprint()
    bp["entry"]["conditions"] = []
    bp["entry"]["condition_logic"] = "any"
    bp["entry"]["condition_nodes"] = [
        {"feature": "spread_bps", "operator": "crosses_above", "value": 1.0},
        {"feature": "quote_volume", "operator": ">", "value": 900.0},
    ]
    pos = strat.generate_positions(_bars(), _features(), {"dsl_blueprint": bp, "strategy_symbol": "BTCUSDT"})
    assert (pos != 0).any()


def test_interpreter_applies_delay_arming_state() -> None:
    strat = DslInterpreterV1()
    bp = _blueprint()
    bp["entry"]["delay_bars"] = 2
    bp["entry"]["arm_bars"] = 2
    pos = strat.generate_positions(_bars(), _features(), {"dsl_blueprint": bp, "strategy_symbol": "BTCUSDT"})
    non_zero_idx = [i for i, v in enumerate(pos.tolist()) if v != 0]
    assert non_zero_idx
    assert non_zero_idx[0] >= 2
