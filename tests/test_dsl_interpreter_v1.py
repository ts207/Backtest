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
            "direction_score": [0.01] * 8,
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
            "triggers": ["event_detected"],
            "conditions": [condition],
            "confirmations": ["oos_validation_pass"],
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


def test_interpreter_rejects_unknown_trigger_signal() -> None:
    strat = DslInterpreterV1()
    bp = _blueprint()
    bp["entry"]["triggers"] = ["totally_unknown_signal"]
    with pytest.raises(ValueError, match=r"Blueprint `bp_interp` has unknown trigger signals: totally_unknown_signal"):
        strat.generate_positions(_bars(), _features(), {"dsl_blueprint": bp, "strategy_symbol": "BTCUSDT"})


def test_interpreter_trigger_confirmation_gate_blocks_entries() -> None:
    strat = DslInterpreterV1()
    bp = _blueprint()
    bp["entry"]["triggers"] = ["spread_guard_pass"]
    bp["entry"]["confirmations"] = ["oos_validation_pass"]

    features = _features()
    features["spread_bps"] = [30.0] * len(features)

    pos = strat.generate_positions(_bars(), features, {"dsl_blueprint": bp, "strategy_symbol": "BTCUSDT"})
    assert (pos == 0).all()


def test_interpreter_conditional_direction_can_take_short_side() -> None:
    strat = DslInterpreterV1()
    bp = _blueprint()
    bp["direction"] = "conditional"
    bp["event_type"] = "range_compression_breakout_window"

    features = _features()
    features["direction_score"] = [-0.02] * len(features)

    pos = strat.generate_positions(_bars(), features, {"dsl_blueprint": bp, "strategy_symbol": "BTCUSDT"})
    assert (-1 in set(pos.unique()))


def test_interpreter_rejects_irrecoverable_missing_signal_inputs() -> None:
    strat = DslInterpreterV1()
    bp = _blueprint()
    bp["entry"]["triggers"] = ["funding_extreme_event"]
    bp["entry"]["confirmations"] = []

    features = _features().drop(columns=["funding_rate_scaled"])
    with pytest.raises(
        ValueError,
        match=r"Blueprint `bp_interp` missing required columns for entry signals -> funding_extreme_event: funding_extreme_event",
    ):
        strat.generate_positions(_bars(), features, {"dsl_blueprint": bp, "strategy_symbol": "BTCUSDT"})


def test_interpreter_derives_session_bull_bear_and_vol_regime_features() -> None:
    strat = DslInterpreterV1()
    bp = _blueprint()
    bp["entry"]["conditions"] = []
    bp["entry"]["condition_nodes"] = [
        {"feature": "session_hour_utc", "operator": "in_range", "value": 0.0, "value_high": 0.0},
        {"feature": "bull_bear_flag", "operator": "==", "value": 0.0},
        {"feature": "vol_regime_code", "operator": ">=", "value": 0.0},
    ]
    pos = strat.generate_positions(_bars(), _features(), {"dsl_blueprint": bp, "strategy_symbol": "BTCUSDT"})
    assert (pos != 0).any()


def test_interpreter_mapped_condition_nodes_gate_entries() -> None:
    strat = DslInterpreterV1()
    bp = _blueprint()
    bp["entry"]["conditions"] = []
    bp["entry"]["condition_nodes"] = [
        {"feature": "session_hour_utc", "operator": "in_range", "value": 10.0, "value_high": 11.0}
    ]
    pos = strat.generate_positions(_bars(), _features(), {"dsl_blueprint": bp, "strategy_symbol": "BTCUSDT"})
    assert (pos == 0).all()
