import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.backtest import backtest_strategies as bts


def _write_blueprints(path: Path) -> None:
    rows = [
        {
            "id": "bp_one",
            "run_id": "r1",
            "event_type": "vol_shock_relaxation",
            "candidate_id": "c1",
            "symbol_scope": {"mode": "single_symbol", "symbols": ["BTCUSDT"], "candidate_symbol": "BTCUSDT"},
            "direction": "conditional",
            "entry": {
                "triggers": ["t"],
                "conditions": ["all"],
                "confirmations": ["c"],
                "delay_bars": 0,
                "cooldown_bars": 4,
            },
            "exit": {
                "time_stop_bars": 12,
                "invalidation": {"metric": "adverse_proxy", "operator": ">", "value": 0.02},
                "stop_type": "percent",
                "stop_value": 0.01,
                "target_type": "percent",
                "target_value": 0.02,
            },
            "sizing": {"mode": "fixed_risk", "risk_per_trade": 0.004, "target_vol": None, "max_gross_leverage": 1.0},
            "overlays": [{"name": "liquidity_guard", "params": {"min_notional": 0.0}}],
            "evaluation": {
                "min_trades": 20,
                "cost_model": {"fees_bps": 3.0, "slippage_bps": 1.0, "funding_included": True},
                "robustness_flags": {"oos_required": True, "multiplicity_required": True, "regime_stability_required": True},
            },
            "lineage": {"source_path": "x", "compiler_version": "v1", "generated_at_utc": "1970-01-01T00:00:00Z"},
        },
        {
            "id": "bp_two",
            "run_id": "r1",
            "event_type": "liquidity_absence_window",
            "candidate_id": "c2",
            "symbol_scope": {"mode": "single_symbol", "symbols": ["ETHUSDT"], "candidate_symbol": "ETHUSDT"},
            "direction": "conditional",
            "entry": {
                "triggers": ["t"],
                "conditions": ["all"],
                "confirmations": ["c"],
                "delay_bars": 0,
                "cooldown_bars": 4,
            },
            "exit": {
                "time_stop_bars": 12,
                "invalidation": {"metric": "adverse_proxy", "operator": ">", "value": 0.02},
                "stop_type": "percent",
                "stop_value": 0.01,
                "target_type": "percent",
                "target_value": 0.02,
            },
            "sizing": {"mode": "fixed_risk", "risk_per_trade": 0.004, "target_vol": None, "max_gross_leverage": 1.0},
            "overlays": [{"name": "liquidity_guard", "params": {"min_notional": 0.0}}],
            "evaluation": {
                "min_trades": 20,
                "cost_model": {"fees_bps": 3.0, "slippage_bps": 1.0, "funding_included": True},
                "robustness_flags": {"oos_required": True, "multiplicity_required": True, "regime_stability_required": True},
            },
            "lineage": {"source_path": "x", "compiler_version": "v1", "generated_at_utc": "1970-01-01T00:00:00Z"},
        },
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def _fake_engine_payload(tmp_path: Path, run_id: str, strategy_ids: list[str]) -> dict:
    engine_dir = tmp_path / "runs" / run_id / "engine"
    engine_dir.mkdir(parents=True, exist_ok=True)
    strategy_frames = {}
    for sid in strategy_ids:
        strategy_frames[sid] = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T00:15:00Z"]),
                "symbol": ["BTCUSDT", "BTCUSDT"],
                "pos": [0, 1],
                "position_scale": [1.0, 1.0],
                "ret": [0.0, 0.01],
                "pnl": [0.0, -0.0006],
                "close": [100.0, 101.0],
                "high": [101.0, 102.0],
                "low": [99.0, 100.0],
                "high_96": [101.0, 102.0],
                "low_96": [99.0, 100.0],
            }
        )
    portfolio = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T00:15:00Z"]),
            "portfolio_pnl": [0.0, -0.0006],
        }
    )
    return {"engine_dir": engine_dir, "strategy_frames": strategy_frames, "portfolio": portfolio}


def test_backtest_blueprint_mode_rejects_mixed_strategy_and_blueprint_args(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest_strategies.py",
            "--run_id",
            "r",
            "--symbols",
            "BTCUSDT",
            "--strategies",
            "vol_compression_v1",
            "--blueprints_path",
            str(tmp_path / "bp.jsonl"),
        ],
    )
    assert bts.main() == 1


def test_backtest_blueprint_mode_uses_file_order_top_k(monkeypatch, tmp_path: Path) -> None:
    run_id = "bp_mode_run"
    bp_path = tmp_path / "blueprints.jsonl"
    _write_blueprints(bp_path)
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(bts, "DATA_ROOT", tmp_path)

    captured = {}

    def _fake_engine(**kwargs):
        captured["kwargs"] = kwargs
        return _fake_engine_payload(tmp_path=tmp_path, run_id=run_id, strategy_ids=kwargs["strategies"])

    monkeypatch.setattr(bts, "run_engine", _fake_engine)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest_strategies.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--blueprints_path",
            str(bp_path),
            "--blueprints_top_k",
            "1",
            "--force",
            "1",
        ],
    )
    assert bts.main() == 0
    assert captured["kwargs"]["strategies"] == ["dsl_interpreter_v1__bp_one"]
    strategy_params = captured["kwargs"]["params_by_strategy"]["dsl_interpreter_v1__bp_one"]
    assert strategy_params["position_scale"] == 1.0
    metrics_path = tmp_path / "lake" / "trades" / "backtests" / "dsl" / run_id / "metrics.json"
    payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert payload["metadata"]["execution_mode"] == "blueprint"
    assert payload["metadata"]["blueprint_ids"] == ["bp_one"]


def test_backtest_blueprint_mode_filters_event_type(monkeypatch, tmp_path: Path) -> None:
    run_id = "bp_mode_filter"
    bp_path = tmp_path / "blueprints.jsonl"
    _write_blueprints(bp_path)
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(bts, "DATA_ROOT", tmp_path)

    captured = {}

    def _fake_engine(**kwargs):
        captured["strategies"] = kwargs["strategies"]
        return _fake_engine_payload(tmp_path=tmp_path, run_id=run_id, strategy_ids=kwargs["strategies"])

    monkeypatch.setattr(bts, "run_engine", _fake_engine)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest_strategies.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--blueprints_path",
            str(bp_path),
            "--blueprints_filter_event_type",
            "liquidity_absence_window",
            "--force",
            "1",
        ],
    )
    assert bts.main() == 0
    assert captured["strategies"] == ["dsl_interpreter_v1__bp_two"]


def test_backtest_blueprint_mode_invalid_schema_fails_fast(monkeypatch, tmp_path: Path) -> None:
    run_id = "bp_mode_invalid"
    bp_path = tmp_path / "invalid.jsonl"
    bp_path.write_text("{\"id\":\"broken\"}\n", encoding="utf-8")
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(bts, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest_strategies.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--blueprints_path",
            str(bp_path),
            "--force",
            "1",
        ],
    )
    assert bts.main() == 1


def test_backtest_blueprint_mode_applies_sizing_caps(monkeypatch, tmp_path: Path) -> None:
    run_id = "bp_mode_caps"
    bp_path = tmp_path / "blueprints_caps.jsonl"
    row = {
        "id": "bp_caps",
        "run_id": "r1",
        "event_type": "vol_shock_relaxation",
        "candidate_id": "c1",
        "symbol_scope": {"mode": "single_symbol", "symbols": ["BTCUSDT"], "candidate_symbol": "BTCUSDT"},
        "direction": "conditional",
        "entry": {"triggers": ["t"], "conditions": ["all"], "confirmations": ["c"], "delay_bars": 0, "cooldown_bars": 4},
        "exit": {
            "time_stop_bars": 12,
            "invalidation": {"metric": "adverse_proxy", "operator": ">", "value": 0.02},
            "stop_type": "percent",
            "stop_value": 0.01,
            "target_type": "percent",
            "target_value": 0.02,
        },
        "sizing": {
            "mode": "fixed_risk",
            "risk_per_trade": 0.004,
            "target_vol": None,
            "max_gross_leverage": 1.0,
            "max_position_scale": 0.25,
            "portfolio_risk_budget": 0.9,
            "symbol_risk_budget": 0.8,
        },
        "overlays": [{"name": "liquidity_guard", "params": {"min_notional": 0.0}}],
        "evaluation": {
            "min_trades": 20,
            "cost_model": {"fees_bps": 3.0, "slippage_bps": 1.0, "funding_included": True},
            "robustness_flags": {"oos_required": True, "multiplicity_required": True, "regime_stability_required": True},
        },
        "lineage": {"source_path": "x", "compiler_version": "v1", "generated_at_utc": "1970-01-01T00:00:00Z"},
    }
    bp_path.write_text(json.dumps(row) + "\n", encoding="utf-8")
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(bts, "DATA_ROOT", tmp_path)

    captured = {}

    def _fake_engine(**kwargs):
        captured["kwargs"] = kwargs
        return _fake_engine_payload(tmp_path=tmp_path, run_id=run_id, strategy_ids=kwargs["strategies"])

    monkeypatch.setattr(bts, "run_engine", _fake_engine)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest_strategies.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--blueprints_path",
            str(bp_path),
            "--force",
            "1",
        ],
    )
    assert bts.main() == 0
    strategy_params = captured["kwargs"]["params_by_strategy"]["dsl_interpreter_v1__bp_caps"]
    assert strategy_params["position_scale"] == 0.25
