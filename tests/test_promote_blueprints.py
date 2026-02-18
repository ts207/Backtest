import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import promote_blueprints


def _blueprint_row(run_id: str, blueprint_id: str = "bp_one") -> dict:
    return {
        "id": blueprint_id,
        "run_id": run_id,
        "event_type": "vol_shock_relaxation",
        "candidate_id": "c1",
        "symbol_scope": {"mode": "multi_symbol", "symbols": ["BTCUSDT", "ETHUSDT"], "candidate_symbol": "ALL"},
        "direction": "conditional",
        "entry": {"triggers": ["t"], "conditions": ["all"], "confirmations": ["c"], "delay_bars": 0, "cooldown_bars": 2},
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
    }


def _write_blueprint(path: Path, run_id: str, blueprint_id: str = "bp_one") -> None:
    row = _blueprint_row(run_id=run_id, blueprint_id=blueprint_id)
    path.write_text(json.dumps(row) + "\n", encoding="utf-8")


def _write_returns(
    path: Path,
    *,
    gross_pnl_when_pos: float = 0.0020,
    trading_cost_when_pos: float = 0.0002,
) -> None:
    records = []
    for symbol in ("BTCUSDT", "ETHUSDT"):
        ts = pd.date_range("2024-01-01", periods=220, freq="15min", tz="UTC")
        for i, t in enumerate(ts):
            pos = 1 if (i % 2 == 1) else 0
            range_amp = 1.0 if (i % 4 < 2) else 3.0
            records.append(
                {
                    "timestamp": t,
                    "symbol": symbol,
                    "pos": pos,
                    "position_scale": 1.0,
                    "ret": 0.0015 if pos else 0.0,
                    "pnl": 0.0010 if pos else 0.0,
                    "gross_pnl": gross_pnl_when_pos if pos else 0.0,
                    "trading_cost": trading_cost_when_pos if pos else 0.0,
                    "funding_pnl": 0.0,
                    "borrow_cost": 0.0,
                    "close": 100.0 + float(i) * 0.01,
                    "high": 100.0 + float(i) * 0.01 + range_amp,
                    "low": 100.0 + float(i) * 0.01 - range_amp,
                }
            )
    pd.DataFrame(records).to_csv(path, index=False)


def _write_walkforward_summary(
    tmp_path: Path,
    run_id: str,
    per_strategy: dict,
    per_strategy_regime: dict | None = None,
    per_strategy_drawdown: dict | None = None,
) -> None:
    eval_dir = tmp_path / "reports" / "eval" / run_id
    eval_dir.mkdir(parents=True, exist_ok=True)
    if per_strategy_regime is None:
        per_strategy_regime = {
            strategy_id: {
                "train": {"max_regime_share": 0.6, "regime_consistent": True},
                "validation": {"max_regime_share": 0.65, "regime_consistent": True},
                "test": {"max_regime_share": 0.7, "regime_consistent": True},
            }
            for strategy_id in per_strategy.keys()
        }
    if per_strategy_drawdown is None:
        per_strategy_drawdown = {
            strategy_id: {
                "train": {
                    "max_loss_cluster_len": 12,
                    "cluster_loss_concentration": 0.30,
                    "tail_conditional_drawdown_95": -0.08,
                },
                "validation": {
                    "max_loss_cluster_len": 10,
                    "cluster_loss_concentration": 0.28,
                    "tail_conditional_drawdown_95": -0.07,
                },
                "test": {
                    "max_loss_cluster_len": 14,
                    "cluster_loss_concentration": 0.35,
                    "tail_conditional_drawdown_95": -0.10,
                },
            }
            for strategy_id in per_strategy.keys()
        }
    payload = {
        "run_id": run_id,
        "per_strategy_split_metrics": per_strategy,
        "per_strategy_regime_metrics": per_strategy_regime,
        "per_strategy_drawdown_cluster_metrics": per_strategy_drawdown,
    }
    (eval_dir / "walkforward_summary.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")


def test_promote_blueprints_emits_non_empty_survivors_and_report_counts(monkeypatch, tmp_path: Path) -> None:
    run_id = "promote_bp_pass"
    bp_dir = tmp_path / "reports" / "strategy_blueprints" / run_id
    bp_dir.mkdir(parents=True, exist_ok=True)
    bp_path = bp_dir / "blueprints.jsonl"
    _write_blueprint(bp_path, run_id=run_id)

    engine_dir = tmp_path / "runs" / run_id / "engine"
    engine_dir.mkdir(parents=True, exist_ok=True)
    _write_returns(engine_dir / "strategy_returns_dsl_interpreter_v1__bp_one.csv")
    _write_walkforward_summary(
        tmp_path,
        run_id,
        {
            "dsl_interpreter_v1__bp_one": {
                "train": {"total_trades": 110, "net_pnl": 1.0, "stressed_net_pnl": 0.7},
                "validation": {"total_trades": 90, "net_pnl": 0.6, "stressed_net_pnl": 0.4},
                "test": {"total_trades": 80, "net_pnl": 0.5, "stressed_net_pnl": 0.3},
            }
        },
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(promote_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "promote_blueprints.py",
            "--run_id",
            run_id,
        ],
    )
    assert promote_blueprints.main() == 0

    promoted_path = tmp_path / "reports" / "promotions" / run_id / "promoted_blueprints.jsonl"
    report_path = tmp_path / "reports" / "promotions" / run_id / "promotion_report.json"
    assert promoted_path.exists()
    assert report_path.exists()
    promoted_lines = [line for line in promoted_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(promoted_lines) >= 1
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["tested_count"] >= 1
    assert report["survivors_count"] >= 1
    assert isinstance(report["tested"][0].get("fail_reasons", []), list)
    assert report["tested"][0].get("evidence_mode") == "walkforward_strategy"
    assert report["tested"][0].get("regime_evidence_source") == "walkforward_strategy"
    assert report["tested"][0].get("drawdown_evidence_source") == "walkforward_strategy"
    assert report["integrity_checks"]["artifacts_validated"] is True
    assert report["integrity_checks"]["walkforward_strategy_evidence_required"] is True


def test_promote_blueprints_uses_per_strategy_walkforward_metrics(monkeypatch, tmp_path: Path) -> None:
    run_id = "promote_bp_wf_strategy"
    bp_dir = tmp_path / "reports" / "strategy_blueprints" / run_id
    bp_dir.mkdir(parents=True, exist_ok=True)
    bp_path = bp_dir / "blueprints.jsonl"
    bp_path.write_text(
        json.dumps(_blueprint_row(run_id=run_id, blueprint_id="bp_one"))
        + "\n"
        + json.dumps(_blueprint_row(run_id=run_id, blueprint_id="bp_two"))
        + "\n",
        encoding="utf-8",
    )

    engine_dir = tmp_path / "runs" / run_id / "engine"
    engine_dir.mkdir(parents=True, exist_ok=True)
    _write_returns(engine_dir / "strategy_returns_dsl_interpreter_v1__bp_one.csv")
    _write_returns(engine_dir / "strategy_returns_dsl_interpreter_v1__bp_two.csv")

    _write_walkforward_summary(
        tmp_path,
        run_id,
        {
            "dsl_interpreter_v1__bp_one": {
                "train": {"total_trades": 120, "net_pnl": 1.25, "stressed_net_pnl": 0.75},
                "validation": {"total_trades": 60, "net_pnl": 0.55, "stressed_net_pnl": 0.35},
                "test": {"total_trades": 50, "net_pnl": 0.45, "stressed_net_pnl": 0.25},
            },
            "dsl_interpreter_v1__bp_two": {
                "train": {"total_trades": 15, "net_pnl": -2.0, "stressed_net_pnl": -2.5},
                "validation": {"total_trades": 10, "net_pnl": -1.0, "stressed_net_pnl": -1.2},
                "test": {"total_trades": 8, "net_pnl": -0.5, "stressed_net_pnl": -0.7},
            },
        },
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(promote_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "promote_blueprints.py",
            "--run_id",
            run_id,
        ],
    )
    assert promote_blueprints.main() == 0

    report = json.loads((tmp_path / "reports" / "promotions" / run_id / "promotion_report.json").read_text(encoding="utf-8"))
    assert report["tested_count"] == 2

    tested = {row["blueprint_id"]: row for row in report["tested"]}
    assert tested["bp_one"]["evidence_mode"] == "walkforward_strategy"
    assert tested["bp_two"]["evidence_mode"] == "walkforward_strategy"
    assert tested["bp_one"]["trades"] == 230
    assert tested["bp_two"]["trades"] == 33
    assert tested["bp_one"]["split_pnl"]["train"] == 1.25
    assert tested["bp_two"]["split_pnl"]["train"] == -2.0


def test_promote_blueprints_reports_zero_when_no_blueprints(monkeypatch, tmp_path: Path) -> None:
    run_id = "promote_bp_empty"
    bp_dir = tmp_path / "reports" / "strategy_blueprints" / run_id
    bp_dir.mkdir(parents=True, exist_ok=True)
    (bp_dir / "blueprints.jsonl").write_text("", encoding="utf-8")

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(promote_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "promote_blueprints.py",
            "--run_id",
            run_id,
        ],
    )
    assert promote_blueprints.main() == 0
    report = json.loads((tmp_path / "reports" / "promotions" / run_id / "promotion_report.json").read_text(encoding="utf-8"))
    assert report["tested_count"] == 0
    assert report["survivors_count"] == 0


def test_promote_blueprints_fails_without_walkforward_summary(monkeypatch, tmp_path: Path) -> None:
    run_id = "promote_bp_missing_wf"
    bp_dir = tmp_path / "reports" / "strategy_blueprints" / run_id
    bp_dir.mkdir(parents=True, exist_ok=True)
    bp_path = bp_dir / "blueprints.jsonl"
    _write_blueprint(bp_path, run_id=run_id)

    engine_dir = tmp_path / "runs" / run_id / "engine"
    engine_dir.mkdir(parents=True, exist_ok=True)
    _write_returns(engine_dir / "strategy_returns_dsl_interpreter_v1__bp_one.csv")

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(promote_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["promote_blueprints.py", "--run_id", run_id])
    assert promote_blueprints.main() == 1


def test_promote_blueprints_supports_fallback_override(monkeypatch, tmp_path: Path) -> None:
    run_id = "promote_bp_fallback_override"
    bp_dir = tmp_path / "reports" / "strategy_blueprints" / run_id
    bp_dir.mkdir(parents=True, exist_ok=True)
    bp_path = bp_dir / "blueprints.jsonl"
    _write_blueprint(bp_path, run_id=run_id)

    engine_dir = tmp_path / "runs" / run_id / "engine"
    engine_dir.mkdir(parents=True, exist_ok=True)
    _write_returns(engine_dir / "strategy_returns_dsl_interpreter_v1__bp_one.csv")

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(promote_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "promote_blueprints.py",
            "--run_id",
            run_id,
            "--allow_fallback_evidence",
            "1",
        ],
    )
    assert promote_blueprints.main() == 0
    report = json.loads((tmp_path / "reports" / "promotions" / run_id / "promotion_report.json").read_text(encoding="utf-8"))
    assert report["tested_count"] == 1
    assert report["tested"][0]["evidence_mode"] == "fallback"
    assert report["integrity_checks"]["walkforward_strategy_evidence_required"] is False


def test_promote_blueprints_fails_regime_gate_when_walkforward_regime_inconsistent(monkeypatch, tmp_path: Path) -> None:
    run_id = "promote_bp_regime_fail"
    bp_dir = tmp_path / "reports" / "strategy_blueprints" / run_id
    bp_dir.mkdir(parents=True, exist_ok=True)
    bp_path = bp_dir / "blueprints.jsonl"
    _write_blueprint(bp_path, run_id=run_id)

    engine_dir = tmp_path / "runs" / run_id / "engine"
    engine_dir.mkdir(parents=True, exist_ok=True)
    _write_returns(engine_dir / "strategy_returns_dsl_interpreter_v1__bp_one.csv")

    _write_walkforward_summary(
        tmp_path,
        run_id,
        {
            "dsl_interpreter_v1__bp_one": {
                "train": {"total_trades": 120, "net_pnl": 1.0, "stressed_net_pnl": 0.8},
                "validation": {"total_trades": 110, "net_pnl": 0.7, "stressed_net_pnl": 0.5},
                "test": {"total_trades": 90, "net_pnl": 0.6, "stressed_net_pnl": 0.4},
            }
        },
        per_strategy_regime={
            "dsl_interpreter_v1__bp_one": {
                "train": {"max_regime_share": 0.95, "regime_consistent": False},
                "validation": {"max_regime_share": 0.91, "regime_consistent": False},
                "test": {"max_regime_share": 0.80, "regime_consistent": True},
            }
        },
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(promote_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["promote_blueprints.py", "--run_id", run_id, "--regime_max_share", "0.80"])
    assert promote_blueprints.main() == 0

    report = json.loads((tmp_path / "reports" / "promotions" / run_id / "promotion_report.json").read_text(encoding="utf-8"))
    assert report["tested_count"] == 1
    tested = report["tested"][0]
    assert tested["gates"]["regime_stability"] is False
    assert tested["regime_evidence_source"] == "walkforward_strategy"
    assert tested["promoted"] is False


def test_promote_blueprints_fails_drawdown_cluster_gates(monkeypatch, tmp_path: Path) -> None:
    run_id = "promote_bp_drawdown_fail"
    bp_dir = tmp_path / "reports" / "strategy_blueprints" / run_id
    bp_dir.mkdir(parents=True, exist_ok=True)
    bp_path = bp_dir / "blueprints.jsonl"
    _write_blueprint(bp_path, run_id=run_id)

    engine_dir = tmp_path / "runs" / run_id / "engine"
    engine_dir.mkdir(parents=True, exist_ok=True)
    _write_returns(engine_dir / "strategy_returns_dsl_interpreter_v1__bp_one.csv")
    _write_walkforward_summary(
        tmp_path,
        run_id,
        {
            "dsl_interpreter_v1__bp_one": {
                "train": {"total_trades": 150, "net_pnl": 1.0, "stressed_net_pnl": 0.8},
                "validation": {"total_trades": 120, "net_pnl": 0.8, "stressed_net_pnl": 0.6},
                "test": {"total_trades": 90, "net_pnl": 0.4, "stressed_net_pnl": 0.2},
            }
        },
        per_strategy_drawdown={
            "dsl_interpreter_v1__bp_one": {
                "train": {
                    "max_loss_cluster_len": 120,
                    "cluster_loss_concentration": 0.8,
                    "tail_conditional_drawdown_95": -0.6,
                },
                "validation": {
                    "max_loss_cluster_len": 110,
                    "cluster_loss_concentration": 0.7,
                    "tail_conditional_drawdown_95": -0.5,
                },
                "test": {
                    "max_loss_cluster_len": 90,
                    "cluster_loss_concentration": 0.6,
                    "tail_conditional_drawdown_95": -0.4,
                },
            }
        },
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(promote_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["promote_blueprints.py", "--run_id", run_id])
    assert promote_blueprints.main() == 0

    report = json.loads((tmp_path / "reports" / "promotions" / run_id / "promotion_report.json").read_text(encoding="utf-8"))
    tested = report["tested"][0]
    assert tested["drawdown_evidence_source"] == "walkforward_strategy"
    assert tested["gates"]["drawdown_cluster_len"] is False
    assert tested["gates"]["drawdown_cluster_concentration"] is False
    assert tested["gates"]["tail_conditional_drawdown"] is False
    assert tested["promoted"] is False


def test_promote_blueprints_blocks_when_realized_cost_ratio_exceeds_threshold(
    monkeypatch,
    tmp_path: Path,
) -> None:
    run_id = "promote_bp_cost_ratio_fail"
    bp_dir = tmp_path / "reports" / "strategy_blueprints" / run_id
    bp_dir.mkdir(parents=True, exist_ok=True)
    bp_path = bp_dir / "blueprints.jsonl"
    _write_blueprint(bp_path, run_id=run_id)

    engine_dir = tmp_path / "runs" / run_id / "engine"
    engine_dir.mkdir(parents=True, exist_ok=True)
    _write_returns(
        engine_dir / "strategy_returns_dsl_interpreter_v1__bp_one.csv",
        gross_pnl_when_pos=0.0010,
        trading_cost_when_pos=0.0100,
    )
    _write_walkforward_summary(
        tmp_path,
        run_id,
        {
            "dsl_interpreter_v1__bp_one": {
                "train": {"total_trades": 150, "net_pnl": 1.0, "stressed_net_pnl": 0.8},
                "validation": {"total_trades": 120, "net_pnl": 0.8, "stressed_net_pnl": 0.6},
                "test": {"total_trades": 90, "net_pnl": 0.4, "stressed_net_pnl": 0.2},
            }
        },
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(promote_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "promote_blueprints.py",
            "--run_id",
            run_id,
            "--max_cost_ratio_train_validation",
            "0.60",
        ],
    )
    assert promote_blueprints.main() == 0

    report = json.loads((tmp_path / "reports" / "promotions" / run_id / "promotion_report.json").read_text(encoding="utf-8"))
    tested = report["tested"][0]
    assert tested["gates"]["cost_ratio_train_validation"] is False
    assert tested["promoted"] is False
    assert tested["realized_cost_ratio_by_split"]["train"]["realized_cost_ratio"] > 0.6
    assert tested["realized_cost_ratio_by_split"]["validation"]["realized_cost_ratio"] > 0.6


def test_promote_blueprints_passes_when_realized_cost_ratio_within_threshold(
    monkeypatch,
    tmp_path: Path,
) -> None:
    run_id = "promote_bp_cost_ratio_pass"
    bp_dir = tmp_path / "reports" / "strategy_blueprints" / run_id
    bp_dir.mkdir(parents=True, exist_ok=True)
    bp_path = bp_dir / "blueprints.jsonl"
    _write_blueprint(bp_path, run_id=run_id)

    engine_dir = tmp_path / "runs" / run_id / "engine"
    engine_dir.mkdir(parents=True, exist_ok=True)
    _write_returns(engine_dir / "strategy_returns_dsl_interpreter_v1__bp_one.csv")
    _write_walkforward_summary(
        tmp_path,
        run_id,
        {
            "dsl_interpreter_v1__bp_one": {
                "train": {"total_trades": 150, "net_pnl": 1.0, "stressed_net_pnl": 0.8},
                "validation": {"total_trades": 120, "net_pnl": 0.8, "stressed_net_pnl": 0.6},
                "test": {"total_trades": 90, "net_pnl": 0.4, "stressed_net_pnl": 0.2},
            }
        },
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(promote_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "promote_blueprints.py",
            "--run_id",
            run_id,
            "--max_cost_ratio_train_validation",
            "0.60",
        ],
    )
    assert promote_blueprints.main() == 0

    report = json.loads((tmp_path / "reports" / "promotions" / run_id / "promotion_report.json").read_text(encoding="utf-8"))
    tested = report["tested"][0]
    assert tested["gates"]["cost_ratio_train_validation"] is True
    assert tested["realized_cost_ratio_by_split"]["train"]["realized_cost_ratio"] <= 0.6
    assert tested["realized_cost_ratio_by_split"]["validation"]["realized_cost_ratio"] <= 0.6
