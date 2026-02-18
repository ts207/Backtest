import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.report.capital_allocation import AllocationConfig, build_allocation_weights
from pipelines.report import make_report


def test_build_allocation_weights_applies_caps_floors_and_vol_target() -> None:
    df = pd.DataFrame(
        [
            {
                "candidate_symbol": "BTCUSDT",
                "status": "PROMOTED",
                "expectancy_per_trade": 0.03,
                "variance": 0.0009,
                "n_events": 80,
            },
            {
                "candidate_symbol": "ETHUSDT",
                "status": "PROMOTED",
                "expectancy_per_trade": 0.02,
                "variance": 0.0004,
                "n_events": 50,
            },
            {
                "candidate_symbol": "SOLUSDT",
                "status": "PROMOTED",
                "expectancy_per_trade": 0.005,
                "variance": 0.0001,
                "n_events": 30,
            },
            {
                "candidate_symbol": "ALL",
                "status": "PROMOTED",
                "expectancy_per_trade": 0.5,
                "variance": 0.0001,
                "n_events": 10,
            },
        ]
    )

    out = build_allocation_weights(
        df,
        AllocationConfig(max_weight=0.5, min_weight=0.2, volatility_target=0.02, volatility_adjustment_cap=2.0),
    )

    assert set(out["symbol"]) == {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
    assert abs(float(out["final_weight"].sum()) - 1.0) < 1e-9
    assert (out["final_weight"] <= 0.5 + 1e-9).all()
    assert (out["final_weight"] >= 0.2 - 1e-9).all()


def test_make_report_writes_allocation_artifacts(tmp_path: Path, monkeypatch) -> None:
    run_id = "alloc_report"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(make_report, "DATA_ROOT", tmp_path)

    trades_dir = tmp_path / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / run_id
    trades_dir.mkdir(parents=True, exist_ok=True)
    (trades_dir / "metrics.json").write_text(
        json.dumps(
            {
                "total_trades": 2,
                "avg_r": 0.0,
                "win_rate": 0.0,
                "ending_equity": 100000.0,
                "sharpe_annualized": 0.0,
                "cost_decomposition": {"net_alpha": 0.0},
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {"symbol": "BTCUSDT", "r_multiple": 0.2},
            {"symbol": "ETHUSDT", "r_multiple": -0.1},
        ]
    ).to_csv(trades_dir / "trades_001.csv", index=False)
    pd.DataFrame([{"equity": 100000.0}, {"equity": 100050.0}]).to_csv(trades_dir / "equity_curve.csv", index=False)

    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    edge_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"candidate_symbol": "BTCUSDT", "status": "PROMOTED", "expectancy_per_trade": 0.03, "variance": 0.0009, "n_events": 50},
            {"candidate_symbol": "ETHUSDT", "status": "PROMOTED", "expectancy_per_trade": 0.02, "variance": 0.0004, "n_events": 50},
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "make_report.py",
            "--run_id",
            run_id,
        ],
    )
    assert make_report.main() == 0

    out_dir = tmp_path / "reports" / "vol_compression_expansion_v1" / run_id
    alloc_csv = out_dir / "allocation_weights.csv"
    alloc_json = out_dir / "allocation_weights.json"
    assert alloc_csv.exists()
    assert alloc_json.exists()

    alloc_df = pd.read_csv(alloc_csv)
    assert not alloc_df.empty
    assert abs(float(alloc_df["final_weight"].sum()) - 1.0) < 1e-9
    summary = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["integrity_checks"]["artifacts_validated"] is True


def test_make_report_fails_when_metrics_key_missing(tmp_path: Path, monkeypatch) -> None:
    run_id = "alloc_report_metrics_missing"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(make_report, "DATA_ROOT", tmp_path)

    trades_dir = tmp_path / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / run_id
    trades_dir.mkdir(parents=True, exist_ok=True)
    (trades_dir / "metrics.json").write_text(
        json.dumps({"total_trades": 0, "avg_r": 0.0, "win_rate": 0.0, "ending_equity": 100000.0}),
        encoding="utf-8",
    )
    pd.DataFrame([{"equity": 100000.0}]).to_csv(trades_dir / "equity_curve.csv", index=False)

    monkeypatch.setattr(sys, "argv", ["make_report.py", "--run_id", run_id])
    assert make_report.main() == 1


def test_make_report_fails_without_trade_evidence(tmp_path: Path, monkeypatch) -> None:
    run_id = "alloc_report_no_trades"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(make_report, "DATA_ROOT", tmp_path)

    trades_dir = tmp_path / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / run_id
    trades_dir.mkdir(parents=True, exist_ok=True)
    (trades_dir / "metrics.json").write_text(
        json.dumps(
            {
                "total_trades": 1,
                "avg_r": 0.0,
                "win_rate": 0.0,
                "ending_equity": 100000.0,
                "sharpe_annualized": 0.0,
                "cost_decomposition": {"net_alpha": 0.0},
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame([{"equity": 100000.0}]).to_csv(trades_dir / "equity_curve.csv", index=False)

    monkeypatch.setattr(sys, "argv", ["make_report.py", "--run_id", run_id])
    assert make_report.main() == 1


def test_make_report_backtest_fallback_override(tmp_path: Path, monkeypatch) -> None:
    run_id = "alloc_report_fallback"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(make_report, "DATA_ROOT", tmp_path)

    fallback_dir = tmp_path / "lake" / "trades" / "backtests" / "alt_strategy" / run_id
    fallback_dir.mkdir(parents=True, exist_ok=True)
    (fallback_dir / "metrics.json").write_text(
        json.dumps(
            {
                "total_trades": 1,
                "avg_r": 0.1,
                "win_rate": 1.0,
                "ending_equity": 100100.0,
                "sharpe_annualized": 1.0,
                "cost_decomposition": {"net_alpha": 0.1},
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame([{"symbol": "BTCUSDT", "r_multiple": 0.1}]).to_csv(fallback_dir / "trades_001.csv", index=False)
    pd.DataFrame([{"equity": 100000.0}, {"equity": 100100.0}]).to_csv(fallback_dir / "equity_curve.csv", index=False)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "make_report.py",
            "--run_id",
            run_id,
            "--allow_backtest_artifact_fallback",
            "1",
        ],
    )
    assert make_report.main() == 0


def test_report_fails_on_metrics_trade_mismatch(tmp_path: Path, monkeypatch) -> None:
    run_id = "report_trade_mismatch"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(make_report, "DATA_ROOT", tmp_path)

    trades_dir = tmp_path / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / run_id
    trades_dir.mkdir(parents=True, exist_ok=True)
    (trades_dir / "metrics.json").write_text(
        json.dumps(
            {
                "total_trades": 3,
                "avg_r": 0.0,
                "win_rate": 0.0,
                "ending_equity": 100000.0,
                "sharpe_annualized": 0.0,
                "cost_decomposition": {"net_alpha": 0.0},
                "metadata": {"strategy_ids": ["vol_compression_v1"]},
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {"symbol": "BTCUSDT", "r_multiple": 0.1},
            {"symbol": "ETHUSDT", "r_multiple": -0.1},
        ]
    ).to_csv(trades_dir / "trades_001.csv", index=False)
    pd.DataFrame([{"equity": 100000.0}, {"equity": 100100.0}]).to_csv(trades_dir / "equity_curve.csv", index=False)

    monkeypatch.setattr(sys, "argv", ["make_report.py", "--run_id", run_id])
    assert make_report.main() == 1


def test_report_engine_fallback_filters_to_strategy_ids(tmp_path: Path, monkeypatch) -> None:
    run_id = "report_engine_fallback_filter"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(make_report, "DATA_ROOT", tmp_path)

    trades_dir = tmp_path / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / run_id
    trades_dir.mkdir(parents=True, exist_ok=True)
    (trades_dir / "metrics.json").write_text(
        json.dumps(
            {
                "total_trades": 1,
                "avg_r": 0.0,
                "win_rate": 0.0,
                "ending_equity": 100000.0,
                "sharpe_annualized": 0.0,
                "cost_decomposition": {"net_alpha": 0.0},
                "metadata": {"strategy_ids": ["vol_compression_v1"]},
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame([{"equity": 100000.0}, {"equity": 100050.0}]).to_csv(trades_dir / "equity_curve.csv", index=False)

    engine_dir = tmp_path / "runs" / run_id / "engine"
    engine_dir.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=4, freq="15min", tz="UTC"),
            "symbol": ["BTCUSDT"] * 4,
            "pos": [0, 1, 1, 0],
        }
    )
    frame.to_csv(engine_dir / "strategy_returns_vol_compression_v1.csv", index=False)
    frame.to_csv(engine_dir / "strategy_returns_stale_strategy_v1.csv", index=False)

    monkeypatch.setattr(sys, "argv", ["make_report.py", "--run_id", run_id])
    assert make_report.main() == 0

    summary = json.loads(
        (tmp_path / "reports" / "vol_compression_expansion_v1" / run_id / "summary.json").read_text(encoding="utf-8")
    )
    assert summary["trade_evidence_source"] == "engine_fallback"
    assert summary["strategy_files_considered"] == ["strategy_returns_vol_compression_v1.csv"]
    assert summary["unexpected_strategy_files_detected"] is True
    assert summary["integrity_checks"]["unexpected_strategy_files_detected"] is True


def test_report_uses_trades_csv_when_present(tmp_path: Path, monkeypatch) -> None:
    run_id = "report_prefers_trades_csv"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(make_report, "DATA_ROOT", tmp_path)

    trades_dir = tmp_path / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / run_id
    trades_dir.mkdir(parents=True, exist_ok=True)
    (trades_dir / "metrics.json").write_text(
        json.dumps(
            {
                "total_trades": 2,
                "avg_r": 0.0,
                "win_rate": 0.0,
                "ending_equity": 100000.0,
                "sharpe_annualized": 0.0,
                "cost_decomposition": {"net_alpha": 0.0},
                "metadata": {"strategy_ids": ["vol_compression_v1"]},
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {"symbol": "BTCUSDT", "r_multiple": 0.2},
            {"symbol": "ETHUSDT", "r_multiple": -0.2},
        ]
    ).to_csv(trades_dir / "trades_001.csv", index=False)
    pd.DataFrame([{"equity": 100000.0}, {"equity": 100010.0}]).to_csv(trades_dir / "equity_curve.csv", index=False)

    engine_dir = tmp_path / "runs" / run_id / "engine"
    engine_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=4, freq="15min", tz="UTC"),
            "symbol": ["BTCUSDT"] * 4,
            "pos": [0, 1, 1, 0],
        }
    ).to_csv(engine_dir / "strategy_returns_stale_strategy_v1.csv", index=False)

    monkeypatch.setattr(sys, "argv", ["make_report.py", "--run_id", run_id])
    assert make_report.main() == 0
    summary = json.loads(
        (tmp_path / "reports" / "vol_compression_expansion_v1" / run_id / "summary.json").read_text(encoding="utf-8")
    )
    assert summary["trade_evidence_source"] == "trades_csv"
    assert summary["integrity_checks"]["trade_files_found"] is True
