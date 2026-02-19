import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.backtest import backtest_strategies as bts


def test_cost_components_from_frame_decomposes_gross_and_costs() -> None:
    frame = pd.DataFrame(
        {
            "pos": [0, 1, 1, 0],
            "ret": [0.0, 0.01, -0.02, 0.03],
        }
    )
    out = bts._cost_components_from_frame(frame, fee_bps=4.0, slippage_bps=2.0)
    assert round(out["gross_alpha"], 10) == round(0.01, 10)
    assert round(out["fees"], 10) == round(2.0 * 4.0 / 10000.0, 10)
    assert round(out["slippage"], 10) == round(2.0 * 2.0 / 10000.0, 10)
    assert out["impact"] == 0.0
    assert round(out["net_alpha"], 10) == round(0.01 - (2.0 * 6.0 / 10000.0), 10)


def test_build_reproducibility_metadata_includes_digest_revision_and_snapshots(monkeypatch, tmp_path: Path) -> None:
    run_id = "repro_run"
    runs_dir = tmp_path / "runs" / run_id
    runs_dir.mkdir(parents=True, exist_ok=True)
    (runs_dir / "build_cleaned_15m.json").write_text(json.dumps({"ok": 1}), encoding="utf-8")
    (runs_dir / "build_features_v1.json").write_text(json.dumps({"ok": 2}), encoding="utf-8")

    def _fake_run(*args, **kwargs):
        return SimpleNamespace(stdout="deadbeef\n")

    monkeypatch.setattr(bts.subprocess, "run", _fake_run)

    meta = bts._build_reproducibility_metadata(
        run_id=run_id,
        config={"fee_bps_per_side": 4},
        params={"symbols": ["BTCUSDT"], "strategies": ["vol_compression_v1"]},
        data_root=tmp_path,
        project_root=ROOT,
    )
    assert meta["code_revision"] == "deadbeef"
    assert len(meta["config_digest"]) == 64
    assert "build_cleaned_15m" in meta["data_snapshot_ids"]
    assert "build_features_v1" in meta["data_snapshot_ids"]


def test_backtest_main_writes_cost_decomposition_and_reproducibility(monkeypatch, tmp_path: Path) -> None:
    run_id = "bt_meta_runtime"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(bts, "DATA_ROOT", tmp_path)

    # Inputs used by reproducibility snapshot hashing.
    runs_dir = tmp_path / "runs" / run_id
    runs_dir.mkdir(parents=True, exist_ok=True)
    (runs_dir / "build_cleaned_15m.json").write_text(json.dumps({"ok": 1}), encoding="utf-8")

    def _fake_engine(**kwargs):
        engine_dir = tmp_path / "runs" / run_id / "engine"
        engine_dir.mkdir(parents=True, exist_ok=True)
        frame = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T00:15:00Z"]),
                "symbol": ["BTCUSDT", "BTCUSDT"],
                "pos": [0, 1],
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
        return {"engine_dir": engine_dir, "strategy_frames": {"vol_compression_v1": frame}, "portfolio": portfolio}

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
            "--strategies",
            "vol_compression_v1",
            "--force",
            "1",
        ],
    )
    assert bts.main() == 0

    metrics_path = tmp_path / "lake" / "trades" / "backtests" / "breakout" / run_id / "metrics.json"
    payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert "cost_decomposition" in payload
    assert "reproducibility" in payload
    assert set(["gross_alpha", "fees", "slippage", "impact", "funding_pnl", "borrow_cost", "net_alpha"]).issubset(payload["cost_decomposition"].keys())
    assert "config_digest" in payload["reproducibility"]


@pytest.mark.parametrize(
    "strategy_id,expected_family",
    [
        ("funding_extreme_reversal_v1", "carry"),
        ("cross_venue_desync_v1", "spread"),
        ("dsl_interpreter_v1__range_compression_breakout_window__btc", "dsl"),
    ],
)
def test_backtest_metadata_records_execution_family_without_breakout_only_config(
    monkeypatch,
    tmp_path: Path,
    strategy_id: str,
    expected_family: str,
) -> None:
    run_id = f"bt_exec_family_{strategy_id}"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(bts, "DATA_ROOT", tmp_path)

    runs_dir = tmp_path / "runs" / run_id
    runs_dir.mkdir(parents=True, exist_ok=True)
    (runs_dir / "build_cleaned_15m.json").write_text(json.dumps({"ok": 1}), encoding="utf-8")

    def _fake_engine(**kwargs):
        assert kwargs["strategies"] == [strategy_id]
        engine_dir = tmp_path / "runs" / run_id / "engine"
        engine_dir.mkdir(parents=True, exist_ok=True)
        frame = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T00:15:00Z"]),
                "symbol": ["BTCUSDT", "BTCUSDT"],
                "pos": [0, 1],
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
        return {"engine_dir": engine_dir, "strategy_frames": {strategy_id: frame}, "portfolio": portfolio}

    monkeypatch.setattr(bts, "run_engine", _fake_engine)
    monkeypatch.setattr(
        bts,
        "resolve_execution_costs",
        lambda **_: SimpleNamespace(
            config={
                "fee_bps_per_side": 4,
                "slippage_bps_per_fill": 2,
                "trade_day_timezone": "UTC",
            },
            fee_bps_per_side=4.0,
            slippage_bps_per_fill=2.0,
            cost_bps=6.0,
            execution_model={},
            config_digest="digest",
        ),
    )

    cfg = tmp_path / "family.yaml"
    cfg.write_text(
        """
strategy_family_params:
  Carry:
    funding_percentile_entry_min: 96.0
    funding_percentile_entry_max: 99.0
strategy_families:
  funding_extreme_reversal_v1: Carry
""",
        encoding="utf-8",
    )

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
            "--strategies",
            strategy_id,
            "--config",
            str(cfg),
            "--force",
            "1",
        ],
    )

    assert bts.main() == 0

    metrics_path = tmp_path / "lake" / "trades" / "backtests" / expected_family / run_id / "metrics.json"
    payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert payload["metadata"]["execution_family"] == expected_family
    assert payload["metadata"]["strategy_ids"] == [strategy_id]


def test_cost_components_include_funding_and_borrow_in_net_alpha() -> None:
    frame = pd.DataFrame(
        {
            "pos": [0, -1, -1, 0],
            "ret": [0.0, 0.01, -0.02, 0.0],
            "funding_pnl": [0.0, 0.001, 0.001, 0.0],
            "borrow_cost": [0.0, 0.0002, 0.0002, 0.0],
        }
    )
    out = bts._cost_components_from_frame(frame, fee_bps=0.0, slippage_bps=0.0)
    assert round(out["funding_pnl"], 10) == 0.002
    assert round(out["borrow_cost"], 10) == 0.0004
    assert round(out["net_alpha"], 10) == round(out["gross_alpha"] + out["funding_pnl"] - out["borrow_cost"], 10)


def test_cost_components_with_scaled_returns_avoids_double_scaling() -> None:
    frame = pd.DataFrame(
        {
            "pos": [0, 1, 1, 0],
            "position_scale": [0.5, 0.5, 0.5, 0.5],
            "ret": [0.0, 0.005, -0.01, 0.0],  # scaled returns
        }
    )
    out = bts._cost_components_from_frame(frame, fee_bps=4.0, slippage_bps=2.0)
    assert round(out["gross_alpha"], 10) == round(-0.01, 10)
    assert round(out["fees"], 10) == round(1.0 * 4.0 / 10000.0, 10)
    assert round(out["slippage"], 10) == round(1.0 * 2.0 / 10000.0, 10)


def test_backtest_force_cleans_engine_artifacts(monkeypatch, tmp_path: Path) -> None:
    run_id = "bt_clean_force"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(bts, "DATA_ROOT", tmp_path)

    runs_dir = tmp_path / "runs" / run_id
    runs_dir.mkdir(parents=True, exist_ok=True)
    (runs_dir / "build_cleaned_15m.json").write_text(json.dumps({"ok": 1}), encoding="utf-8")

    engine_dir = runs_dir / "engine"
    engine_dir.mkdir(parents=True, exist_ok=True)
    stale_paths = [
        engine_dir / "strategy_returns_stale.csv",
        engine_dir / "strategy_trace_stale.csv",
        engine_dir / "portfolio_returns.csv",
        engine_dir / "metrics.json",
    ]
    for path in stale_paths:
        path.write_text("stale", encoding="utf-8")

    def _fake_engine(**kwargs):
        frame = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T00:15:00Z"]),
                "symbol": ["BTCUSDT", "BTCUSDT"],
                "pos": [0, 1],
                "ret": [0.0, 0.001],
                "pnl": [0.0, 0.0],
                "close": [100.0, 100.1],
                "high": [100.2, 100.3],
                "low": [99.9, 100.0],
                "high_96": [100.2, 100.3],
                "low_96": [99.9, 100.0],
            }
        )
        portfolio = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T00:15:00Z"]),
                "portfolio_pnl": [0.0, 0.0],
            }
        )
        return {"engine_dir": engine_dir, "strategy_frames": {"vol_compression_v1": frame}, "portfolio": portfolio}

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
            "--strategies",
            "vol_compression_v1",
            "--force",
            "1",
        ],
    )
    assert bts.main() == 0

    for path in stale_paths:
        assert not path.exists()


def test_backtest_clean_override_off_preserves_files(monkeypatch, tmp_path: Path) -> None:
    run_id = "bt_clean_override_off"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(bts, "DATA_ROOT", tmp_path)

    runs_dir = tmp_path / "runs" / run_id
    runs_dir.mkdir(parents=True, exist_ok=True)
    (runs_dir / "build_cleaned_15m.json").write_text(json.dumps({"ok": 1}), encoding="utf-8")

    engine_dir = runs_dir / "engine"
    engine_dir.mkdir(parents=True, exist_ok=True)
    stale_paths = [
        engine_dir / "strategy_returns_stale.csv",
        engine_dir / "strategy_trace_stale.csv",
        engine_dir / "portfolio_returns.csv",
        engine_dir / "metrics.json",
    ]
    for path in stale_paths:
        path.write_text("stale", encoding="utf-8")

    def _fake_engine(**kwargs):
        frame = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T00:15:00Z"]),
                "symbol": ["BTCUSDT", "BTCUSDT"],
                "pos": [0, 1],
                "ret": [0.0, 0.001],
                "pnl": [0.0, 0.0],
                "close": [100.0, 100.1],
                "high": [100.2, 100.3],
                "low": [99.9, 100.0],
                "high_96": [100.2, 100.3],
                "low_96": [99.9, 100.0],
            }
        )
        portfolio = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T00:15:00Z"]),
                "portfolio_pnl": [0.0, 0.0],
            }
        )
        return {"engine_dir": engine_dir, "strategy_frames": {"vol_compression_v1": frame}, "portfolio": portfolio}

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
            "--strategies",
            "vol_compression_v1",
            "--force",
            "1",
            "--clean_engine_artifacts",
            "0",
        ],
    )
    assert bts.main() == 0

    for path in stale_paths:
        assert path.exists()
