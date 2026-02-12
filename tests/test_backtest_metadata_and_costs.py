import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

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

    metrics_path = tmp_path / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / run_id / "metrics.json"
    payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert "cost_decomposition" in payload
    assert "reproducibility" in payload
    assert set(["gross_alpha", "fees", "slippage", "impact", "net_alpha"]).issubset(payload["cost_decomposition"].keys())
    assert "config_digest" in payload["reproducibility"]
