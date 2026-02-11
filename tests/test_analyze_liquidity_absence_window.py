import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _write_feature_partitions(base: Path, symbol: str) -> None:
    ts = pd.date_range("2024-01-01", periods=1200, freq="15min", tz="UTC")
    close = np.full(len(ts), 100.0)
    for i in range(1, len(ts)):
        close[i] = close[i - 1] * (1.0 + 0.00015)
    frame = pd.DataFrame(
        {
            "timestamp": ts,
            "open": close,
            "high": close * 1.001,
            "low": close * 0.999,
            "close": close,
            "volume": 10.0,
        }
    )
    # create low-liquidity anchors at 00:00 UTC bars
    mask = (frame["timestamp"].dt.hour == 0) & (frame["timestamp"].dt.minute == 0)
    frame.loc[mask, "volume"] = 0.5

    out = base / "lake" / "features" / "perp" / symbol / "15m" / "features_v1"
    out.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(out / "part-000.parquet", index=False)


def test_liquidity_absence_analyzer_outputs_phase1_artifacts(tmp_path: Path) -> None:
    run_id = "liquidity_absence_test"
    _write_feature_partitions(tmp_path, "BTCUSDT")

    env = dict(**__import__("os").environ)
    env["BACKTEST_DATA_ROOT"] = str(tmp_path)
    out_dir = tmp_path / "reports" / "liquidity_absence_window" / run_id

    cmd = [
        sys.executable,
        str(ROOT / "project" / "pipelines" / "research" / "analyze_liquidity_absence_window.py"),
        "--run_id",
        run_id,
        "--symbols",
        "BTCUSDT",
        "--window_end",
        "96",
        "--absence_quantile",
        "0.3",
        "--out_dir",
        str(out_dir),
    ]
    subprocess.run(cmd, check=True, env=env)

    assert (out_dir / "liquidity_absence_window_events.csv").exists()
    assert (out_dir / "liquidity_absence_window_controls.csv").exists()
    assert (out_dir / "liquidity_absence_window_matched_deltas.csv").exists()
    assert (out_dir / "liquidity_absence_window_hazards.csv").exists()
    assert (out_dir / "liquidity_absence_window_phase_stability.csv").exists()
    assert (out_dir / "liquidity_absence_window_sign_stability.csv").exists()
    assert (out_dir / "liquidity_absence_window_interpretation.md").exists()

    summary = json.loads((out_dir / "liquidity_absence_window_summary.json").read_text())
    assert summary["event_type"] == "liquidity_absence_window"
    assert summary["actions_generated"] == 0
