import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _write_feature_partitions(base: Path, symbol: str) -> None:
    ts = pd.date_range("2024-01-01", periods=1800, freq="15min", tz="UTC")
    close = np.full(len(ts), 100.0)
    for i in range(1, len(ts)):
        close[i] = close[i - 1] * (1.0 + 0.0001)

    high = close * 1.002
    low = close * 0.998
    for idx in [320, 760, 1180, 1520]:
        high[idx - 20 : idx] = close[idx - 20 : idx] * 1.0005
        low[idx - 20 : idx] = close[idx - 20 : idx] * 0.9995
        high[idx : idx + 8] = close[idx : idx + 8] * 1.015
        low[idx : idx + 8] = close[idx : idx + 8] * 0.985

    frame = pd.DataFrame(
        {
            "timestamp": ts,
            "open": close,
            "high": high,
            "low": low,
            "close": close,
            "volume": 1.0,
        }
    )

    out = base / "lake" / "features" / "perp" / symbol / "15m" / "features_v1"
    out.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(out / "part-000.parquet", index=False)


def test_range_compression_breakout_analyzer_outputs_phase1_artifacts(tmp_path: Path) -> None:
    run_id = "range_compression_breakout_test"
    _write_feature_partitions(tmp_path, "BTCUSDT")

    env = dict(**__import__("os").environ)
    env["BACKTEST_DATA_ROOT"] = str(tmp_path)
    out_dir = tmp_path / "reports" / "range_compression_breakout_window" / run_id

    cmd = [
        sys.executable,
        str(ROOT / "project" / "pipelines" / "research" / "analyze_range_compression_breakout_window.py"),
        "--run_id",
        run_id,
        "--symbols",
        "BTCUSDT",
        "--window_end",
        "96",
        "--compression_quantile",
        "0.25",
        "--out_dir",
        str(out_dir),
    ]
    subprocess.run(cmd, check=True, env=env)

    assert (out_dir / "range_compression_breakout_window_events.csv").exists()
    assert (out_dir / "range_compression_breakout_window_controls.csv").exists()
    assert (out_dir / "range_compression_breakout_window_matched_deltas.csv").exists()
    assert (out_dir / "range_compression_breakout_window_hazards.csv").exists()
    assert (out_dir / "range_compression_breakout_window_phase_stability.csv").exists()
    assert (out_dir / "range_compression_breakout_window_sign_stability.csv").exists()
    assert (out_dir / "range_compression_breakout_window_summary.json").exists()
    assert (out_dir / "range_compression_breakout_window_summary.md").exists()

    summary = json.loads((out_dir / "range_compression_breakout_window_summary.json").read_text())
    assert summary["event_type"] == "range_compression_breakout_window"
    assert summary["actions_generated"] == 0

    events = pd.read_csv(out_dir / "range_compression_breakout_window_events.csv")
    assert not events.empty
    assert "compression_ratio" in events.columns
