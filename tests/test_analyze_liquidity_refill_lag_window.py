import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _write_feature_partitions(base: Path, symbol: str) -> None:
    ts = pd.date_range("2024-01-01", periods=1400, freq="15min", tz="UTC")
    close = np.full(len(ts), 100.0)
    for i in range(1, len(ts)):
        close[i] = close[i - 1] * (1.0 + 0.0001)

    high = close * 1.001
    low = close * 0.999

    # synthetic impulse + refill-lag patches
    impulse_idx = [300, 700, 1100]
    for idx in impulse_idx:
        close[idx] = close[idx - 1] * 1.03
        high[idx : idx + 8] = close[idx : idx + 8] * 1.012
        low[idx : idx + 8] = close[idx : idx + 8] * 0.988

    frame = pd.DataFrame({"timestamp": ts, "open": close, "high": high, "low": low, "close": close, "volume": 1.0})

    out = base / "lake" / "features" / "perp" / symbol / "15m" / "features_v1"
    out.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(out / "part-000.parquet", index=False)


def test_liquidity_refill_lag_analyzer_outputs_phase1_artifacts(tmp_path: Path) -> None:
    run_id = "liquidity_refill_lag_test"
    _write_feature_partitions(tmp_path, "BTCUSDT")

    env = dict(**__import__("os").environ)
    env["BACKTEST_DATA_ROOT"] = str(tmp_path)
    out_dir = tmp_path / "reports" / "liquidity_refill_lag_window" / run_id

    cmd = [
        sys.executable,
        str(ROOT / "project" / "pipelines" / "research" / "analyze_liquidity_refill_lag_window.py"),
        "--run_id",
        run_id,
        "--symbols",
        "BTCUSDT",
        "--window_end",
        "48",
        "--impulse_quantile",
        "0.995",
        "--refill_horizon",
        "8",
        "--refill_mult",
        "1.2",
        "--out_dir",
        str(out_dir),
    ]
    subprocess.run(cmd, check=True, env=env)

    assert (out_dir / "liquidity_refill_lag_window_events.csv").exists()
    assert (out_dir / "liquidity_refill_lag_window_controls.csv").exists()
    assert (out_dir / "liquidity_refill_lag_window_matched_deltas.csv").exists()
    assert (out_dir / "liquidity_refill_lag_window_hazards.csv").exists()
    assert (out_dir / "liquidity_refill_lag_window_phase_stability.csv").exists()
    assert (out_dir / "liquidity_refill_lag_window_sign_stability.csv").exists()
    assert (out_dir / "liquidity_refill_lag_window_summary.json").exists()
    assert (out_dir / "liquidity_refill_lag_window_summary.md").exists()

    summary = json.loads((out_dir / "liquidity_refill_lag_window_summary.json").read_text())
    assert summary["event_type"] == "liquidity_refill_lag_window"
    assert summary["actions_generated"] == 0
