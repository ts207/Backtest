import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _write_feature_partitions(base: Path, symbol: str) -> None:
    ts = pd.date_range("2024-01-01", periods=1600, freq="15min", tz="UTC")
    close = np.full(len(ts), 100.0)
    for i in range(1, len(ts)):
        close[i] = close[i - 1] * (1.0 + 0.00012)

    funding = 0.0002 * np.sin(np.linspace(0, 18 * np.pi, len(ts)))
    spike_idx = [300, 620, 940, 1260, 1500]
    for j, idx in enumerate(spike_idx):
        funding[idx] = 0.01 if j % 2 == 0 else -0.01
        close[idx : idx + 5] = close[idx : idx + 5] * (1.015 if funding[idx] < 0 else 0.985)

    frame = pd.DataFrame(
        {
            "timestamp": ts,
            "open": close,
            "high": close * 1.002,
            "low": close * 0.998,
            "close": close,
            "funding_rate_scaled": funding,
            "volume": 1.0,
        }
    )

    out = base / "lake" / "features" / "perp" / symbol / "15m" / "features_v1"
    out.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(out / "part-000.parquet", index=False)


def test_funding_extreme_reversal_analyzer_outputs_phase1_artifacts(tmp_path: Path) -> None:
    run_id = "funding_extreme_reversal_test"
    _write_feature_partitions(tmp_path, "BTCUSDT")

    env = dict(**__import__("os").environ)
    env["BACKTEST_DATA_ROOT"] = str(tmp_path)
    out_dir = tmp_path / "reports" / "funding_extreme_reversal_window" / run_id

    cmd = [
        sys.executable,
        str(ROOT / "project" / "pipelines" / "research" / "analyze_funding_extreme_reversal_window.py"),
        "--run_id",
        run_id,
        "--symbols",
        "BTCUSDT",
        "--window_end",
        "96",
        "--funding_abs_quantile",
        "0.995",
        "--out_dir",
        str(out_dir),
    ]
    subprocess.run(cmd, check=True, env=env)

    assert (out_dir / "funding_extreme_reversal_window_events.csv").exists()
    assert (out_dir / "funding_extreme_reversal_window_controls.csv").exists()
    assert (out_dir / "funding_extreme_reversal_window_matched_deltas.csv").exists()
    assert (out_dir / "funding_extreme_reversal_window_hazards.csv").exists()
    assert (out_dir / "funding_extreme_reversal_window_phase_stability.csv").exists()
    assert (out_dir / "funding_extreme_reversal_window_sign_stability.csv").exists()
    assert (out_dir / "funding_extreme_reversal_window_summary.json").exists()
    assert (out_dir / "funding_extreme_reversal_window_summary.md").exists()

    summary = json.loads((out_dir / "funding_extreme_reversal_window_summary.json").read_text())
    assert summary["event_type"] == "funding_extreme_reversal_window"
    assert summary["actions_generated"] == 0

    events = pd.read_csv(out_dir / "funding_extreme_reversal_window_events.csv")
    assert not events.empty
    assert "funding_sign" in events.columns
