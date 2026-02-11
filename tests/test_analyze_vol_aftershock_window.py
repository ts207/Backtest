import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _write_feature_partitions(base: Path, symbol: str) -> None:
    ts = pd.date_range("2024-01-01", periods=1000, freq="15min", tz="UTC")
    close = np.full(len(ts), 100.0)
    for i in range(1, len(ts)):
        close[i] = close[i - 1] * (1.0 + 0.0002)
    for i in range(330, 352):
        close[i] = close[i - 1] * (1.0 + (0.012 if i % 2 == 0 else -0.011))
    for i in range(650, 668):
        close[i] = close[i - 1] * (1.0 + (0.013 if i % 2 == 0 else -0.012))

    frame = pd.DataFrame(
        {
            "timestamp": ts,
            "open": close,
            "high": close * 1.002,
            "low": close * 0.998,
            "close": close,
            "volume": 1.0,
        }
    )

    out = base / "lake" / "features" / "perp" / symbol / "15m" / "features_v1"
    out.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(out / "part-000.parquet", index=False)


def test_aftershock_analyzer_outputs_phase1_artifacts(tmp_path: Path) -> None:
    run_id = "aftershock_test"
    _write_feature_partitions(tmp_path, "BTCUSDT")

    env = dict(**__import__("os").environ)
    env["BACKTEST_DATA_ROOT"] = str(tmp_path)
    out_dir = tmp_path / "reports" / "vol_aftershock_window" / run_id

    cmd = [
        sys.executable,
        str(ROOT / "project" / "pipelines" / "research" / "analyze_vol_aftershock_window.py"),
        "--run_id",
        run_id,
        "--symbols",
        "BTCUSDT",
        "--window_start",
        "0",
        "--window_end",
        "96",
        "--out_dir",
        str(out_dir),
    ]
    subprocess.run(cmd, check=True, env=env)

    assert (out_dir / "vol_aftershock_window_events.csv").exists()
    assert (out_dir / "vol_aftershock_window_controls.csv").exists()
    assert (out_dir / "vol_aftershock_window_matched_deltas.csv").exists()
    assert (out_dir / "vol_aftershock_window_hazards.csv").exists()
    assert (out_dir / "vol_aftershock_window_phase_stability.csv").exists()
    assert (out_dir / "vol_aftershock_window_sign_stability.csv").exists()
    assert (out_dir / "vol_aftershock_window_conditional_hazards.csv").exists()
    assert (out_dir / "vol_aftershock_window_window_sensitivity.csv").exists()
    assert (out_dir / "vol_aftershock_window_placebo_deltas.csv").exists()
    assert (out_dir / "vol_aftershock_window_re_risk_note.md").exists()

    summary = json.loads((out_dir / "vol_aftershock_window_summary.json").read_text())
    assert summary["event_type"] == "vol_aftershock_window"
    assert summary["actions_generated"] == 0
    sweep = {(int(w["x"]), int(w["y"])) for w in summary["window_sweep"]}
    assert {(0, 48), (24, 96), (48, 144)}.issubset(sweep)
