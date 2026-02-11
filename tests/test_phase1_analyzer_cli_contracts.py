import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _run(script_name: str, run_id: str, data_root: Path) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["BACKTEST_DATA_ROOT"] = str(data_root)
    cmd = [
        sys.executable,
        str(ROOT / "project" / "pipelines" / "research" / script_name),
        "--run_id",
        run_id,
        "--symbols",
        "BTCUSDT",
        "--log_path",
        str(data_root / "runs" / run_id / f"{script_name}.log"),
    ]
    return subprocess.run(cmd, env=env, capture_output=True, text=True)


def test_cross_venue_desync_accepts_log_path(tmp_path: Path) -> None:
    run_id = "phase1_cross_cli"
    proc = _run("analyze_cross_venue_desync.py", run_id, tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "reports" / "cross_venue_desync" / run_id / "cross_venue_desync_summary.json").exists()


def test_directional_exhaustion_accepts_log_path(tmp_path: Path) -> None:
    run_id = "phase1_directional_cli"
    proc = _run("analyze_directional_exhaustion_after_forced_flow.py", run_id, tmp_path)
    assert proc.returncode == 0, proc.stderr
    out = tmp_path / "reports" / "directional_exhaustion_after_forced_flow" / run_id
    assert (out / "directional_exhaustion_after_forced_flow_summary.json").exists()
