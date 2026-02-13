import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from engine import runner


def _write_cleaned_bars(root: Path, run_id: str, symbol: str, start: str, periods: int) -> None:
    ts = pd.date_range(start, periods=periods, freq="15min", tz="UTC")
    bars = pd.DataFrame(
        {
            "timestamp": ts,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 1.0,
            "is_gap": False,
            "gap_len": 0,
        }
    )
    out = root / "lake" / "runs" / run_id / "cleaned" / "perp" / symbol / "bars_15m" / "year=2024" / "month=01"
    out.mkdir(parents=True, exist_ok=True)
    bars.to_csv(out / f"bars_{symbol}_15m_2024-01.csv", index=False)


def test_build_universe_snapshots_and_eligibility_mask(tmp_path: Path) -> None:
    run_id = "u_snap"
    symbol = "BTCUSDT"
    _write_cleaned_bars(tmp_path, run_id, symbol, "2024-01-01", 20)

    env = os.environ.copy()
    env["BACKTEST_DATA_ROOT"] = str(tmp_path)

    cmd = [
        sys.executable,
        str(ROOT / "project" / "pipelines" / "ingest" / "build_universe_snapshots.py"),
        "--run_id",
        run_id,
        "--symbols",
        symbol,
        "--market",
        "perp",
    ]
    subprocess.run(cmd, check=True, env=env)

    manifest = json.loads((tmp_path / "runs" / run_id / "build_universe_snapshots.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "success"

    snapshots = runner._load_universe_snapshots(tmp_path, run_id)
    assert not snapshots.empty

    timestamps = pd.Series(pd.to_datetime(["2023-12-01T00:00:00Z", "2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z"]))
    mask = runner._symbol_eligibility_mask(timestamps, symbol, snapshots)
    assert mask.tolist() == [False, True, False]
