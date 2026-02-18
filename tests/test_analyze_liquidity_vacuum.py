import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _write_cleaned_bars(base: Path, symbol: str) -> None:
    ts = pd.date_range("2023-01-01", periods=700, freq="15min", tz="UTC")
    close = pd.Series(100.0, index=range(len(ts)))
    frame = pd.DataFrame(
        {
            "timestamp": ts,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": 1000.0,
        }
    )
    out = base / "lake" / "cleaned" / "perp" / symbol / "bars_15m"
    out.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(out / "part-000.parquet", index=False)


def test_liquidity_vacuum_writes_events_csv_even_when_empty(tmp_path: Path) -> None:
    run_id = "liquidity_vacuum_empty_events"
    _write_cleaned_bars(tmp_path, "BTCUSDT")

    env = dict(**__import__("os").environ)
    env["BACKTEST_DATA_ROOT"] = str(tmp_path)
    out_dir = tmp_path / "reports" / "liquidity_vacuum" / run_id

    cmd = [
        sys.executable,
        str(ROOT / "project" / "pipelines" / "research" / "analyze_liquidity_vacuum.py"),
        "--run_id",
        run_id,
        "--symbols",
        "BTCUSDT",
        "--out_dir",
        str(out_dir),
    ]
    subprocess.run(cmd, check=True, env=env)

    assert (out_dir / "liquidity_vacuum_events.csv").exists()
    assert (out_dir / "liquidity_vacuum_controls.csv").exists()
    assert (out_dir / "liquidity_vacuum_summary.json").exists()

    summary = json.loads((out_dir / "liquidity_vacuum_summary.json").read_text(encoding="utf-8"))
    assert summary["run_id"] == run_id
    assert summary["symbols"] == ["BTCUSDT"]
