from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.clean import build_cleaned_15m


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _seed_raw(tmp_path: Path, symbol: str, start: str, periods: int) -> None:
    ts = pd.date_range(start, periods=periods, freq="15min", tz="UTC")
    raw = pd.DataFrame(
        {
            "timestamp": ts,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 1.0,
        }
    )
    _write_csv(raw, tmp_path / "lake" / "raw" / "binance" / "perp" / symbol / "ohlcv_15m" / "part.csv")


def test_clean_fails_when_requested_window_has_no_overlap(monkeypatch, tmp_path: Path) -> None:
    run_id = "clean_no_overlap"
    symbol = "BTCUSDT"
    _seed_raw(tmp_path, symbol=symbol, start="2024-01-01", periods=96)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_cleaned_15m, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_cleaned_15m.py",
            "--run_id",
            run_id,
            "--symbols",
            symbol,
            "--start",
            "2024-02-01",
            "--end",
            "2024-02-02",
            "--allow_missing_funding",
            "1",
        ],
    )
    assert build_cleaned_15m.main() == 1
    manifest = json.loads((tmp_path / "runs" / run_id / "build_cleaned_15m.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "failed"
    assert "No overlapping OHLCV window" in str(manifest.get("error", ""))


def test_partition_complete_requires_exact_timestamp_coverage(tmp_path: Path) -> None:
    path = tmp_path / "bars.parquet"
    ts = pd.date_range("2024-01-01 00:15:00", periods=4, freq="15min", tz="UTC")
    _write_csv(pd.DataFrame({"timestamp": ts, "open": 1.0}), path.with_suffix(".csv"))
    ok = build_cleaned_15m._partition_complete(
        path=path,
        expected_rows=4,
        range_start=pd.Timestamp("2024-01-01 00:00:00", tz="UTC").to_pydatetime(),
        range_end_exclusive=pd.Timestamp("2024-01-01 01:00:00", tz="UTC").to_pydatetime(),
    )
    assert ok is False


def test_clean_writes_run_scoped_partitions(monkeypatch, tmp_path: Path) -> None:
    run_id = "clean_run_scoped"
    symbol = "BTCUSDT"
    _seed_raw(tmp_path, symbol=symbol, start="2024-01-01", periods=96)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_cleaned_15m, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_cleaned_15m.py",
            "--run_id",
            run_id,
            "--symbols",
            symbol,
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-01",
            "--allow_missing_funding",
            "1",
            "--force",
            "1",
        ],
    )
    assert build_cleaned_15m.main() == 0
    run_scoped = (
        tmp_path
        / "lake"
        / "runs"
        / run_id
        / "cleaned"
        / "perp"
        / symbol
        / "bars_15m"
        / "year=2024"
        / "month=01"
    )
    assert list(run_scoped.glob("*.csv")) or list(run_scoped.glob("*.parquet"))
