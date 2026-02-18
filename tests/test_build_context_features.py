import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from features.funding_persistence import DEFAULT_FP_CONFIG
from pipelines.features import build_context_features


def _write_cleaned_fixture(tmp_path: Path, run_id: str, symbol: str) -> None:
    ts = pd.date_range("2021-01-01 00:00", periods=192, freq="15min", tz="UTC")
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
    funding = pd.DataFrame({"timestamp": ts, "funding_rate_scaled": 0.0001})

    bars_dir = (
        tmp_path
        / "lake"
        / "runs"
        / run_id
        / "cleaned"
        / "perp"
        / symbol
        / "bars_15m"
        / "year=2021"
        / "month=01"
    )
    funding_dir = (
        tmp_path
        / "lake"
        / "runs"
        / run_id
        / "cleaned"
        / "perp"
        / symbol
        / "funding_15m"
        / "year=2021"
        / "month=01"
    )
    bars_dir.mkdir(parents=True, exist_ok=True)
    funding_dir.mkdir(parents=True, exist_ok=True)
    bars.to_parquet(bars_dir / "bars_BTCUSDT_15m_2021-01.parquet", index=False)
    funding.to_parquet(funding_dir / "funding15m_BTCUSDT_2021-01.parquet", index=False)


def _write_cleaned_fixture_with_bar_duplicates(tmp_path: Path, run_id: str, symbol: str) -> None:
    ts = pd.date_range("2021-01-01 00:00", periods=12, freq="15min", tz="UTC")
    bars = pd.DataFrame(
        {
            "timestamp": list(ts) + [ts[3], ts[7]],
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 1.0,
            "is_gap": False,
            "gap_len": 0,
        }
    )
    funding = pd.DataFrame({"timestamp": ts, "funding_rate_scaled": 0.0001})
    bars_dir = (
        tmp_path
        / "lake"
        / "runs"
        / run_id
        / "cleaned"
        / "perp"
        / symbol
        / "bars_15m"
        / "year=2021"
        / "month=01"
    )
    funding_dir = (
        tmp_path
        / "lake"
        / "runs"
        / run_id
        / "cleaned"
        / "perp"
        / symbol
        / "funding_15m"
        / "year=2021"
        / "month=01"
    )
    bars_dir.mkdir(parents=True, exist_ok=True)
    funding_dir.mkdir(parents=True, exist_ok=True)
    bars.to_parquet(bars_dir / "bars_BTCUSDT_15m_2021-01.parquet", index=False)
    funding.to_parquet(funding_dir / "funding15m_BTCUSDT_2021-01.parquet", index=False)


def test_context_build_end_date_is_inclusive_day(monkeypatch, tmp_path: Path) -> None:
    run_id = "ctx_date_fix"
    symbol = "BTCUSDT"
    _write_cleaned_fixture(tmp_path, run_id=run_id, symbol=symbol)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_context_features, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_context_features.py",
            "--run_id",
            run_id,
            "--symbols",
            symbol,
            "--timeframe",
            "15m",
            "--start",
            "2021-01-01",
            "--end",
            "2021-01-02",
            "--force",
            "1",
        ],
    )

    assert build_context_features.main() == 0

    output_path = tmp_path / "lake" / "runs" / run_id / "context" / "funding_persistence" / symbol / "15m.parquet"
    assert output_path.exists()
    out = pd.read_parquet(output_path)
    assert len(out) == 192

    manifest_path = tmp_path / "runs" / run_id / "build_context_features.json"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["parameters"]["end_exclusive"] == "2021-01-03T00:00:00+00:00"
    assert manifest["stats"]["symbols"][symbol]["rows"] == 192


def test_context_skip_requires_timestamp_and_version_match(monkeypatch, tmp_path: Path) -> None:
    run_id = "ctx_skip_guard"
    symbol = "BTCUSDT"
    _write_cleaned_fixture(tmp_path, run_id=run_id, symbol=symbol)

    output_path = tmp_path / "lake" / "runs" / run_id / "context" / "funding_persistence" / symbol / "15m.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ts_stale = pd.date_range("2021-04-01 00:00", periods=192, freq="15min", tz="UTC")
    stale = pd.DataFrame(
        {
            "timestamp": ts_stale,
            "fp_def_version": ["stale"] * len(ts_stale),
            "fp_active": [0] * len(ts_stale),
            "fp_age_bars": [0] * len(ts_stale),
            "fp_event_id": [None] * len(ts_stale),
            "fp_enter_ts": [pd.NaT] * len(ts_stale),
            "fp_exit_ts": [pd.NaT] * len(ts_stale),
            "fp_severity": [0.0] * len(ts_stale),
            "fp_norm_due": [0] * len(ts_stale),
        }
    )
    stale.to_parquet(output_path, index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_context_features, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_context_features.py",
            "--run_id",
            run_id,
            "--symbols",
            symbol,
            "--timeframe",
            "15m",
            "--start",
            "2021-01-01",
            "--end",
            "2021-01-02",
        ],
    )
    assert build_context_features.main() == 0

    out = pd.read_parquet(output_path)
    assert out["timestamp"].min() == pd.Timestamp("2021-01-01 00:00:00+00:00")
    assert out["fp_def_version"].dropna().unique().tolist() == [DEFAULT_FP_CONFIG.def_version]


def test_context_build_dedupes_duplicate_bar_timestamps(monkeypatch, tmp_path: Path) -> None:
    run_id = "ctx_dedupe_bars"
    symbol = "BTCUSDT"
    _write_cleaned_fixture_with_bar_duplicates(tmp_path, run_id=run_id, symbol=symbol)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_context_features, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_context_features.py",
            "--run_id",
            run_id,
            "--symbols",
            symbol,
            "--timeframe",
            "15m",
            "--start",
            "2021-01-01",
            "--end",
            "2021-01-01",
            "--force",
            "1",
        ],
    )
    assert build_context_features.main() == 0

    output_path = tmp_path / "lake" / "runs" / run_id / "context" / "funding_persistence" / symbol / "15m.parquet"
    out = pd.read_parquet(output_path)
    assert len(out) == 12
    assert not pd.Series(out["timestamp"]).duplicated().any()

    manifest_path = tmp_path / "runs" / run_id / "build_context_features.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["stats"]["symbols"][symbol]["bars_duplicate_rows_dropped"] == 2
