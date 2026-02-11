import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines._lib.io_utils import list_parquet_files, read_parquet
from pipelines.ingest import ingest_binance_um_funding


def test_funding_api_fallback_is_persisted(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(ingest_binance_um_funding, "DATA_ROOT", tmp_path)

    def _fake_download(*args, **kwargs):
        return SimpleNamespace(status="not_found", path=None, error=None)

    def _fake_fetch_api(session, base_url, symbol, start, end_exclusive, limit, sleep_sec):
        ts = pd.to_datetime(
            [
                "2024-01-01 00:00:00+00:00",
                "2024-01-01 08:00:00+00:00",
                "2024-01-01 16:00:00+00:00",
            ]
        )
        frame = pd.DataFrame(
            {
                "timestamp": ts,
                "funding_rate": [0.0001, 0.0002, 0.0003],
                "symbol": [symbol] * 3,
                "source": ["api"] * 3,
            }
        )
        return frame, 1

    monkeypatch.setattr(ingest_binance_um_funding, "download_with_retries", _fake_download)
    monkeypatch.setattr(ingest_binance_um_funding, "_fetch_funding_api", _fake_fetch_api)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ingest_binance_um_funding.py",
            "--run_id",
            "funding_api_fallback",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-01",
            "--max_retries",
            "0",
            "--retry_backoff_sec",
            "0",
            "--use_api_fallback",
            "1",
        ],
    )
    assert ingest_binance_um_funding.main() == 0

    funding_dir = tmp_path / "lake" / "raw" / "binance" / "perp" / "BTCUSDT" / "funding"
    files = list_parquet_files(funding_dir)
    assert files, "expected persisted funding partitions from API fallback"

    saved = read_parquet(files)
    saved["timestamp"] = pd.to_datetime(saved["timestamp"], utc=True)
    assert len(saved) == 3
    assert set(saved["source"].astype(str)) == {"api"}
    assert set(saved["timestamp"]) == set(
        pd.to_datetime(
            [
                "2024-01-01 00:00:00+00:00",
                "2024-01-01 08:00:00+00:00",
                "2024-01-01 16:00:00+00:00",
            ]
        )
    )

    manifest = json.loads((tmp_path / "runs" / "funding_api_fallback" / "ingest_binance_um_funding.json").read_text())
    stats = manifest["stats"]["symbols"]["BTCUSDT"]
    assert stats["got_count"] == 3
    assert stats["missing_count"] == 0
    assert stats["api_calls"] == 1
    assert stats["partitions_written"]
