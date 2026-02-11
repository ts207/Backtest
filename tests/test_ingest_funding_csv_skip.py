import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.ingest import ingest_binance_um_funding as funding


def test_funding_skip_path_reads_csv_partition(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(funding, "DATA_ROOT", tmp_path)
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))

    # Existing partition only in CSV form (parquet absent).
    out_dir = (
        tmp_path
        / "lake"
        / "raw"
        / "binance"
        / "perp"
        / "BTCUSDT"
        / "funding"
        / "year=2020"
        / "month=06"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "funding_BTCUSDT_2020-06.csv"

    # For start=end=2020-06-01, expected funding events are 00:00, 08:00, 16:00 UTC.
    existing = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2020-06-01T00:00:00Z",
                    "2020-06-01T08:00:00Z",
                    "2020-06-01T16:00:00Z",
                ],
                utc=True,
            ),
            "funding_rate": [0.0001, 0.0001, 0.0001],
            "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT"],
            "source": ["archive_monthly", "archive_monthly", "archive_monthly"],
        }
    )
    existing.to_csv(csv_path, index=False)

    # No network path should be exercised because partition is complete.
    def _fail_download(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("download_with_retries should not be called for complete CSV partition")

    def _fail_api(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("API fallback should not be called when archive coverage is complete")

    monkeypatch.setattr(funding, "download_with_retries", _fail_download)
    monkeypatch.setattr(funding, "_fetch_funding_api", _fail_api)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ingest_binance_um_funding.py",
            "--run_id",
            "funding_csv_skip",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2020-06-01",
            "--end",
            "2020-06-01",
        ],
    )

    assert funding.main() == 0

    manifest_path = tmp_path / "runs" / "funding_csv_skip" / "ingest_binance_um_funding.json"
    assert manifest_path.exists()
