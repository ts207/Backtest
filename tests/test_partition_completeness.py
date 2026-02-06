import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.features.build_features_v1 import _partition_complete as features_partition_complete
from pipelines.ingest.ingest_binance_um_ohlcv_15m import _expected_15m_index, _partition_complete as ingest_partition_complete


def _write_csv_partition(parquet_path: Path, timestamps: list[pd.Timestamp]) -> None:
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path = parquet_path.with_suffix(".csv")
    pd.DataFrame({"timestamp": timestamps, "open": 1.0}).to_csv(csv_path, index=False)


def test_ingest_partition_completeness_uses_timestamp_grid(tmp_path: Path) -> None:
    start = datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_exclusive = start + timedelta(hours=1)
    expected_ts = _expected_15m_index(start, end_exclusive)
    out_path = tmp_path / "ohlcv.parquet"

    malformed = [
        expected_ts[0],
        expected_ts[1],
        expected_ts[2],
        expected_ts[2] + timedelta(minutes=5),
    ]
    _write_csv_partition(out_path, malformed)
    assert ingest_partition_complete(out_path, expected_ts) is False

    _write_csv_partition(out_path, list(expected_ts))
    assert ingest_partition_complete(out_path, expected_ts) is True


def test_features_partition_completeness_uses_timestamp_grid(tmp_path: Path) -> None:
    start = pd.Timestamp("2021-02-01 00:00:00+00:00")
    end_exclusive = start + timedelta(hours=1)
    expected_ts = pd.date_range(start=start, end=end_exclusive - timedelta(minutes=15), freq="15min", tz="UTC")
    out_path = tmp_path / "features.parquet"

    malformed = [
        expected_ts[0],
        expected_ts[1],
        expected_ts[2],
        expected_ts[2] + timedelta(minutes=7),
    ]
    _write_csv_partition(out_path, malformed)
    assert features_partition_complete(out_path, expected_ts) is False

    _write_csv_partition(out_path, list(expected_ts))
    assert features_partition_complete(out_path, expected_ts) is True
