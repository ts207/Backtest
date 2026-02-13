import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines._lib.io_utils import list_parquet_files
from pipelines._lib.io_utils import read_parquet

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    HAS_PYARROW = True
except ImportError:  # pragma: no cover - optional dependency
    HAS_PYARROW = False


def test_list_parquet_files_prefers_parquet_but_keeps_csv_only_partitions(tmp_path: Path) -> None:
    base = tmp_path / "dataset"
    (base / "year=2020").mkdir(parents=True, exist_ok=True)
    (base / "year=2024").mkdir(parents=True, exist_ok=True)

    csv_only = base / "year=2020" / "bars_2020-01.csv"
    both_csv = base / "year=2024" / "bars_2024-01.csv"
    both_parquet = base / "year=2024" / "bars_2024-01.parquet"

    csv_only.write_text("a,b\\n1,2\\n", encoding="utf-8")
    both_csv.write_text("a,b\\n3,4\\n", encoding="utf-8")
    both_parquet.write_text("PAR1", encoding="utf-8")

    files = list_parquet_files(base)

    assert csv_only in files
    assert both_parquet in files
    assert both_csv not in files


def test_read_parquet_handles_hive_partition_dirs_with_partition_columns(tmp_path: Path) -> None:
    if not HAS_PYARROW:
        return

    partition_dir = tmp_path / "dataset" / "year=2024" / "month=01"
    partition_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = partition_dir / "bars.parquet"

    table = pa.table(
        {
            "timestamp": ["2024-01-01T00:00:00Z"],
            "close": [100.0],
            "year": [2024],
            "month": [1],
        }
    )
    pq.write_table(table, parquet_path)

    out = read_parquet([parquet_path])

    assert len(out) == 1
    assert int(out.loc[0, "year"]) == 2024
    assert int(out.loc[0, "month"]) == 1
