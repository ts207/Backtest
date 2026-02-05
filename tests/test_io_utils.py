import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines._lib.io_utils import list_parquet_files


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
