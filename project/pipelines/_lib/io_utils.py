from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    HAS_PYARROW = True
except ImportError:  # pragma: no cover - optional dependency
    HAS_PYARROW = False


def ensure_dir(path: Path) -> None:
    """
    Ensure a directory exists.
    """
    path.mkdir(parents=True, exist_ok=True)


def run_scoped_lake_path(data_root: Path, run_id: str, *parts: str) -> Path:
    """
    Build a run-scoped lake path under ``data/lake/runs/<run_id>/...``.
    """
    return Path(data_root) / "lake" / "runs" / str(run_id) / Path(*parts)


def choose_partition_dir(candidates: Sequence[Path]) -> Path | None:
    """
    Pick the best available partition directory from ordered candidates.

    Selection order:
    1) first existing directory containing parquet/csv files (recursive)
    2) first existing non-empty directory
    3) first existing directory
    """
    normalized = [Path(p) for p in candidates if p is not None]
    if not normalized:
        return None

    for candidate in normalized:
        if not candidate.exists() or not candidate.is_dir():
            continue
        if any(candidate.rglob("*.parquet")) or any(candidate.rglob("*.csv")):
            return candidate

    for candidate in normalized:
        if not candidate.exists() or not candidate.is_dir():
            continue
        try:
            next(candidate.iterdir())
            return candidate
        except StopIteration:
            continue

    for candidate in normalized:
        if candidate.exists() and candidate.is_dir():
            return candidate

    return None


def list_parquet_files(path: Path) -> List[Path]:
    """
    Recursively list all parquet files under a directory.
    If parquet exists in some partitions, include parquet files plus CSV-only
    partitions that have no parquet in the same directory.
    """
    if not path.exists():
        return []
    parquet_files = sorted([p for p in path.rglob("*.parquet") if p.is_file()])
    csv_files = sorted([p for p in path.rglob("*.csv") if p.is_file()])
    if not parquet_files:
        return csv_files

    parquet_dirs = {p.parent for p in parquet_files}
    csv_only_partitions = [p for p in csv_files if p.parent not in parquet_dirs]
    return sorted(parquet_files + csv_only_partitions)


def read_parquet(files: Iterable[Path]) -> pd.DataFrame:
    """
    Read multiple Parquet (or CSV fallback) files into a single DataFrame.
    """
    frames = []
    for file_path in files:
        if file_path.suffix == ".csv":
            frames.append(pd.read_csv(file_path))
        else:
            if not HAS_PYARROW:
                raise ImportError("pyarrow is required to read parquet files")
            # Use ParquetFile for single-file reads so hive-style partition
            # directories (e.g. year=2024/month=01) do not trigger dataset
            # partition schema inference conflicts when the file also contains
            # partition columns.
            frames.append(pq.ParquetFile(file_path).read().to_pandas())
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def write_parquet(df: pd.DataFrame, path: Path) -> Tuple[Path, str]:
    """
    Write a DataFrame to a Parquet file if available; otherwise fall back to CSV.
    Returns the actual path written and the storage format ("parquet" or "csv").
    """
    ensure_dir(path.parent)
    if HAS_PYARROW:
        temp_path = path.with_suffix(path.suffix + ".tmp")
        table = pa.Table.from_pandas(df)
        pq.write_table(table, temp_path)
        temp_path.replace(path)
        return path, "parquet"
    csv_path = path.with_suffix(".csv")
    temp_path = csv_path.with_suffix(".csv.tmp")
    df.to_csv(temp_path, index=False)
    temp_path.replace(csv_path)
    return csv_path, "csv"


def sorted_glob(paths):
    import glob
    return sorted(glob.glob(paths))
