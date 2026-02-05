from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Tuple

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


def list_parquet_files(path: Path) -> List[Path]:
    """
    Recursively list partition files under a directory, supporting parquet and csv.
    If both parquet and csv exist for the same partition file stem, parquet is preferred.
    """
    if not path.exists():
        return []
    parquet_files = [p for p in path.rglob("*.parquet") if p.is_file()]
    csv_files = [p for p in path.rglob("*.csv") if p.is_file()]
    if not parquet_files and not csv_files:
        return []

    by_stem = {}
    for file_path in parquet_files + csv_files:
        stem_key = str(file_path.with_suffix(""))
        chosen = by_stem.get(stem_key)
        if chosen is None:
            by_stem[stem_key] = file_path
            continue
        if chosen.suffix != ".parquet" and file_path.suffix == ".parquet":
            by_stem[stem_key] = file_path
    return sorted(by_stem.values())


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
            frames.append(pq.read_table(file_path).to_pandas())
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
