from __future__ import annotations

from pathlib import Path
from typing import Iterable, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def ensure_dir(path: Path) -> None:
    """
    Ensure a directory exists.
    """
    path.mkdir(parents=True, exist_ok=True)


def list_parquet_files(path: Path) -> List[Path]:
    """
    Recursively list all parquet files under a directory.
    """
    if not path.exists():
        return []
    return sorted([p for p in path.rglob("*.parquet") if p.is_file()])


def read_parquet(files: Iterable[Path]) -> pd.DataFrame:
    """
    Read multiple Parquet files into a single DataFrame.
    """
    frames = []
    for file_path in files:
        frames.append(pq.read_table(file_path).to_pandas())
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """
    Write a DataFrame to a Parquet file, ensuring parent directories exist.
    """
    ensure_dir(path.parent)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)
