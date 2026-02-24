from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def _make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Print parquet columns and row count.")
    parser.add_argument("--path", required=True, help="Path to parquet file.")
    return parser


def main() -> int:
    args = _make_parser().parse_args()
    path = Path(args.path)
    if not path.exists():
        print(f"File not found: {path}")
        return 1
    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        print(f"Error reading {path}: {exc}")
        return 1

    print(f"Rows: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
