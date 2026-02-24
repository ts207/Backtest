from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def _make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect a phase2 candidate parquet file.")
    parser.add_argument("--path", required=True, help="Path to a parquet file.")
    parser.add_argument("--head", type=int, default=5, help="Rows to preview.")
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
        print(f"Failed to read {path}: {exc}")
        return 1

    print(f"Rows: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    if len(df) > 0:
        print(df.head(max(1, int(args.head))).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
