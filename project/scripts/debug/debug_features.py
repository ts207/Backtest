from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def _make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect a features parquet file.")
    parser.add_argument("--path", required=True, help="Path to features parquet.")
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
    if "liquidation_notional" in df.columns:
        col = pd.to_numeric(df["liquidation_notional"], errors="coerce")
        print("liquidation_notional stats:")
        print(col.describe())
        print(f"Non-zero count: {int((col > 0).sum())}")
    else:
        print("liquidation_notional column missing")

    if "oi_delta_1h" in df.columns:
        print("oi_delta_1h stats:")
        print(pd.to_numeric(df["oi_delta_1h"], errors="coerce").describe())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
