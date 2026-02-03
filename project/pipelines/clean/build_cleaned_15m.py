from __future__ import annotations

import argparse
import sys
from datetime import timezone
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.config import load_configs
from pipelines._lib.io_utils import list_parquet_files, read_parquet, write_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.validation import ensure_utc_timestamp, validate_columns


def _collect_stats(df: pd.DataFrame) -> Dict[str, object]:
    if df.empty:
        return {"rows": 0, "start_ts": None, "end_ts": None}
    return {
        "rows": int(len(df)),
        "start_ts": df["timestamp"].min().isoformat(),
        "end_ts": df["timestamp"].max().isoformat(),
    }


def _gap_lengths(is_gap: pd.Series) -> pd.Series:
    gap_group = (is_gap != is_gap.shift()).cumsum()
    lengths = is_gap.groupby(gap_group).transform("sum")
    return lengths.where(is_gap, 0).astype(int)


def _align_funding(bars: pd.DataFrame, funding: pd.DataFrame) -> pd.DataFrame:
    if funding.empty:
        bars["funding_rate"] = np.nan
        bars["funding_filled"] = False
        return bars

    funding_sorted = funding.sort_values("timestamp").copy()
    merged = pd.merge_asof(
        bars.sort_values("timestamp"),
        funding_sorted,
        on="timestamp",
        direction="backward",
    )
    merged["funding_filled"] = merged["funding_rate"].notna()
    merged["funding_rate"] = merged["funding_rate"].ffill(limit=96)
    merged["funding_filled"] = merged["funding_rate"].notna()
    return merged


def main() -> int:
    parser = argparse.ArgumentParser(description="Build cleaned 15m bars")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    run_id = args.run_id
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    config_paths = ["project/configs/pipeline.yaml"]
    config_paths.extend(args.config)
    config = load_configs(config_paths)
    manifest = start_manifest(run_id, "build_cleaned_15m", config_paths)
    manifest["parameters"] = {
        "symbols": symbols,
        "trade_day_timezone": config.get("trade_day_timezone", "UTC"),
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    if args.log_path:
        outputs.append({"path": args.log_path, "rows": None, "start_ts": None, "end_ts": None})
    manifest = start_manifest(run_id, "build_cleaned_15m", ["project/configs/pipeline.yaml"])
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []

    try:
        for symbol in symbols:
            raw_dir = Path("project") / "lake" / "raw" / "binance" / "perp" / symbol / "ohlcv_15m"
            funding_dir = Path("project") / "lake" / "raw" / "binance" / "perp" / symbol / "funding"
            raw_files = list_parquet_files(raw_dir)
            funding_files = list_parquet_files(funding_dir)

            raw = read_parquet(raw_files)
            funding = read_parquet(funding_files)

            if raw.empty:
                raise ValueError(f"No raw OHLCV data for {symbol}")

            validate_columns(raw, ["timestamp", "open", "high", "low", "close", "volume"])
            raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True)
            ensure_utc_timestamp(raw["timestamp"], "timestamp")
            raw = raw.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)

            start = raw["timestamp"].min()
            end = raw["timestamp"].max()
            full_index = pd.date_range(start=start, end=end, freq="15min", tz=timezone.utc)
            bars = raw.set_index("timestamp").reindex(full_index).reset_index().rename(columns={"index": "timestamp"})
            bars["is_gap"] = bars[["open", "high", "low", "close", "volume"]].isna().any(axis=1)
            bars["gap_len"] = _gap_lengths(bars["is_gap"])

            inputs.append({"path": str(raw_dir), **_collect_stats(raw)})
            if not funding.empty:
                inputs.append({"path": str(funding_dir), **_collect_stats(funding)})

            cleaned_dir = Path("project") / "lake" / "cleaned" / "perp" / symbol / "bars_15m"
            funding_out_dir = Path("project") / "lake" / "cleaned" / "perp" / symbol / "funding_aligned_15m"

            bars_with_funding = _align_funding(bars, funding)

            bars["year"] = bars["timestamp"].dt.year
            bars["month"] = bars["timestamp"].dt.month
            bars_with_funding["year"] = bars_with_funding["timestamp"].dt.year
            bars_with_funding["month"] = bars_with_funding["timestamp"].dt.month

            for (year, month), group in bars.groupby(["year", "month"], sort=True):
                group_out = group.drop(columns=["year", "month"]).reset_index(drop=True)
                out_path = cleaned_dir / f"year={year}" / f"month={month:02d}" / f"bars_{year}_{month:02d}.parquet"
                write_parquet(group_out, out_path)
                outputs.append({"path": str(out_path), **_collect_stats(group_out)})

            for (year, month), group in bars_with_funding.groupby(["year", "month"], sort=True):
                group_out = group.drop(columns=["year", "month"]).reset_index(drop=True)
                out_path = (
                    funding_out_dir
                    / f"year={year}" / f"month={month:02d}" / f"funding_aligned_{year}_{month:02d}.parquet"
                )
                write_parquet(group_out, out_path)
                outputs.append({"path": str(out_path), **_collect_stats(group_out)})

        finalize_manifest(manifest, inputs, outputs, "success")
        return 0
    except Exception as exc:
        finalize_manifest(manifest, inputs, outputs, "failed", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
