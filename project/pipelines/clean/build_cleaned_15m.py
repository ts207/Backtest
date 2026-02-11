from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.config import load_configs
from pipelines._lib.io_utils import ensure_dir, list_parquet_files, read_parquet, write_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.validation import ensure_utc_timestamp, validate_columns


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _month_start(ts: datetime) -> datetime:
    return ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _next_month(ts: datetime) -> datetime:
    year = ts.year + (ts.month // 12)
    month = 1 if ts.month == 12 else ts.month + 1
    return ts.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)


def _iter_months(start: datetime, end: datetime) -> List[datetime]:
    months: List[datetime] = []
    cursor = _month_start(start)
    while cursor <= end:
        months.append(cursor)
        cursor = _next_month(cursor)
    return months


def _expected_bars(start: datetime, end_exclusive: datetime) -> int:
    if end_exclusive <= start:
        return 0
    return int((end_exclusive - start).total_seconds() // (15 * 60))


def _gap_lengths(is_gap: pd.Series) -> pd.Series:
    gap_group = (is_gap != is_gap.shift()).cumsum()
    lengths = is_gap.groupby(gap_group).transform("sum")
    return lengths.where(is_gap, 0).astype(int)


def _align_funding(bars: pd.DataFrame, funding: pd.DataFrame) -> Tuple[pd.DataFrame, float]:
    if funding.empty:
        aligned = bars[["timestamp"]].copy()
        aligned["funding_rate"] = 0.0
        aligned["funding_filled"] = True
        return aligned, 1.0

    funding_sorted = funding.sort_values("timestamp").copy()
    merged = pd.merge_asof(
        bars.sort_values("timestamp"),
        funding_sorted,
        on="timestamp",
        direction="backward",
    )
    filled_mask = merged["funding_rate"].isna()
    merged["funding_rate"] = merged["funding_rate"].ffill()
    missing_after_ffill = merged["funding_rate"].isna()
    merged["funding_rate"] = merged["funding_rate"].fillna(0.0)
    merged["funding_filled"] = filled_mask | missing_after_ffill
    fill_pct = float(merged["funding_filled"].mean()) if len(merged) else 0.0
    return merged[["timestamp", "funding_rate", "funding_filled"]], fill_pct


def _partition_complete(path: Path, expected_rows: int) -> bool:
    if not path.exists():
        csv_path = path.with_suffix(".csv")
        if csv_path.exists():
            path = csv_path
        else:
            return False
    try:
        if expected_rows == 0:
            return True
        data = read_parquet([path])
        return len(data) >= expected_rows
    except Exception:
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Build cleaned 15m bars")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=False)
    parser.add_argument("--end", required=False)
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    run_id = args.run_id
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    config_paths = [str(PROJECT_ROOT / "configs" / "pipeline.yaml")]
    config_paths.extend(args.config)
    config = load_configs(config_paths)

    params = {
        "symbols": symbols,
        "trade_day_timezone": config.get("trade_day_timezone", "UTC"),
        "start": args.start,
        "end": args.end,
        "force": int(args.force),
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("build_cleaned_15m", run_id, params, inputs, outputs)
    stats: Dict[str, object] = {"symbols": {}}

    try:
        for symbol in symbols:
            raw_dir = PROJECT_ROOT / "lake" / "raw" / "binance" / "perp" / symbol / "ohlcv_15m"
            funding_dir = PROJECT_ROOT / "lake" / "raw" / "binance" / "perp" / symbol / "funding"
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

            raw_start = raw["timestamp"].min()
            raw_end = raw["timestamp"].max()
            start_ts = max(raw_start, _parse_date(args.start)) if args.start else raw_start
            if args.end:
                end_ts = min(raw_end, _parse_date(args.end))
                end_exclusive = end_ts + timedelta(days=1)
            else:
                end_ts = raw_end
                end_exclusive = raw_end + timedelta(minutes=15)

            full_index = pd.date_range(
                start=start_ts,
                end=end_exclusive - timedelta(minutes=15),
                freq="15min",
                tz=timezone.utc,
            )
            bars = raw.set_index("timestamp").reindex(full_index).reset_index().rename(columns={"index": "timestamp"})
            bars["is_gap"] = bars[["open", "high", "low", "close", "volume"]].isna().any(axis=1)
            bars["gap_len"] = _gap_lengths(bars["is_gap"])

            if not funding.empty:
                validate_columns(funding, ["timestamp", "funding_rate"])
                funding["timestamp"] = pd.to_datetime(funding["timestamp"], utc=True)
                funding = funding.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)
            aligned_funding, funding_fill_pct = _align_funding(bars, funding)

            inputs.append(
                {"path": str(raw_dir), "rows": int(len(raw)), "start_ts": raw_start.isoformat(), "end_ts": raw_end.isoformat()}
            )
            if not funding.empty:
                inputs.append(
                    {
                        "path": str(funding_dir),
                        "rows": int(len(funding)),
                        "start_ts": funding["timestamp"].min().isoformat(),
                        "end_ts": funding["timestamp"].max().isoformat(),
                    }
                )

            cleaned_dir = PROJECT_ROOT / "lake" / "cleaned" / "perp" / symbol / "bars_15m"
            funding_out_dir = PROJECT_ROOT / "lake" / "cleaned" / "perp" / symbol / "funding_15m"

            missing_stats: Dict[str, Dict[str, float]] = {}
            funding_stats: Dict[str, Dict[str, float]] = {}
            partitions_written: List[str] = []
            partitions_skipped: List[str] = []

            for month_start in _iter_months(start_ts, end_ts):
                month_end = _next_month(month_start)
                range_start = max(start_ts, month_start)
                range_end_exclusive = min(end_exclusive, month_end)
                expected_rows = _expected_bars(range_start, range_end_exclusive)
                if expected_rows == 0:
                    continue

                bars_month = bars[(bars["timestamp"] >= range_start) & (bars["timestamp"] < range_end_exclusive)]
                funding_month = aligned_funding[
                    (aligned_funding["timestamp"] >= range_start) & (aligned_funding["timestamp"] < range_end_exclusive)
                ]

                missing_pct = float(bars_month["is_gap"].mean()) if len(bars_month) else 0.0
                funding_fill = float(funding_month["funding_filled"].mean()) if len(funding_month) else 0.0
                missing_stats[f"{month_start.year}-{month_start.month:02d}"] = {"pct_missing_ohlcv": missing_pct}
                funding_stats[f"{month_start.year}-{month_start.month:02d}"] = {
                    "pct_missing_funding_filled": funding_fill
                }

                bars_out_path = (
                    cleaned_dir
                    / f"year={month_start.year}"
                    / f"month={month_start.month:02d}"
                    / f"bars_{symbol}_15m_{month_start.year}-{month_start.month:02d}.parquet"
                )
                funding_out_path = (
                    funding_out_dir
                    / f"year={month_start.year}"
                    / f"month={month_start.month:02d}"
                    / f"funding15m_{symbol}_{month_start.year}-{month_start.month:02d}.parquet"
                )

                if not args.force and _partition_complete(bars_out_path, expected_rows):
                    partitions_skipped.append(str(bars_out_path))
                else:
                    ensure_dir(bars_out_path.parent)
                    bars_written, storage = write_parquet(bars_month.reset_index(drop=True), bars_out_path)
                    outputs.append(
                        {
                            "path": str(bars_written),
                            "rows": int(len(bars_month)),
                            "start_ts": bars_month["timestamp"].min().isoformat(),
                            "end_ts": bars_month["timestamp"].max().isoformat(),
                            "storage": storage,
                        }
                    )
                    partitions_written.append(str(bars_written))

                if not args.force and _partition_complete(funding_out_path, expected_rows):
                    partitions_skipped.append(str(funding_out_path))
                else:
                    ensure_dir(funding_out_path.parent)
                    funding_written, storage = write_parquet(funding_month.reset_index(drop=True), funding_out_path)
                    outputs.append(
                        {
                            "path": str(funding_written),
                            "rows": int(len(funding_month)),
                            "start_ts": funding_month["timestamp"].min().isoformat(),
                            "end_ts": funding_month["timestamp"].max().isoformat(),
                            "storage": storage,
                        }
                    )
                    partitions_written.append(str(funding_written))

            stats["symbols"][symbol] = {
                "effective_start": start_ts.isoformat(),
                "effective_end": end_ts.isoformat(),
                "pct_missing_ohlcv": missing_stats,
                "pct_missing_funding_filled": funding_stats,
                "partitions_written": partitions_written,
                "partitions_skipped": partitions_skipped,
            }

        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Cleaning failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
