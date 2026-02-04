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
from pipelines._lib.sanity import (
    assert_funding_event_grid,
    assert_funding_sane,
    assert_monotonic_utc_timestamp,
    assert_ohlcv_schema,
    coerce_timestamps_to_hour,
    infer_and_apply_funding_scale,
    is_constant_series,
)
from pipelines._lib.validation import validate_columns


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _month_start(ts: datetime) -> datetime:
    ts = ts.astimezone(timezone.utc)
    return ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _next_month(ts: datetime) -> datetime:
    # FIX: robust month increment (avoids subtle replace issues)
    ts = ts.astimezone(timezone.utc)
    y, m = ts.year, ts.month
    if m == 12:
        y += 1
        m = 1
    else:
        m += 1
    return ts.replace(year=y, month=m, day=1, hour=0, minute=0, second=0, microsecond=0)


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
        aligned["funding_event_ts"] = pd.NaT
        aligned["funding_rate_scaled"] = np.nan
        aligned["funding_missing"] = True
        return aligned, 1.0

    funding_sorted = funding.sort_values("timestamp").copy()
    funding_sorted = funding_sorted.rename(columns={"timestamp": "funding_event_ts"})
    merged = pd.merge_asof(
        bars.sort_values("timestamp"),
        funding_sorted[["funding_event_ts", "funding_rate_scaled"]],
        left_on="timestamp",
        right_on="funding_event_ts",
        direction="backward",
    )
    merged["funding_missing"] = merged["funding_rate_scaled"].isna()
    missing_pct = float(merged["funding_missing"].mean()) if len(merged) else 0.0
    return merged[["timestamp", "funding_event_ts", "funding_rate_scaled", "funding_missing"]], missing_pct


def _expected_15m_index(range_start: datetime, range_end_exclusive: datetime) -> pd.DatetimeIndex:
    return pd.date_range(
        start=range_start,
        end=range_end_exclusive - timedelta(minutes=15),
        freq="15min",
        tz=timezone.utc,
    )


def _partition_complete_15m(path: Path, range_start: datetime, range_end_exclusive: datetime) -> bool:
    """
    FIX: correctness-based completeness check:
    - File exists
    - Has timestamp column
    - Unique timestamps
    - Contains the entire expected 15m timestamp grid for the month partition
    """
    if not path.exists():
        return False
    try:
        df = read_parquet([path])
        if df.empty:
            return False
        if "timestamp" not in df.columns:
            return False

        ts = pd.to_datetime(df["timestamp"], utc=True, format="mixed")
        if ts.isna().any():
            return False
        if ts.duplicated().any():
            return False

        expected = _expected_15m_index(range_start, range_end_exclusive)
        got = set(ts)
        # Subset test avoids being strict about extra rows (but duplicates already rejected)
        return set(expected).issubset(got)
    except Exception:
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Build cleaned 15m bars")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=False)
    parser.add_argument("--end", required=False)
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--allow_missing_funding", type=int, default=0)
    parser.add_argument("--allow_constant_funding", type=int, default=0)
    parser.add_argument("--allow_funding_timestamp_rounding", type=int, default=0)
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
        "allow_missing_funding": int(args.allow_missing_funding),
        "allow_constant_funding": int(args.allow_constant_funding),
        "allow_funding_timestamp_rounding": int(args.allow_funding_timestamp_rounding),
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

            assert_ohlcv_schema(raw)
            raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True, format="mixed")
            if raw["timestamp"].isna().any():
                raise ValueError(f"Invalid raw timestamps for {symbol}")
            raw = raw.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)
            assert_monotonic_utc_timestamp(raw, "timestamp")

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

            funding_scale_used = None
            funding_timestamp_rounded = 0
            if funding.empty:
                if not args.allow_missing_funding:
                    raise ValueError(f"No funding data for {symbol}. Use --allow_missing_funding=1 to proceed.")
            else:
                validate_columns(funding, ["timestamp", "funding_rate"])
                funding["timestamp"] = pd.to_datetime(funding["timestamp"], utc=True, format="mixed")
                if funding["timestamp"].isna().any():
                    raise ValueError(f"Invalid funding timestamps for {symbol}")

                non_hour_mask = (
                    funding["timestamp"].dt.minute.ne(0)
                    | funding["timestamp"].dt.second.ne(0)
                    | funding["timestamp"].dt.microsecond.ne(0)
                )
                if non_hour_mask.any():
                    if not args.allow_funding_timestamp_rounding:
                        raise ValueError(
                            f"Funding timestamps must be on the hour for {symbol}. "
                            "Use --allow_funding_timestamp_rounding=1 to round to the nearest hour."
                        )
                    funding, funding_timestamp_rounded = coerce_timestamps_to_hour(funding, "timestamp")
                    if funding["timestamp"].duplicated().any():
                        raise ValueError(
                            f"Funding timestamp rounding created duplicates for {symbol}. "
                            "Fix raw data or disable rounding."
                        )

                funding = funding.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)
                assert_monotonic_utc_timestamp(funding, "timestamp")
                assert_funding_event_grid(funding, "timestamp", expected_hours=8)

                # Important: infer_and_apply_funding_scale should NOT “scale” decimals from Data.Vision col2.
                # If it does, fix that function or pass a flag there.
                funding, funding_scale_used = infer_and_apply_funding_scale(funding, "funding_rate")
                assert_funding_sane(funding, "funding_rate_scaled")

            aligned_funding, funding_missing_pct = _align_funding(bars, funding)
            funding_event_coverage_total = float(aligned_funding["funding_event_ts"].notna().mean()) if len(aligned_funding) else 0.0
            funding_rate_values = aligned_funding["funding_rate_scaled"].dropna()
            funding_rate_min = float(funding_rate_values.min()) if len(funding_rate_values) else None
            funding_rate_max = float(funding_rate_values.max()) if len(funding_rate_values) else None
            funding_rate_std = float(funding_rate_values.std()) if len(funding_rate_values) else None

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

            gap_count = int((bars["is_gap"] & ~bars["is_gap"].shift(fill_value=False)).sum())
            max_gap_len = int(bars["gap_len"].max()) if len(bars) else 0
            pct_missing_bars = float(bars["is_gap"].mean()) if len(bars) else 0.0

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
                funding_missing = float(funding_month["funding_missing"].mean()) if len(funding_month) else 0.0
                funding_event_coverage = float(funding_month["funding_event_ts"].notna().mean()) if len(funding_month) else 0.0
                funding_event_count = int(funding_month["funding_event_ts"].dropna().nunique()) if len(funding_month) else 0
                funding_month_values = funding_month["funding_rate_scaled"].dropna()
                funding_rate_month_std = float(funding_month_values.std()) if len(funding_month_values) else None

                missing_stats[f"{month_start.year}-{month_start.month:02d}"] = {"pct_missing_ohlcv": missing_pct}
                funding_stats[f"{month_start.year}-{month_start.month:02d}"] = {
                    "pct_missing_funding_event": funding_missing,
                    "pct_funding_event_coverage": funding_event_coverage,
                    "funding_event_count": funding_event_count,
                    "funding_rate_scaled_std": funding_rate_month_std,
                }

                if not args.allow_constant_funding:
                    funding_values = funding_month["funding_rate_scaled"].dropna()
                    if len(funding_values) and is_constant_series(funding_values):
                        raise ValueError(
                            f"Constant funding detected for {symbol} in {month_start:%Y-%m}. "
                            "Use --allow_constant_funding=1 to proceed."
                        )

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

                if not args.force and _partition_complete_15m(bars_out_path, range_start, range_end_exclusive):
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

                if not args.force and _partition_complete_15m(funding_out_path, range_start, range_end_exclusive):
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
                "pct_missing_funding_event": funding_stats,
                "funding_scale_used": funding_scale_used,
                "funding_missing_pct": funding_missing_pct,
                "pct_bars_with_funding_event": funding_event_coverage_total,
                "funding_rate_scaled_min": funding_rate_min,
                "funding_rate_scaled_max": funding_rate_max,
                "funding_rate_scaled_std": funding_rate_std,
                "funding_timestamp_rounded_count": funding_timestamp_rounded,
                "gap_stats": {
                    "pct_missing_ohlcv": pct_missing_bars,
                    "gap_count": gap_count,
                    "max_gap_len": max_gap_len,
                },
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
