from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.config import load_configs
from pipelines._lib.io_utils import ensure_dir, list_parquet_files, read_parquet, run_scoped_lake_path, write_parquet
from pipelines._lib.run_manifest import (
    finalize_manifest,
    schema_hash_from_columns,
    start_manifest,
    validate_input_provenance,
)
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
    if "funding_rate_scaled" in merged.columns:
        interval_hours = 8
        bars_per_event = int((interval_hours * 60) / 1)
        merged["funding_rate_event_scaled"] = merged["funding_rate_scaled"]
        merged["funding_rate_scaled"] = merged["funding_rate_scaled"] / bars_per_event
    
    merged["funding_missing"] = merged["funding_rate_scaled"].isna()
    missing_pct = float(merged["funding_missing"].mean()) if len(merged) else 0.0
    return merged[["timestamp", "funding_event_ts", "funding_rate_scaled", "funding_missing"]], missing_pct


def main() -> int:
    parser = argparse.ArgumentParser(description="Build cleaned 5m bars")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--market", choices=["perp", "spot"], default="perp")
    parser.add_argument("--start", required=False)
    parser.add_argument("--end", required=False)
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    run_id = args.run_id
    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    market = str(args.market).strip().lower()

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    params = {
        "symbols": symbols,
        "market": market,
        "start": args.start,
        "end": args.end,
        "force": int(args.force),
        "source_vendor": "binance",
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    stage_name = "build_cleaned_5m" if market == "perp" else "build_cleaned_5m_spot"
    manifest = start_manifest(stage_name, run_id, params, inputs, outputs)
    stats: Dict[str, object] = {"symbols": {}}

    try:
        for symbol in symbols:
            raw_dir = DATA_ROOT / "lake" / "raw" / "binance" / market / symbol / "ohlcv_5m"
            funding_dir = DATA_ROOT / "lake" / "raw" / "binance" / market / symbol / "funding"
            raw_files = list_parquet_files(raw_dir)
            funding_files = list_parquet_files(funding_dir) if market == "perp" else []

            raw = read_parquet(raw_files)
            funding = read_parquet(funding_files) if market == "perp" and funding_files else pd.DataFrame()

            if raw.empty:
                logging.warning("No raw OHLCV 5m data for %s", symbol)
                continue

            raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True)
            raw = raw.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)

            start_ts = raw["timestamp"].min()
            end_ts = raw["timestamp"].max()
            end_exclusive = end_ts + timedelta(minutes=15)

            full_index = pd.date_range(start=start_ts, end=end_exclusive - timedelta(minutes=15), freq="5min", tz=timezone.utc)
            bars = raw.set_index("timestamp").reindex(full_index).reset_index().rename(columns={"index": "timestamp"})
            
            gap_cols = ["open", "high", "low", "close", "volume"]
            for opt_col in ["quote_volume", "taker_base_volume"]:
                if opt_col in bars.columns:
                    gap_cols.append(opt_col)
            
            bars["is_gap"] = bars[gap_cols].isna().any(axis=1)
            bars["gap_len"] = _gap_lengths(bars["is_gap"])

            if market == "perp" and not funding.empty:
                funding["timestamp"] = pd.to_datetime(funding["timestamp"], utc=True)
                funding, _ = infer_and_apply_funding_scale(funding, "funding_rate")
                aligned_funding, _ = _align_funding(bars, funding)
                bars = bars.merge(
                    aligned_funding[["timestamp", "funding_event_ts", "funding_rate_scaled", "funding_missing"]],
                    on="timestamp",
                    how="left",
                )
                bars["funding_rate_scaled"] = bars["funding_rate_scaled"].fillna(0.0) # Fill NaN after merge
                bars["funding_missing"] = bars["funding_missing"].fillna(True) # Fill NaN after merge
            else:
                bars["funding_rate_scaled"] = 0.0
                bars["funding_missing"] = True

            cleaned_dir = DATA_ROOT / "lake" / "cleaned" / market / symbol / "bars_5m"
            run_cleaned_dir = run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", market, symbol, "bars_5m")

            for month_start in _iter_months(start_ts, end_ts):
                month_end = _next_month(month_start)
                range_start = max(start_ts, month_start)
                range_end_exclusive = min(end_exclusive, month_end)

                bars_month = bars[(bars["timestamp"] >= range_start) & (bars["timestamp"] < range_end_exclusive)]
                
                out_path = (
                    run_cleaned_dir
                    / f"year={month_start.year}"
                    / f"month={month_start.month:02d}"
                    / f"bars_{symbol}_5m_{month_start.year}-{month_start.month:02d}.parquet"
                )
                compat_path = (
                    cleaned_dir
                    / f"year={month_start.year}"
                    / f"month={month_start.month:02d}"
                    / f"bars_{symbol}_5m_{month_start.year}-{month_start.month:02d}.parquet"
                )

                logging.info("Writing cleaned data to out_path: %s", out_path)
                ensure_dir(out_path.parent)
                written, storage = write_parquet(bars_month.reset_index(drop=True), out_path)
                logging.info("Writing cleaned data to compat_path: %s", compat_path)
                ensure_dir(compat_path.parent)
                write_parquet(bars_month.reset_index(drop=True), compat_path)
                
                outputs.append({
                    "path": str(written),
                    "rows": int(len(bars_month)),
                    "start_ts": bars_month["timestamp"].min().isoformat(),
                    "end_ts": bars_month["timestamp"].max().isoformat(),
                    "storage": storage,
                })

            stats["symbols"][symbol] = {
                "start": start_ts.isoformat(),
                "end": end_ts.isoformat(),
                "rows": int(len(bars)),
            }

        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Cleaning failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
