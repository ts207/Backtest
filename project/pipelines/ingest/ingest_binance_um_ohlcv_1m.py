from __future__ import annotations

import argparse
import logging
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple
from zipfile import ZipFile

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.http_utils import download_with_retries
from pipelines._lib.io_utils import ensure_dir, read_parquet, write_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.url_utils import join_url
from pipelines._lib.validation import ensure_utc_timestamp


ARCHIVE_BASE = "https://data.binance.vision/data/futures/um"
EARLIEST_UM_FUTURES = datetime(2019, 9, 1, tzinfo=timezone.utc)


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


def _iter_days(start: datetime, end: datetime) -> List[datetime]:
    days: List[datetime] = []
    cursor = start.replace(hour=0, minute=0, second=0, microsecond=0)
    while cursor <= end:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days


def _expected_bars(start: datetime, end_exclusive: datetime) -> int:
    if end_exclusive <= start:
        return 0
    return int((end_exclusive - start).total_seconds() // 60)


def _read_ohlcv_from_zip(path: Path, symbol: str, source: str) -> pd.DataFrame:
    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "trade_count",
        "taker_base_volume",
        "taker_quote_volume",
        "ignore",
    ]
    with ZipFile(path) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            df = pd.read_csv(f, header=None)

    if df.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "symbol", "source"])

    usable_cols = min(len(columns), df.shape[1])
    df = df.iloc[:, :usable_cols].copy()
    df.columns = columns[:usable_cols]

    if "open_time" not in df.columns:
        raise ValueError(f"Unexpected OHLCV archive schema in {path}: missing open_time column")

    df["open_time"] = pd.to_numeric(df["open_time"], errors="coerce")
    df = df[df["open_time"].notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "symbol", "source"])

    df["timestamp"] = pd.to_datetime(df["open_time"].astype("int64"), unit="ms", utc=True)
    df = df[["timestamp", "open", "high", "low", "close", "volume", "quote_volume", "taker_base_volume"]].copy()
    for col in ["open", "high", "low", "close", "volume", "quote_volume", "taker_base_volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close", "volume", "quote_volume", "taker_base_volume"]).copy()
    df["symbol"] = symbol
    df["source"] = source
    ensure_utc_timestamp(df["timestamp"], "timestamp")
    return df


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
    parser = argparse.ArgumentParser(description="Ingest Binance USD-M OHLCV 1m from archives")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--out_root", default=str(DATA_ROOT / "lake" / "raw" / "binance" / "perp"))
    parser.add_argument("--max_retries", type=int, default=5)
    parser.add_argument("--retry_backoff_sec", type=float, default=2.0)
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    run_id = args.run_id
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    requested_start = _parse_date(args.start)
    requested_end = _parse_date(args.end)
    effective_start = max(requested_start, EARLIEST_UM_FUTURES)
    effective_end = requested_end

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    params = {
        "symbols": symbols,
        "requested_start": args.start,
        "requested_end": args.end,
        "effective_start": effective_start.isoformat(),
        "effective_end": effective_end.isoformat(),
        "out_root": args.out_root,
        "max_retries": args.max_retries,
        "retry_backoff_sec": args.retry_backoff_sec,
        "force": int(args.force),
    }
    manifest = start_manifest("ingest_binance_um_ohlcv_1m", run_id, params, inputs, outputs)

    stats: Dict[str, object] = {"symbols": {}}

    try:
        out_root = Path(args.out_root)
        session = requests.Session()

        for symbol in symbols:
            missing_archives: List[str] = []
            partitions_written: List[str] = []
            partitions_skipped: List[str] = []
            bars_written_total = 0

            for month_start in _iter_months(effective_start, effective_end):
                month_end = _next_month(month_start)
                range_start = max(effective_start, month_start)
                range_end_exclusive = min(effective_end + timedelta(days=1), month_end)
                expected_rows = _expected_bars(range_start, range_end_exclusive)
                if expected_rows == 0:
                    continue

                out_dir = (
                    out_root
                    / symbol
                    / "ohlcv_1m"
                    / f"year={month_start.year}"
                    / f"month={month_start.month:02d}"
                )
                out_path = out_dir / f"ohlcv_{symbol}_1m_{month_start.year}-{month_start.month:02d}.parquet"

                if not args.force and _partition_complete(out_path, expected_rows):
                    partitions_skipped.append(str(out_path))
                    continue

                monthly_url = join_url(
                    ARCHIVE_BASE,
                    "monthly",
                    "klines",
                    symbol,
                    "1m",
                    f"{symbol}-1m-{month_start.year}-{month_start.month:02d}.zip",
                )
                logging.info("Downloading monthly archive %s", monthly_url)

                with tempfile.TemporaryDirectory() as tmpdir:
                    temp_zip = Path(tmpdir) / "ohlcv.zip"
                    result = download_with_retries(
                        monthly_url,
                        temp_zip,
                        max_retries=args.max_retries,
                        backoff_sec=args.retry_backoff_sec,
                        session=session,
                    )

                    frames: List[pd.DataFrame] = []
                    if result.status == "ok":
                        frames.append(_read_ohlcv_from_zip(temp_zip, symbol, "archive_monthly"))
                    else:
                        if result.status == "not_found":
                            missing_archives.append(monthly_url)
                        else:
                            raise RuntimeError(f"Failed to download {monthly_url}: {result.error}")

                        for day in _iter_days(range_start, range_end_exclusive - timedelta(seconds=1)):
                            daily_url = join_url(
                                ARCHIVE_BASE,
                                "daily",
                                "klines",
                                symbol,
                                "1m",
                                f"{symbol}-1m-{day.year}-{day.month:02d}-{day.day:02d}.zip",
                            )
                            logging.info("Downloading daily archive %s", daily_url)
                            daily_zip = Path(tmpdir) / f"ohlcv_{day:%Y%m%d}.zip"
                            daily_result = download_with_retries(
                                daily_url,
                                daily_zip,
                                max_retries=args.max_retries,
                                backoff_sec=args.retry_backoff_sec,
                                session=session,
                            )
                            if daily_result.status == "ok":
                                frames.append(_read_ohlcv_from_zip(daily_zip, symbol, "archive_daily"))
                            elif daily_result.status == "not_found":
                                missing_archives.append(daily_url)
                            else:
                                raise RuntimeError(f"Failed to download {daily_url}: {daily_result.error}")

                if frames:
                    data = pd.concat(frames, ignore_index=True)
                    data = data.sort_values("timestamp").drop_duplicates(subset=["timestamp"])
                    data = data[(data["timestamp"] >= range_start) & (data["timestamp"] < range_end_exclusive)]
                else:
                    data = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "symbol", "source"])

                if not data.empty:
                    if data["timestamp"].duplicated().any():
                        raise ValueError(f"Duplicate timestamps in {symbol} {month_start:%Y-%m}")
                    if not data["timestamp"].is_monotonic_increasing:
                        raise ValueError(f"Timestamps not sorted for {symbol} {month_start:%Y-%m}")

                if not data.empty:
                    ensure_dir(out_dir)
                    written_path, storage = write_parquet(data, out_path)
                    outputs.append(
                        {
                            "path": str(written_path),
                            "rows": int(len(data)),
                            "start_ts": data["timestamp"].min().isoformat(),
                            "end_ts": data["timestamp"].max().isoformat(),
                            "storage": storage,
                        }
                    )
                    partitions_written.append(str(written_path))
                    bars_written_total += int(len(data))
                else:
                    logging.info("No data for %s %s-%02d", symbol, month_start.year, month_start.month)

            bars_expected_total = _expected_bars(effective_start, effective_end + timedelta(days=1))
            stats["symbols"][symbol] = {
                "requested_start": args.start,
                "requested_end": args.end,
                "effective_start": effective_start.isoformat(),
                "effective_end": effective_end.isoformat(),
                "bars_expected": bars_expected_total,
                "bars_written": bars_written_total,
                "missing_archive_files": missing_archives,
                "partitions_written": partitions_written,
                "partitions_skipped": partitions_skipped,
            }

        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Ingestion failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
