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


def _read_book_ticker_from_zip(path: Path, symbol: str, source: str) -> pd.DataFrame:
    columns = [
        "event_time",
        "transaction_time",
        "symbol",
        "bid_price",
        "bid_qty",
        "ask_price",
        "ask_qty",
    ]
    with ZipFile(path) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            df = pd.read_csv(f, header=None)

    if df.empty:
        return pd.DataFrame(columns=["timestamp", "bid_price", "bid_qty", "ask_price", "ask_qty", "symbol", "source"])

    usable_cols = min(len(columns), df.shape[1])
    df = df.iloc[:, :usable_cols].copy()
    df.columns = columns[:usable_cols]

    # Binance UM bookTicker sometimes has event_time, sometimes not.
    # Usually it's: event_time, transaction_time, symbol, bid_price, bid_qty, ask_price, ask_qty
    # If the first column is symbol, then it's different.
    
    if not pd.to_numeric(df.iloc[0, 0], errors="coerce") > 0:
         # maybe it's symbol first?
         # Check if symbol column exists
         if isinstance(df.iloc[0, 0], str) and df.iloc[0, 0].upper() == symbol:
             # it is: symbol, bid_price, bid_qty, ask_price, ask_qty, event_time, transaction_time
             cols = ["symbol", "bid_price", "bid_qty", "ask_price", "ask_qty", "event_time", "transaction_time"]
             df.columns = cols[:df.shape[1]]
         else:
             # try to find timestamp column
             pass

    ts_col = "event_time" if "event_time" in df.columns else "transaction_time"
    if ts_col not in df.columns:
        # Fallback to first column if it's numeric
        df["timestamp_raw"] = pd.to_numeric(df.iloc[:, 0], errors="coerce")
        ts_col = "timestamp_raw"

    df["timestamp"] = pd.to_datetime(df[ts_col].astype("int64"), unit="ms", utc=True)
    df = df[["timestamp", "bid_price", "bid_qty", "ask_price", "ask_qty"]].copy()
    for col in ["bid_price", "bid_qty", "ask_price", "ask_qty"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["bid_price", "ask_price"]).copy()
    df["symbol"] = symbol
    df["source"] = source
    ensure_utc_timestamp(df["timestamp"], "timestamp")
    return df


def _partition_complete(path: Path) -> bool:
    return path.exists()


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest Binance USD-M bookTicker from archives")
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
    manifest = start_manifest("ingest_binance_um_book_ticker", run_id, params, inputs, outputs)

    stats: Dict[str, object] = {"symbols": {}}

    try:
        out_root = Path(args.out_root)
        session = requests.Session()

        for symbol in symbols:
            missing_archives: List[str] = []
            partitions_written: List[str] = []
            partitions_skipped: List[str] = []
            rows_written_total = 0

            for month_start in _iter_months(effective_start, effective_end):
                month_end = _next_month(month_start)
                range_start = max(effective_start, month_start)
                range_end_exclusive = min(effective_end + timedelta(days=1), month_end)

                out_dir = (
                    out_root
                    / symbol
                    / "book_ticker"
                    / f"year={month_start.year}"
                    / f"month={month_start.month:02d}"
                )
                out_path = out_dir / f"book_ticker_{symbol}_{month_start.year}-{month_start.month:02d}.parquet"

                if not args.force and _partition_complete(out_path):
                    partitions_skipped.append(str(out_path))
                    continue

                monthly_url = join_url(
                    ARCHIVE_BASE,
                    "monthly",
                    "bookTicker",
                    symbol,
                    f"{symbol}-bookTicker-{month_start.year}-{month_start.month:02d}.zip",
                )
                logging.info("Downloading monthly archive %s", monthly_url)

                with tempfile.TemporaryDirectory() as tmpdir:
                    temp_zip = Path(tmpdir) / "book_ticker.zip"
                    result = download_with_retries(
                        monthly_url,
                        temp_zip,
                        max_retries=args.max_retries,
                        backoff_sec=args.retry_backoff_sec,
                        session=session,
                    )

                    frames: List[pd.DataFrame] = []
                    if result.status == "ok":
                        frames.append(_read_book_ticker_from_zip(temp_zip, symbol, "archive_monthly"))
                    else:
                        if result.status == "not_found":
                            missing_archives.append(monthly_url)
                        else:
                            raise RuntimeError(f"Failed to download {monthly_url}: {result.error}")

                        for day in _iter_days(range_start, range_end_exclusive - timedelta(seconds=1)):
                            daily_url = join_url(
                                ARCHIVE_BASE,
                                "daily",
                                "bookTicker",
                                symbol,
                                f"{symbol}-bookTicker-{day.year}-{day.month:02d}-{day.day:02d}.zip",
                            )
                            logging.info("Downloading daily archive %s", daily_url)
                            daily_zip = Path(tmpdir) / f"book_ticker_{day:%Y%m%d}.zip"
                            daily_result = download_with_retries(
                                daily_url,
                                daily_zip,
                                max_retries=args.max_retries,
                                backoff_sec=args.retry_backoff_sec,
                                session=session,
                            )
                            if daily_result.status == "ok":
                                frames.append(_read_book_ticker_from_zip(daily_zip, symbol, "archive_daily"))
                            elif daily_result.status == "not_found":
                                missing_archives.append(daily_url)
                            else:
                                raise RuntimeError(f"Failed to download {daily_url}: {daily_result.error}")

                if frames:
                    data = pd.concat(frames, ignore_index=True)
                    data = data.sort_values("timestamp").drop_duplicates(subset=["timestamp"])
                    data = data[(data["timestamp"] >= range_start) & (data["timestamp"] < range_end_exclusive)]
                else:
                    data = pd.DataFrame(columns=["timestamp", "bid_price", "bid_qty", "ask_price", "ask_qty", "symbol", "source"])

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
                    rows_written_total += int(len(data))
                else:
                    logging.info("No data for %s %s-%02d", symbol, month_start.year, month_start.month)

            stats["symbols"][symbol] = {
                "rows_written": rows_written_total,
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
