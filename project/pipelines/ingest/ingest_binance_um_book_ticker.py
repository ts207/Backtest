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
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
import requests

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.http_utils import download_with_retries
from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.url_utils import join_url
from pipelines._lib.validation import ensure_utc_timestamp


ARCHIVE_BASE = "https://data.binance.vision/data/futures/um"
EARLIEST_UM_FUTURES = datetime(2019, 9, 1, tzinfo=timezone.utc)
CHUNK_SIZE = 500_000 # Increased for better throughput with pyarrow-based writing
MAX_WORKERS = 3 # Memory-safe concurrency limit for heavy high-freq data


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


def _clean_book_ticker_chunk(df: pd.DataFrame, symbol: str, source: str) -> pd.DataFrame:
    target_cols = ["timestamp", "bid_price", "bid_qty", "ask_price", "ask_qty"]
    
    if df.empty:
        return pd.DataFrame(columns=target_cols + ["symbol", "source"])

    first_val = str(df.iloc[0, 0])
    has_header = not first_val.isdigit() and first_val.lower() != "nan"
    
    if has_header:
        headers = [str(c).lower().strip() for c in df.iloc[0]]
        df = df.iloc[1:].copy()
        mapping = {
            "event_time": "timestamp", "event_timestamp": "timestamp",
            "transaction_time": "timestamp", "transact_time": "timestamp",
            "bid_price": "bid_price", "bid_p": "bid_price",
            "bid_qty": "bid_qty", "bid_q": "bid_qty",
            "ask_price": "ask_price", "ask_p": "ask_price",
            "ask_qty": "ask_qty", "ask_q": "ask_qty",
        }
        df.columns = [mapping.get(h, h) for h in headers]
    else:
        cols = ["event_time", "transaction_time", "symbol", "bid_price", "bid_qty", "ask_price", "ask_qty"]
        df.columns = cols[:df.shape[1]]
        if "event_time" in df.columns: df = df.rename(columns={"event_time": "timestamp"})
        elif "transaction_time" in df.columns: df = df.rename(columns={"transaction_time": "timestamp"})

    if "timestamp" not in df.columns:
        for col in df.columns:
            try:
                val = pd.to_numeric(df[col], errors="coerce").iloc[0]
                if pd.notna(val) and val > 1e12:
                    df = df.rename(columns={col: "timestamp"})
                    break
            except: continue

    if "timestamp" not in df.columns:
        return pd.DataFrame(columns=target_cols + ["symbol", "source"])

    # Final cleanup
    def _to_numeric_series(series_or_df):
        if isinstance(series_or_df, pd.DataFrame):
            series_or_df = series_or_df.iloc[:, 0]
        return pd.to_numeric(series_or_df, errors="coerce")

    df["timestamp"] = pd.to_datetime(_to_numeric_series(df["timestamp"]), unit="ms", utc=True)
    for col in ["bid_price", "bid_qty", "ask_price", "ask_qty"]:
        if col in df.columns:
            df[col] = _to_numeric_series(df[col])
        else:
            df[col] = 0.0
            
    df = df.dropna(subset=["timestamp", "bid_price", "ask_price"]).copy()
    df["symbol"] = symbol
    df["source"] = source
    return df[target_cols + ["symbol", "source"]]


def _process_csv_stream_to_parquet(
    csv_file_obj, out_path: Path, symbol: str, source: str, 
    range_start: datetime, range_end_exclusive: datetime,
    writer: pq.ParquetWriter | None = None
) -> Tuple[int, datetime | None, datetime | None, pq.ParquetWriter | None]:
    total_rows = 0
    start_ts = None
    end_ts = None
    
    reader = pd.read_csv(csv_file_obj, header=None, chunksize=CHUNK_SIZE, low_memory=False)
    for chunk in reader:
        df = _clean_book_ticker_chunk(chunk, symbol, source)
        df = df[(df["timestamp"] >= range_start) & (df["timestamp"] < range_end_exclusive)]
        if df.empty: continue
            
        if writer is None:
            ensure_dir(out_path.parent)
            table = pa.Table.from_pandas(df)
            writer = pq.ParquetWriter(out_path, table.schema, compression='snappy')
            start_ts = df["timestamp"].min()
            
        writer.write_table(pa.Table.from_pandas(df))
        total_rows += len(df)
        if start_ts is None: start_ts = df["timestamp"].min()
        end_ts = df["timestamp"].max()
        
    return total_rows, start_ts, end_ts, writer


def _ingest_symbol(
    symbol: str,
    effective_start: datetime,
    effective_end: datetime,
    out_root: Path,
    args: argparse.Namespace
) -> Dict[str, object]:
    # Worker processes don't inherit logging config from the parent
    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        log_handlers.append(logging.FileHandler(args.log_path, mode="a"))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s", force=True)

    session = requests.Session()
    missing_archives: List[str] = []
    partitions_written: List[str] = []
    partitions_skipped: List[str] = []
    rows_written_total = 0

    logging.info("[%s] Starting ingestion", symbol)

    for month_start in _iter_months(effective_start, effective_end):
        month_end = _next_month(month_start)
        range_start = max(effective_start, month_start)
        range_end_exclusive = min(effective_end + timedelta(days=1), month_end)

        out_dir = out_root / symbol / "book_ticker" / f"year={month_start.year}" / f"month={month_start.month:02d}"
        out_path = out_dir / f"book_ticker_{symbol}_{month_start.year}-{month_start.month:02d}.parquet"

        if not args.force and out_path.exists():
            partitions_skipped.append(str(out_path))
            continue

        monthly_url = join_url(ARCHIVE_BASE, "monthly", "bookTicker", symbol, f"{symbol}-bookTicker-{month_start.year}-{month_start.month:02d}.zip")
        writer: pq.ParquetWriter | None = None
        rows_in_partition = 0
        first_ts, last_ts = None, None

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_zip = Path(tmpdir) / "book_ticker.zip"
            result = download_with_retries(monthly_url, temp_zip, max_retries=args.max_retries, session=session)

            if result.status == "ok":
                with ZipFile(temp_zip) as zf:
                    csv_name = zf.namelist()[0]
                    with zf.open(csv_name) as f:
                        n, start, end, writer = _process_csv_stream_to_parquet(f, out_path, symbol, "archive_monthly", range_start, range_end_exclusive, writer)
                        rows_in_partition += n
                        if first_ts is None: first_ts = start
                        last_ts = end
            else:
                if result.status == "not_found": missing_archives.append(monthly_url)
                else: raise RuntimeError(f"Failed to download {monthly_url}: {result.error}")

                for day in _iter_days(range_start, range_end_exclusive - timedelta(seconds=1)):
                    daily_url = join_url(ARCHIVE_BASE, "daily", "bookTicker", symbol, f"{symbol}-bookTicker-{day:%Y-%m-%d}.zip")
                    daily_zip = Path(tmpdir) / f"book_ticker_{day:%Y%m%d}.zip"
                    daily_result = download_with_retries(daily_url, daily_zip, max_retries=args.max_retries, session=session)
                    if daily_result.status == "ok":
                        with ZipFile(daily_zip) as zf:
                            csv_name = zf.namelist()[0]
                            with zf.open(csv_name) as f:
                                n, start, end, writer = _process_csv_stream_to_parquet(f, out_path, symbol, "archive_daily", range_start, range_end_exclusive, writer)
                                rows_in_partition += n
                                if first_ts is None: first_ts = start
                                last_ts = end
                    elif daily_result.status == "not_found":
                        missing_archives.append(daily_url)
                    else:
                        raise RuntimeError(f"Failed to download {daily_url}: {daily_result.error}")

        if writer:
            writer.close()
            partitions_written.append(str(out_path))
            rows_written_total += rows_in_partition
            logging.info("[%s] Finished %s-%02d (%d rows)", symbol, month_start.year, month_start.month, rows_in_partition)

    return {
        "symbol": symbol,
        "rows_written": rows_written_total,
        "missing_archive_files": missing_archives,
        "partitions_written": partitions_written,
        "partitions_skipped": partitions_skipped,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest Binance USD-M bookTicker (Parallel & Memory-Safe)")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--out_root", default=str(DATA_ROOT / "lake" / "raw" / "binance" / "perp"))
    parser.add_argument("--max_retries", type=int, default=5)
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--concurrency", type=int, default=MAX_WORKERS)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

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

    if not HAS_PYARROW:
        logging.error("pyarrow is required for optimized ingestion.")
        return 1

    manifest = start_manifest("ingest_binance_um_book_ticker", args.run_id, vars(args), [], [])
    stats: Dict[str, object] = {"symbols": {}}

    try:
        out_root = Path(args.out_root)
        symbol_results: Dict[str, Dict[str, object]] = {}
        symbol_failures: Dict[str, Dict[str, object]] = {}
        with ProcessPoolExecutor(max_workers=args.concurrency) as executor:
            futures = {executor.submit(_ingest_symbol, s, effective_start, effective_end, out_root, args): s for s in symbols}
            for future in as_completed(futures):
                symbol = str(futures[future])
                try:
                    res = future.result()
                except Exception as exc:
                    logging.exception("[%s] Worker failed", symbol)
                    symbol_failures[symbol] = {
                        "symbol": symbol,
                        "status": "failed",
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                    }
                else:
                    payload = dict(res)
                    payload.setdefault("symbol", symbol)
                    symbol_results[symbol] = payload

        for symbol in symbols:
            if symbol in symbol_results:
                stats["symbols"][symbol] = symbol_results[symbol]
            elif symbol in symbol_failures:
                stats["symbols"][symbol] = symbol_failures[symbol]
            else:
                stats["symbols"][symbol] = {
                    "symbol": symbol,
                    "status": "failed",
                    "error_type": "MissingResultError",
                    "error_message": "No worker result was captured for symbol",
                }
                symbol_failures[symbol] = dict(stats["symbols"][symbol])

        if symbol_failures:
            failed_symbols = sorted(symbol_failures.keys())
            summary = (
                f"Symbol ingestion failed for {len(failed_symbols)}/{len(symbols)} symbols: "
                f"{', '.join(failed_symbols)}"
            )
            logging.error(summary)
            finalize_manifest(manifest, "failed", error=summary, stats=stats)
            return 1

        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Ingestion failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
