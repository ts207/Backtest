from __future__ import annotations

import argparse
import logging
import os
import sys
import tempfile
import time
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
CHUNK_SIZE = 1_000_000 
MAX_WORKERS = 2 


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

    # Mapping based on Binance CSV header (update_id,best_bid_price,best_bid_qty,best_ask_price,best_ask_qty,transaction_time,event_time)
    mapping = {
        "event_time": "timestamp",
        "event_timestamp": "timestamp",
        "transaction_time": "timestamp",
        "transact_time": "timestamp",
        "best_bid_price": "bid_price",
        "bid_price": "bid_price",
        "bid_p": "bid_price",
        "best_bid_qty": "bid_qty",
        "bid_qty": "bid_qty",
        "bid_q": "bid_qty",
        "best_ask_price": "ask_price",
        "ask_price": "ask_price",
        "ask_p": "ask_price",
        "best_ask_qty": "ask_qty",
        "ask_qty": "ask_qty",
        "ask_q": "ask_qty",
    }
    
    # Rename columns using map with priority
    new_cols = []
    found_timestamp = False
    for c in df.columns:
        clean_c = str(c).lower().strip()
        target = mapping.get(clean_c, clean_c)
        if target == "timestamp":
            if not found_timestamp and clean_c in ["transaction_time", "transact_time"]:
                new_cols.append("timestamp")
                found_timestamp = True
            elif not found_timestamp and clean_c in ["event_time", "event_timestamp"]:
                new_cols.append("timestamp")
                found_timestamp = True
            else:
                new_cols.append(f"raw_{clean_c}") # Keep but rename to avoid duplicates
        else:
            new_cols.append(target)
    df.columns = new_cols

    # If timestamp still not found, try to infer
    if "timestamp" not in df.columns:
        if df.shape[1] >= 7:
            # update_id, b_p, b_q, a_p, a_q, t_t, e_t
            df = df.rename(columns={df.columns[5]: "timestamp"})

    if "timestamp" not in df.columns:
        logging.warning("[%s] Could not find timestamp column in chunk. Cols: %s", symbol, df.columns.tolist())
        return pd.DataFrame(columns=target_cols + ["symbol", "source"])

    # Final cleanup
    for col in ["timestamp", "bid_price", "bid_qty", "ask_price", "ask_qty"]:
        if col in df.columns:
            # Ensure we are dealing with a Series and convert to numeric
            s = df[col]
            if isinstance(s, pd.DataFrame): # Handle potential duplicate column names
                s = s.iloc[:, 0]
            df[col] = pd.to_numeric(s, errors="coerce")
        else:
            df[col] = 0.0
            
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
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
    
    # Infer header automatically
    reader = pd.read_csv(csv_file_obj, header='infer', chunksize=CHUNK_SIZE, low_memory=False)
    
    last_log_time = time.time()

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
        n = len(df)
        total_rows += n
        
        if time.time() - last_log_time > 30:
            logging.info("[%s] Processing... total_rows=%d current_ts=%s", symbol, total_rows, df["timestamp"].max())
            last_log_time = time.time()
            
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
    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        log_handlers.append(logging.FileHandler(args.log_path, mode="a"))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s", force=True)

    session = requests.Session()
    missing_archives: List[str] = []
    partitions_written: List[str] = []
    partitions_skipped: List[str] = []
    rows_written_total = 0

    logging.info("[%s] Starting ingestion: %s to %s", symbol, effective_start.date(), effective_end.date())

    for month_start in _iter_months(effective_start, effective_end):
        month_end = _next_month(month_start)
        range_start = max(effective_start, month_start)
        range_end_exclusive = min(effective_end + timedelta(days=1), month_end)

        out_dir = out_root / symbol / "book_ticker" / f"year={month_start.year}" / f"month={month_start.month:02d}"
        out_path = out_dir / f"book_ticker_{symbol}_{month_start.year}-{month_start.month:02d}.parquet"

        if not args.force and out_path.exists():
            logging.info("[%s] Skipping existing month: %s-%02d", symbol, month_start.year, month_start.month)
            partitions_skipped.append(str(out_path))
            continue

        writer: pq.ParquetWriter | None = None
        rows_in_partition = 0
        
        logging.info("[%s] Ingesting month: %s-%02d", symbol, month_start.year, month_start.month)

        with tempfile.TemporaryDirectory() as tmpdir:
            for day in _iter_days(range_start, range_end_exclusive - timedelta(seconds=1)):
                daily_url = join_url(ARCHIVE_BASE, "daily", "bookTicker", symbol, f"{symbol}-bookTicker-{day:%Y-%m-%d}.zip")
                daily_zip = Path(tmpdir) / f"book_ticker_{day:%Y%m%d}.zip"
                
                logging.info("[%s] Downloading %s", symbol, daily_url)
                daily_result = download_with_retries(daily_url, daily_zip, max_retries=args.max_retries, session=session)
                
                if daily_result.status == "ok":
                    logging.info("[%s] Successfully downloaded %s", symbol, daily_url)
                    try:
                        with ZipFile(daily_zip) as zf:
                            csv_name = zf.namelist()[0]
                            logging.info("[%s] Processing %s", symbol, csv_name)
                            with zf.open(csv_name) as f:
                                n, _, __, writer = _process_csv_stream_to_parquet(f, out_path, symbol, "archive_daily", range_start, range_end_exclusive, writer)
                                logging.info("[%s] Added %d rows from %s", symbol, n, day.date())
                                rows_in_partition += n
                    except Exception as zip_exc:
                        logging.error("[%s] Error processing ZIP %s: %s", symbol, daily_zip.name, zip_exc)
                else:
                    logging.warning("[%s] Daily archive download failed (status=%s): %s", symbol, daily_result.status, daily_url)
                    missing_archives.append(daily_url)

        if writer:
            writer.close()
            partitions_written.append(str(out_path))
            rows_written_total += rows_in_partition
            logging.info("[%s] Finished %s-%02d (%d rows)", symbol, month_start.year, month_start.month, rows_in_partition)
        else:
            logging.error("[%s] No data found for %s-%02d", symbol, month_start.year, month_start.month)

    return {
        "symbol": symbol,
        "rows_written": rows_written_total,
        "missing_archive_files": missing_archives,
        "partitions_written": partitions_written,
        "partitions_skipped": partitions_skipped,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest Binance USD-M bookTicker (Optimized Daily)")
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

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
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
        
        actual_concurrency = min(args.concurrency, len(symbols))
        
        if actual_concurrency > 1:
            with ProcessPoolExecutor(max_workers=actual_concurrency) as executor:
                futures = {executor.submit(_ingest_symbol, s, effective_start, effective_end, out_root, args): s for s in symbols}
                for future in as_completed(futures):
                    symbol = str(futures[future])
                    try:
                        res = future.result()
                        payload = dict(res)
                        payload.setdefault("symbol", symbol)
                        symbol_results[symbol] = payload
                    except Exception as exc:
                        logging.exception("[%s] Worker failed", symbol)
                        symbol_failures[symbol] = {"symbol": symbol, "status": "failed", "error": str(exc)}
        else:
            for s in symbols:
                try:
                    res = _ingest_symbol(s, effective_start, effective_end, out_root, args)
                    symbol_results[s] = res
                except Exception as exc:
                    logging.exception("[%s] Ingestion failed", s)
                    symbol_failures[s] = {"symbol": s, "status": "failed", "error": str(exc)}

        for symbol in symbols:
            if symbol in symbol_results:
                stats["symbols"][symbol] = symbol_results[symbol]
            else:
                stats["symbols"][symbol] = symbol_failures.get(symbol, {"status": "failed"})

        finalize_manifest(manifest, "success" if not symbol_failures else "failed", stats=stats)
        return 0 if not symbol_failures else 1
    except Exception as exc:
        logging.exception("Ingestion orchestrator failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
