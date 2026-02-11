from __future__ import annotations

import argparse
import logging
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple
from zipfile import ZipFile

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.http_utils import download_with_retries
from pipelines._lib.io_utils import ensure_dir, read_parquet, write_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.url_utils import join_url
from pipelines._lib.validation import ensure_utc_timestamp


ARCHIVE_BASE = "https://data.binance.vision/data/futures/um"
DEFAULT_API_BASE = "https://fapi.binance.com"
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


def _read_funding_from_zip(path: Path, symbol: str, source: str) -> pd.DataFrame:
    with ZipFile(path) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            df = pd.read_csv(f)

            columns = {col.lower(): col for col in df.columns}
            ts_col = None
            rate_col = None
            for candidate in ["fundingtime", "funding_time", "calctime", "timestamp"]:
                if candidate in columns:
                    ts_col = columns[candidate]
                    break
            for candidate in ["fundingrate", "funding_rate", "lastfundingrate"]:
                if candidate in columns:
                    rate_col = columns[candidate]
                    break

            if ts_col is None or rate_col is None:
                f.seek(0)
                df = pd.read_csv(f, header=None)
                ts_col = df.columns[0]
                rate_col = df.columns[1]

    df["timestamp"] = pd.to_datetime(df[ts_col], unit="ms", utc=True)
    df["funding_rate"] = df[rate_col].astype(float)
    df = df[["timestamp", "funding_rate"]]
    df["symbol"] = symbol
    df["source"] = source
    ensure_utc_timestamp(df["timestamp"], "timestamp")
    return df


def _fetch_funding_api(
    session: requests.Session,
    base_url: str,
    symbol: str,
    start: datetime,
    end: datetime,
    limit: int,
    sleep_sec: float,
) -> Tuple[pd.DataFrame, int]:
    rows: List[Dict[str, object]] = []
    cursor = start
    api_calls = 0
    while cursor < end:
        params = {
            "symbol": symbol,
            "startTime": int(cursor.timestamp() * 1000),
            "endTime": int(end.timestamp() * 1000),
            "limit": limit,
        }
        url = join_url(base_url, "fapi", "v1", "fundingRate")
        response = session.get(url, params=params, timeout=30)
        api_calls += 1
        if response.status_code != 200:
            raise RuntimeError(f"API error {response.status_code}: {response.text}")
        payload = response.json()
        if not payload:
            break
        rows.extend(payload)
        last_time = int(payload[-1]["fundingTime"])
        next_cursor = datetime.fromtimestamp(last_time / 1000, tz=timezone.utc) + timedelta(hours=8)
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if sleep_sec:
            time.sleep(sleep_sec)
    if not rows:
        return pd.DataFrame(columns=["timestamp", "funding_rate", "symbol", "source"]), api_calls
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)
    df["funding_rate"] = df["fundingRate"].astype(float)
    df["symbol"] = symbol
    df["source"] = "api"
    df = df[["timestamp", "funding_rate", "symbol", "source"]]
    ensure_utc_timestamp(df["timestamp"], "timestamp")
    return df, api_calls


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
    parser = argparse.ArgumentParser(description="Ingest Binance USD-M funding rates")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--out_root", default=str(PROJECT_ROOT / "lake" / "raw" / "binance" / "perp"))
    parser.add_argument("--use_api_fallback", type=int, default=1)
    parser.add_argument("--api_base_url", default=DEFAULT_API_BASE)
    parser.add_argument("--api_limit", type=int, default=1000)
    parser.add_argument("--api_sleep_sec", type=float, default=0.2)
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
        "use_api_fallback": int(args.use_api_fallback),
        "api_base_url": args.api_base_url,
        "api_limit": args.api_limit,
        "api_sleep_sec": args.api_sleep_sec,
        "force": int(args.force),
    }
    manifest = start_manifest("ingest_binance_um_funding", run_id, params, inputs, outputs)

    stats: Dict[str, object] = {"symbols": {}}

    try:
        out_root = Path(args.out_root)
        session = requests.Session()

        for symbol in symbols:
            missing_archives: List[str] = []
            partitions_written: List[str] = []
            partitions_skipped: List[str] = []
            archive_files_downloaded: List[str] = []
            api_calls = 0
            archive_rows_assumed = 0

            month_frames: List[pd.DataFrame] = []

            for month_start in _iter_months(effective_start, effective_end):
                month_end = _next_month(month_start)
                range_start = max(effective_start, month_start)
                range_end_exclusive = min(effective_end + timedelta(days=1), month_end)
                expected_rows = int((range_end_exclusive - range_start).total_seconds() // (8 * 3600))

                out_dir = (
                    out_root
                    / symbol
                    / "funding"
                    / f"year={month_start.year}"
                    / f"month={month_start.month:02d}"
                )
                out_path = out_dir / f"funding_{symbol}_{month_start.year}-{month_start.month:02d}.parquet"

                if not args.force and _partition_complete(out_path, expected_rows):
                    partitions_skipped.append(str(out_path))
                    archive_rows_assumed += expected_rows
                    continue

                monthly_url = join_url(
                    ARCHIVE_BASE,
                    "monthly",
                    "fundingRate",
                    symbol,
                    f"{symbol}-fundingRate-{month_start.year}-{month_start.month:02d}.zip",
                )
                logging.info("Downloading monthly archive %s", monthly_url)

                with tempfile.TemporaryDirectory() as tmpdir:
                    temp_zip = Path(tmpdir) / "funding.zip"
                    result = download_with_retries(
                        monthly_url,
                        temp_zip,
                        max_retries=args.max_retries,
                        backoff_sec=args.retry_backoff_sec,
                        session=session,
                    )

                    frames: List[pd.DataFrame] = []
                    if result.status == "ok":
                        archive_files_downloaded.append(monthly_url)
                        frames.append(_read_funding_from_zip(temp_zip, symbol, "archive_monthly"))
                    else:
                        if result.status == "not_found":
                            missing_archives.append(monthly_url)
                        else:
                            raise RuntimeError(f"Failed to download {monthly_url}: {result.error}")

                        for day in _iter_days(range_start, range_end_exclusive - timedelta(seconds=1)):
                            daily_url = join_url(
                                ARCHIVE_BASE,
                                "daily",
                                "fundingRate",
                                symbol,
                                f"{symbol}-fundingRate-{day.year}-{day.month:02d}-{day.day:02d}.zip",
                            )
                            logging.info("Downloading daily archive %s", daily_url)
                            daily_zip = Path(tmpdir) / f"funding_{day:%Y%m%d}.zip"
                            daily_result = download_with_retries(
                                daily_url,
                                daily_zip,
                                max_retries=args.max_retries,
                                backoff_sec=args.retry_backoff_sec,
                                session=session,
                            )
                            if daily_result.status == "ok":
                                archive_files_downloaded.append(daily_url)
                                frames.append(_read_funding_from_zip(daily_zip, symbol, "archive_daily"))
                            elif daily_result.status == "not_found":
                                missing_archives.append(daily_url)
                            else:
                                raise RuntimeError(f"Failed to download {daily_url}: {daily_result.error}")

                if frames:
                    data = pd.concat(frames, ignore_index=True)
                    data = data.sort_values("timestamp").drop_duplicates(subset=["timestamp"])
                    data = data[(data["timestamp"] >= range_start) & (data["timestamp"] < range_end_exclusive)]
                else:
                    data = pd.DataFrame(columns=["timestamp", "funding_rate", "symbol", "source"])

                if not data.empty:
                    if data["timestamp"].duplicated().any():
                        raise ValueError(f"Duplicate timestamps in {symbol} {month_start:%Y-%m}")
                    if not data["timestamp"].is_monotonic_increasing:
                        raise ValueError(f"Timestamps not sorted for {symbol} {month_start:%Y-%m}")

                month_frames.append(data)
                archive_rows_assumed += int(len(data))

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

            archive_data = pd.concat(month_frames, ignore_index=True) if month_frames else pd.DataFrame()
            archive_data = archive_data.sort_values("timestamp") if not archive_data.empty else archive_data

            api_data = pd.DataFrame(columns=["timestamp", "funding_rate", "symbol", "source"])
            api_coverage_start = None
            api_coverage_end = None
            expected_total = int((effective_end + timedelta(days=1) - effective_start).total_seconds() // (8 * 3600))
            needs_api = args.use_api_fallback and (archive_data.empty or archive_rows_assumed < expected_total)
            if needs_api:
                api_data, api_calls = _fetch_funding_api(
                    session,
                    args.api_base_url,
                    symbol,
                    effective_start,
                    effective_end + timedelta(days=1),
                    args.api_limit,
                    args.api_sleep_sec,
                )
                if not api_data.empty:
                    api_coverage_start = api_data["timestamp"].min().isoformat()
                    api_coverage_end = api_data["timestamp"].max().isoformat()

            combined = pd.concat([archive_data, api_data], ignore_index=True)
            if not combined.empty:
                combined = combined.sort_values("timestamp")
                combined = combined.drop_duplicates(subset=["timestamp"], keep="first")

            coverage_start = combined["timestamp"].min().isoformat() if not combined.empty else None
            coverage_end = combined["timestamp"].max().isoformat() if not combined.empty else None

            stats["symbols"][symbol] = {
                "requested_start": args.start,
                "requested_end": args.end,
                "effective_start": effective_start.isoformat(),
                "effective_end": effective_end.isoformat(),
                "coverage_start": coverage_start,
                "coverage_end": coverage_end,
                "archive_files_downloaded": archive_files_downloaded,
                "missing_archive_files": missing_archives,
                "api_calls": api_calls,
                "api_coverage_start": api_coverage_start,
                "api_coverage_end": api_coverage_end,
                "partitions_written": partitions_written,
                "partitions_skipped": partitions_skipped,
            }

        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Funding ingestion failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
