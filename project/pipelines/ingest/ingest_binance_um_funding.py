
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
DEFAULT_API_BASE = "https://fapi.binance.com"
EARLIEST_UM_FUTURES = datetime(2019, 9, 1, tzinfo=timezone.utc)

FUNDING_HOURS = (0, 8, 16)
FUNDING_STEP = timedelta(hours=8)


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _month_start(ts: datetime) -> datetime:
    ts = ts.astimezone(timezone.utc)
    return ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _next_month(ts: datetime) -> datetime:
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


def _iter_days(start: datetime, end: datetime) -> List[datetime]:
    days: List[datetime] = []
    cursor = start.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    while cursor <= end:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days


def _ceil_to_next_funding(ts: datetime) -> datetime:
    ts = ts.astimezone(timezone.utc)
    base = ts.replace(minute=0, second=0, microsecond=0)
    candidates: List[datetime] = []
    for h in FUNDING_HOURS:
        cand = base.replace(hour=h)
        if cand < ts:
            cand += timedelta(days=1)
        candidates.append(cand)
    return min(candidates)


def _expected_funding_timestamps(start: datetime, end_exclusive: datetime) -> List[pd.Timestamp]:
    start = start.astimezone(timezone.utc)
    end_exclusive = end_exclusive.astimezone(timezone.utc)
    cur = _ceil_to_next_funding(start)
    out: List[pd.Timestamp] = []
    while cur < end_exclusive:
        out.append(pd.Timestamp(cur))
        cur += FUNDING_STEP
    return out


def _infer_epoch_unit(ts_series: pd.Series) -> str:
    vals = pd.to_numeric(ts_series, errors="coerce").dropna().astype("int64")
    if vals.empty:
        return "ms"
    med = int(vals.median())
    return "s" if med < 1_000_000_000_000 else "ms"


def _snap_to_8h_grid(ts: pd.Series) -> pd.Series:
    ts = pd.to_datetime(ts, utc=True).dt.round("1s")
    secs = (ts.astype("int64", copy=False) // 1_000_000_000).astype("int64")
    snap = ((secs + 4 * 3600) // (8 * 3600)) * (8 * 3600)
    return pd.to_datetime(snap, unit="s", utc=True)


def _read_funding_from_zip(path: Path, symbol: str, source: str) -> pd.DataFrame:
    with ZipFile(path) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            df = pd.read_csv(f)

            # Header-based detection first
            columns = {str(col).lower(): col for col in df.columns}
            ts_col = None
            rate_col = None

            for candidate in ["fundingtime", "funding_time", "calctime", "calc_time", "timestamp", "time"]:
                if candidate in columns:
                    ts_col = columns[candidate]
                    break

            for candidate in ["fundingrate", "funding_rate", "lastfundingrate"]:
                if candidate in columns:
                    rate_col = columns[candidate]
                    break

            # If missing/ambiguous headers, re-read as headerless and use observed Data.Vision format:
            # [timestamp_ms, interval_hours(=8), fundingRate(decimal)]
            if ts_col is None or rate_col is None:
                f.seek(0)
                df = pd.read_csv(f, header=None)

                if df.shape[1] < 2:
                    raise ValueError(f"Unexpected fundingRate CSV format (cols={df.shape[1]}) in {csv_name}")

                ts_col = df.columns[0]

                if df.shape[1] >= 3:
                    rate_col = df.columns[2]  # IMPORTANT FIX: skip constant interval-hours column
                else:
                    rate_col = df.columns[1]

    df["_ts"] = pd.to_numeric(df[ts_col], errors="coerce")
    df["_rate"] = pd.to_numeric(df[rate_col], errors="coerce")
    df = df.loc[df["_ts"].notna() & df["_rate"].notna()].copy()

    unit = _infer_epoch_unit(df["_ts"])
    df["timestamp"] = pd.to_datetime(df["_ts"].astype("int64"), unit=unit, utc=True)
    df["timestamp"] = _snap_to_8h_grid(df["timestamp"])

    # IMPORTANT FIX: Data.Vision column 2 is already decimal funding rate; do NOT rescale.
    df["funding_rate"] = df["_rate"].astype(float)

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
    end_exclusive: datetime,
    limit: int,
    sleep_sec: float,
) -> Tuple[pd.DataFrame, int]:
    rows: List[Dict[str, object]] = []
    cursor = start.astimezone(timezone.utc)
    end_exclusive = end_exclusive.astimezone(timezone.utc)
    api_calls = 0

    while cursor < end_exclusive:
        params = {
            "symbol": symbol,
            "startTime": int(cursor.timestamp() * 1000),
            "endTime": int(end_exclusive.timestamp() * 1000),
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
        last_time_ms = int(payload[-1]["fundingTime"])
        cursor = datetime.fromtimestamp(last_time_ms / 1000, tz=timezone.utc) + timedelta(milliseconds=1)

        if sleep_sec:
            time.sleep(sleep_sec)

    if not rows:
        return pd.DataFrame(columns=["timestamp", "funding_rate", "symbol", "source"]), api_calls

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)
    df["timestamp"] = _snap_to_8h_grid(df["timestamp"])
    df["funding_rate"] = pd.to_numeric(df["fundingRate"], errors="coerce").astype(float)
    df["symbol"] = symbol
    df["source"] = "api"
    df = df[["timestamp", "funding_rate", "symbol", "source"]]
    ensure_utc_timestamp(df["timestamp"], "timestamp")
    df = df.dropna(subset=["timestamp", "funding_rate"]).sort_values("timestamp").drop_duplicates(subset=["timestamp"])
    return df, api_calls


def _partition_complete(path: Path, expected_ts: List[pd.Timestamp]) -> bool:
    if not path.exists():
        csv_path = path.with_suffix(".csv")
        if csv_path.exists():
            path = csv_path
        else:
            return False
    try:
        df = read_parquet([path])
        if df.empty:
            return len(expected_ts) == 0
        if "timestamp" not in df.columns:
            return False
        ts = pd.to_datetime(df["timestamp"], utc=True)
        if ts.duplicated().any():
            return False
        got = set(ts)
        exp = set(expected_ts)
        return exp.issubset(got)
    except Exception:
        return False


def _missing_expected_timestamps(df: pd.DataFrame, expected_ts: List[pd.Timestamp]) -> List[pd.Timestamp]:
    if df is None or df.empty:
        return expected_ts
    got = set(pd.to_datetime(df["timestamp"], utc=True))
    return [t for t in expected_ts if t not in got]


def _missing_timestamp_ranges(missing_ts: List[pd.Timestamp]) -> List[Tuple[datetime, datetime]]:
    if not missing_ts:
        return []
    ordered = sorted(pd.to_datetime(missing_ts, utc=True))
    step = pd.Timedelta(hours=8)
    ranges: List[Tuple[datetime, datetime]] = []
    range_start = ordered[0]
    prev = ordered[0]
    for ts in ordered[1:]:
        if ts - prev > step:
            ranges.append((range_start.to_pydatetime(), (prev + step).to_pydatetime()))
            range_start = ts
        prev = ts
    ranges.append((range_start.to_pydatetime(), (prev + step).to_pydatetime()))
    return ranges


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest Binance USD-M funding rates")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--out_root", default=str(DATA_ROOT / "lake" / "raw" / "binance" / "perp"))
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
    end_exclusive_all = effective_end + timedelta(days=1)

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

            month_frames: List[pd.DataFrame] = []
            month_specs: List[Dict[str, object]] = []

            # ---------- archive ingest per month ----------
            for month_start in _iter_months(effective_start, effective_end):
                month_end = _next_month(month_start)
                range_start = max(effective_start, month_start)
                range_end_exclusive = min(end_exclusive_all, month_end)

                expected_ts_month = _expected_funding_timestamps(range_start, range_end_exclusive)

                out_dir = (
                    out_root
                    / symbol
                    / "funding"
                    / f"year={month_start.year}"
                    / f"month={month_start.month:02d}"
                )
                out_path = out_dir / f"funding_{symbol}_{month_start.year}-{month_start.month:02d}.parquet"
                month_specs.append(
                    {
                        "out_path": out_path,
                        "range_start": range_start,
                        "range_end_exclusive": range_end_exclusive,
                        "expected_ts_month": expected_ts_month,
                    }
                )

                if not args.force and _partition_complete(out_path, expected_ts_month):
                    partitions_skipped.append(str(out_path))
                    existing_path = out_path if out_path.exists() else out_path.with_suffix(".csv")
                    df_month = read_parquet([existing_path])
                    if "timestamp" not in df_month.columns:
                        raise ValueError(f"Missing timestamp column in existing partition: {existing_path}")
                    df_month["timestamp"] = pd.to_datetime(df_month["timestamp"], utc=True, errors="coerce")
                    df_month = df_month.dropna(subset=["timestamp"]).sort_values("timestamp")
                    df_month = df_month[(df_month["timestamp"] >= range_start) & (df_month["timestamp"] < range_end_exclusive)]
                    month_frames.append(df_month)
                    continue

                monthly_url = join_url(
                    ARCHIVE_BASE,
                    "monthly",
                    "fundingRate",
                    symbol,
                    f"{symbol}-fundingRate-{month_start.year}-{month_start.month:02d}.zip",
                )
                logging.info("Downloading monthly archive %s", monthly_url)

                frames: List[pd.DataFrame] = []
                with tempfile.TemporaryDirectory() as tmpdir:
                    tmpdir = str(tmpdir)
                    temp_zip = Path(tmpdir) / "funding.zip"
                    result = download_with_retries(
                        monthly_url,
                        temp_zip,
                        max_retries=args.max_retries,
                        backoff_sec=args.retry_backoff_sec,
                        session=session,
                    )

                    if result.status == "ok":
                        archive_files_downloaded.append(monthly_url)
                        frames.append(_read_funding_from_zip(temp_zip, symbol, "archive_monthly"))
                    else:
                        if result.status == "not_found":
                            missing_archives.append(monthly_url)
                        else:
                            raise RuntimeError(f"Failed to download {monthly_url}: {result.error}")

                        # fall back to daily archives
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
                        data = data.sort_values("timestamp")
                        if not data["timestamp"].is_monotonic_increasing:
                            raise ValueError(f"Timestamps not sorted for {symbol} {month_start:%Y-%m}")

                month_frames.append(data)

            archive_data = pd.concat(month_frames, ignore_index=True) if month_frames else pd.DataFrame()
            archive_data = archive_data.sort_values("timestamp") if not archive_data.empty else archive_data
            if not archive_data.empty:
                archive_data = archive_data.drop_duplicates(subset=["timestamp"], keep="first")

            expected_ts_all = _expected_funding_timestamps(effective_start, end_exclusive_all)
            missing_from_archive = _missing_expected_timestamps(archive_data, expected_ts_all)

            api_data = pd.DataFrame(columns=["timestamp", "funding_rate", "symbol", "source"])
            api_coverage_start = None
            api_coverage_end = None

            if int(args.use_api_fallback) and len(missing_from_archive) > 0:
                api_frames: List[pd.DataFrame] = []
                for api_start, api_end_exclusive in _missing_timestamp_ranges(missing_from_archive):
                    frame, calls = _fetch_funding_api(
                        session,
                        args.api_base_url,
                        symbol,
                        api_start,
                        api_end_exclusive,
                        args.api_limit,
                        args.api_sleep_sec,
                    )
                    api_calls += int(calls)
                    if not frame.empty:
                        api_frames.append(frame)
                if api_frames:
                    api_data = pd.concat(api_frames, ignore_index=True)
                    api_data = api_data.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="first")
                if not api_data.empty:
                    api_coverage_start = api_data["timestamp"].min().isoformat()
                    api_coverage_end = api_data["timestamp"].max().isoformat()

            frames_to_merge = [f for f in (archive_data, api_data) if not f.empty]
            if frames_to_merge:
                combined = pd.concat(frames_to_merge, ignore_index=True)
                combined = combined.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="first")
            else:
                combined = pd.DataFrame(columns=["timestamp", "funding_rate", "symbol", "source"])

            # Persist final per-month coverage (archive + API fallback) for downstream stages.
            for spec in month_specs:
                out_path = spec["out_path"]
                range_start = spec["range_start"]
                range_end_exclusive = spec["range_end_exclusive"]
                expected_ts_month = spec["expected_ts_month"]
                if combined.empty:
                    month_data = pd.DataFrame(columns=["timestamp", "funding_rate", "symbol", "source"])
                else:
                    month_data = combined[
                        (combined["timestamp"] >= range_start) & (combined["timestamp"] < range_end_exclusive)
                    ].copy()
                    month_data = month_data.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="first")
                if month_data.empty:
                    continue
                if not args.force and _partition_complete(out_path, expected_ts_month):
                    partitions_skipped.append(str(out_path))
                    continue
                ensure_dir(out_path.parent)
                written_path, storage = write_parquet(month_data, out_path)
                outputs.append(
                    {
                        "path": str(written_path),
                        "rows": int(len(month_data)),
                        "start_ts": month_data["timestamp"].min().isoformat(),
                        "end_ts": month_data["timestamp"].max().isoformat(),
                        "storage": storage,
                    }
                )
                partitions_written.append(str(written_path))

            missing_after_all = _missing_expected_timestamps(combined, expected_ts_all)

            coverage_start = combined["timestamp"].min().isoformat() if not combined.empty else None
            coverage_end = combined["timestamp"].max().isoformat() if not combined.empty else None

            stats["symbols"][symbol] = {
                "requested_start": args.start,
                "requested_end": args.end,
                "effective_start": effective_start.isoformat(),
                "effective_end": effective_end.isoformat(),
                "coverage_start": coverage_start,
                "coverage_end": coverage_end,
                "expected_count": len(expected_ts_all),
                "got_count": int(len(combined)) if not combined.empty else 0,
                "missing_count": len(missing_after_all),
                "missing_timestamps_preview": [t.isoformat() for t in missing_after_all[:200]],
                "archive_files_downloaded": archive_files_downloaded,
                "missing_archive_files": missing_archives,
                "api_calls": api_calls,
                "api_coverage_start": api_coverage_start,
                "api_coverage_end": api_coverage_end,
                "partitions_written": sorted(set(partitions_written)),
                "partitions_skipped": sorted(set(partitions_skipped)),
            }

        finalize_manifest(manifest, "success", stats=stats)
        return 0

    except Exception as exc:
        logging.exception("Funding ingestion failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
