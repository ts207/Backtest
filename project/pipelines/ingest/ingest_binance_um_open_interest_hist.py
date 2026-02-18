from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from zipfile import ZipFile
from xml.etree import ElementTree as ET

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


DEFAULT_API_BASE = "https://fapi.binance.com"
DEFAULT_ARCHIVE_BASE = "https://data.binance.vision/data/futures/um"
DEFAULT_ARCHIVE_LIST_BASE = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
_ARCHIVE_KEY_DATE_RE = re.compile(r"-metrics-(\d{4}-\d{2}-\d{2})\.zip$")


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _month_start(ts: datetime) -> datetime:
    return ts.astimezone(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)


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
    out: List[datetime] = []
    cur = _month_start(start)
    while cur <= end:
        out.append(cur)
        cur = _next_month(cur)
    return out


def _iter_days(start: datetime, end_exclusive: datetime) -> Iterable[datetime]:
    cursor = start.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end_exclusive = end_exclusive.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    while cursor < end_exclusive:
        yield cursor
        cursor += timedelta(days=1)


def _read_csv_from_zip(path: Path) -> pd.DataFrame:
    with ZipFile(path) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            return pd.read_csv(f)


def _list_available_metric_day_set(symbol: str, list_base: str, session: requests.Session) -> set[str]:
    prefix = f"data/futures/um/daily/metrics/{symbol}/{symbol}-metrics-"
    marker: str | None = None
    out: set[str] = set()

    while True:
        params = {"prefix": prefix, "max-keys": "1000"}
        if marker:
            params["marker"] = marker

        resp = session.get(list_base, params=params, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

        contents = root.findall("s3:Contents", ns)
        for item in contents:
            key = item.find("s3:Key", ns)
            if key is None or not key.text:
                continue
            m = _ARCHIVE_KEY_DATE_RE.search(key.text)
            if m:
                out.add(m.group(1))

        is_truncated = (root.findtext("s3:IsTruncated", default="false", namespaces=ns) or "").lower() == "true"
        if not is_truncated:
            break

        next_marker = root.findtext("s3:NextMarker", default="", namespaces=ns)
        if next_marker:
            marker = next_marker
        elif contents:
            tail_key = contents[-1].find("s3:Key", ns)
            marker = tail_key.text if tail_key is not None else None
        else:
            break

    return out


def _normalize_archive_metrics(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=["timestamp", "symbol", "sum_open_interest", "sum_open_interest_value", "cmc_circulating_supply", "source"]
        )

    ts_col = None
    for candidate in ("create_time", "createTime", "timestamp", "time"):
        if candidate in df.columns:
            ts_col = candidate
            break
    if ts_col is None:
        ts_col = df.columns[0]

    out = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(df[ts_col], utc=True, errors="coerce"),
            "symbol": symbol,
            "sum_open_interest": pd.to_numeric(df.get("sum_open_interest", df.get("sumOpenInterest")), errors="coerce"),
            "sum_open_interest_value": pd.to_numeric(
                df.get("sum_open_interest_value", df.get("sumOpenInterestValue")), errors="coerce"
            ),
            "cmc_circulating_supply": pd.to_numeric(
                df.get("cmc_circulating_supply", df.get("CMCCirculatingSupply")), errors="coerce"
            ),
            "source": "archive_metrics_daily",
        }
    )
    out = out.dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)
    ensure_utc_timestamp(out["timestamp"], "timestamp")
    return out


def _fetch_open_interest_archive_metrics(
    session: requests.Session,
    archive_base: str,
    archive_list_base: str,
    symbol: str,
    start: datetime,
    end_exclusive: datetime,
    max_retries: int,
    retry_backoff_sec: float,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    day_set = _list_available_metric_day_set(symbol=symbol, list_base=archive_list_base, session=session)
    frames: List[pd.DataFrame] = []

    stats = {
        "available_days": int(len(day_set)),
        "requested_days": 0,
        "days_in_archive": 0,
        "downloads_attempted": 0,
        "downloads_ok": 0,
        "downloads_not_found": 0,
    }

    for day in _iter_days(start, end_exclusive):
        stats["requested_days"] += 1
        day_str = day.strftime("%Y-%m-%d")
        if day_set and day_str not in day_set:
            continue

        stats["days_in_archive"] += 1
        stats["downloads_attempted"] += 1
        url = join_url(archive_base, "daily", "metrics", symbol, f"{symbol}-metrics-{day_str}.zip")
        with tempfile.TemporaryDirectory() as tmpdir:
            out_zip = Path(tmpdir) / "metrics.zip"
            result = download_with_retries(
                url,
                out_zip,
                max_retries=max_retries,
                backoff_sec=retry_backoff_sec,
                session=session,
            )
            if result.status == "ok":
                try:
                    parsed = _normalize_archive_metrics(_read_csv_from_zip(out_zip), symbol=symbol)
                    if not parsed.empty:
                        frames.append(parsed)
                finally:
                    stats["downloads_ok"] += 1
            elif result.status == "not_found":
                stats["downloads_not_found"] += 1
            else:
                raise RuntimeError(f"Failed to download archive metrics {url}: {result.error}")

    out = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(
            columns=["timestamp", "symbol", "sum_open_interest", "sum_open_interest_value", "cmc_circulating_supply", "source"]
        )
    )
    if not out.empty:
        out = out[(out["timestamp"] >= start) & (out["timestamp"] < end_exclusive)].copy()
        out = out.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)
    return out, stats


def _fetch_open_interest_hist_api(
    session: requests.Session,
    api_base: str,
    symbol: str,
    period: str,
    start: datetime,
    end_exclusive: datetime,
    limit: int,
    sleep_sec: float,
    max_retries: int,
    retry_backoff_sec: float,
    timeout_sec: int,
) -> Tuple[pd.DataFrame, int]:
    url = join_url(api_base, "futures", "data", "openInterestHist")
    cursor = start.astimezone(timezone.utc)
    end_exclusive = end_exclusive.astimezone(timezone.utc)
    rows: List[dict[str, object]] = []
    api_calls = 0

    while cursor < end_exclusive:
        # Binance data API accepts at most ~30d windows.
        query_end = min(end_exclusive, cursor + timedelta(days=30))
        params = {
            "symbol": symbol,
            "period": period,
            "startTime": int(cursor.timestamp() * 1000),
            "endTime": int(query_end.timestamp() * 1000),
            "limit": int(limit),
        }

        last_error: str | None = None
        payload: object | None = None
        for attempt in range(max_retries + 1):
            try:
                resp = session.get(url, params=params, timeout=timeout_sec)
                api_calls += 1
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise RuntimeError(f"API error {resp.status_code}: {resp.text}")
                if resp.status_code != 200:
                    raise RuntimeError(f"API error {resp.status_code}: {resp.text}")
                payload = resp.json()
                break
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                if attempt >= max_retries:
                    break
                time.sleep(retry_backoff_sec * (2**attempt))

        if payload is None:
            raise RuntimeError(last_error or "openInterestHist API request failed")

        if not payload:
            break
        if not isinstance(payload, list):
            raise RuntimeError(f"Unexpected openInterestHist payload for {symbol}: {payload}")

        rows.extend(payload)
        ts_values = [int(row.get("timestamp", 0) or 0) for row in payload if row.get("timestamp") is not None]
        if not ts_values:
            break
        last_ts = max(ts_values)
        cursor = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc) + timedelta(milliseconds=1)
        if sleep_sec > 0:
            time.sleep(sleep_sec)

    if not rows:
        return (
            pd.DataFrame(
                columns=["timestamp", "symbol", "sum_open_interest", "sum_open_interest_value", "cmc_circulating_supply", "source"]
            ),
            api_calls,
        )

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(pd.to_numeric(df.get("timestamp"), errors="coerce"), unit="ms", utc=True, errors="coerce")
    df["symbol"] = symbol
    df["sum_open_interest"] = pd.to_numeric(df.get("sumOpenInterest"), errors="coerce")
    df["sum_open_interest_value"] = pd.to_numeric(df.get("sumOpenInterestValue"), errors="coerce")
    df["cmc_circulating_supply"] = pd.to_numeric(df.get("CMCCirculatingSupply"), errors="coerce")
    df["source"] = "api"
    out = df[["timestamp", "symbol", "sum_open_interest", "sum_open_interest_value", "cmc_circulating_supply", "source"]].copy()
    out = out.dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates(subset=["timestamp"])
    ensure_utc_timestamp(out["timestamp"], "timestamp")
    return out, api_calls


def _fetch_open_interest_hist(
    session: requests.Session,
    api_base: str,
    symbol: str,
    period: str,
    start: datetime,
    end_exclusive: datetime,
    limit: int,
    sleep_sec: float,
) -> Tuple[pd.DataFrame, int]:
    """
    Backward-compatible wrapper retained for tests and older callers.
    """
    return _fetch_open_interest_hist_api(
        session=session,
        api_base=api_base,
        symbol=symbol,
        period=period,
        start=start,
        end_exclusive=end_exclusive,
        limit=limit,
        sleep_sec=sleep_sec,
        max_retries=5,
        retry_backoff_sec=2.0,
        timeout_sec=30,
    )


def _partition_has_rows(path: Path) -> bool:
    target = path
    if not target.exists():
        csv_path = path.with_suffix(".csv")
        if csv_path.exists():
            target = csv_path
        else:
            return False
    try:
        df = read_parquet([target]) if target.suffix != ".csv" else pd.read_csv(target)
    except Exception:
        return False
    return int(len(df)) > 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ingest Binance USD-M open interest history (archive metrics + API tail in auto mode)"
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--period", default="5m")
    parser.add_argument("--api_base", default=DEFAULT_API_BASE)
    parser.add_argument("--archive_base", default=DEFAULT_ARCHIVE_BASE)
    parser.add_argument("--archive_list_base", default=DEFAULT_ARCHIVE_LIST_BASE)
    parser.add_argument("--ingest_mode", choices=["auto", "archive", "api"], default="auto")
    parser.add_argument("--api_history_days", type=int, default=30)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--sleep_sec", type=float, default=0.0)
    parser.add_argument("--max_retries", type=int, default=5)
    parser.add_argument("--retry_backoff_sec", type=float, default=2.0)
    parser.add_argument("--request_timeout_sec", type=int, default=30)
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--fail_if_no_data", type=int, default=1)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    run_id = args.run_id
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    if end < start:
        raise ValueError("end must be >= start")

    params = {
        "run_id": run_id,
        "symbols": symbols,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "period": args.period,
        "api_base": args.api_base,
        "archive_base": args.archive_base,
        "archive_list_base": args.archive_list_base,
        "ingest_mode": args.ingest_mode,
        "api_history_days": int(args.api_history_days),
        "limit": int(args.limit),
        "sleep_sec": float(args.sleep_sec),
        "max_retries": int(args.max_retries),
        "retry_backoff_sec": float(args.retry_backoff_sec),
        "request_timeout_sec": int(args.request_timeout_sec),
        "force": int(args.force),
        "fail_if_no_data": int(args.fail_if_no_data),
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("ingest_binance_um_open_interest_hist", run_id, params, inputs, outputs)

    stats: Dict[str, object] = {
        "symbols": {},
        "api_calls_total": 0,
        "ingest_mode": args.ingest_mode,
    }
    try:
        session = requests.Session()
        end_exclusive = end + timedelta(days=1)
        now_utc = datetime.now(timezone.utc)
        api_window_floor = now_utc - timedelta(days=max(1, int(args.api_history_days)))

        total_rows = 0
        total_written_parts = 0

        for symbol in symbols:
            logging.info("Open-interest ingest symbol=%s mode=%s period=%s", symbol, args.ingest_mode, args.period)

            source_frames: List[pd.DataFrame] = []
            symbol_stats: Dict[str, object] = {}

            use_archive = args.ingest_mode in {"auto", "archive"}
            if use_archive and str(args.period) != "5m":
                logging.warning(
                    "Archive metrics are 5m-only. Skipping archive for symbol=%s because period=%s",
                    symbol,
                    args.period,
                )
                use_archive = False

            if use_archive:
                archive_df, archive_stats = _fetch_open_interest_archive_metrics(
                    session=session,
                    archive_base=args.archive_base,
                    archive_list_base=args.archive_list_base,
                    symbol=symbol,
                    start=start,
                    end_exclusive=end_exclusive,
                    max_retries=args.max_retries,
                    retry_backoff_sec=args.retry_backoff_sec,
                )
                symbol_stats.update({f"archive_{k}": int(v) for k, v in archive_stats.items()})
                symbol_stats["archive_rows"] = int(len(archive_df))
                if not archive_df.empty:
                    source_frames.append(archive_df)

            api_calls = 0
            use_api = args.ingest_mode in {"auto", "api"}
            if use_api:
                api_start = max(start, api_window_floor)
                if api_start >= end_exclusive:
                    logging.info(
                        "Skipping API tail for symbol=%s because requested end=%s is older than API floor=%s",
                        symbol,
                        end.strftime("%Y-%m-%d"),
                        api_window_floor.strftime("%Y-%m-%d"),
                    )
                    api_df = pd.DataFrame(
                        columns=[
                            "timestamp",
                            "symbol",
                            "sum_open_interest",
                            "sum_open_interest_value",
                            "cmc_circulating_supply",
                            "source",
                        ]
                    )
                else:
                    api_df, api_calls = _fetch_open_interest_hist_api(
                        session=session,
                        api_base=args.api_base,
                        symbol=symbol,
                        period=args.period,
                        start=api_start,
                        end_exclusive=end_exclusive,
                        limit=args.limit,
                        sleep_sec=args.sleep_sec,
                        max_retries=args.max_retries,
                        retry_backoff_sec=args.retry_backoff_sec,
                        timeout_sec=args.request_timeout_sec,
                    )
                stats["api_calls_total"] = int(stats["api_calls_total"]) + int(api_calls)
                symbol_stats["api_calls"] = int(api_calls)
                symbol_stats["api_rows"] = int(len(api_df))
                if not api_df.empty:
                    source_frames.append(api_df)

            oi_df = (
                pd.concat(source_frames, ignore_index=True)
                if source_frames
                else pd.DataFrame(
                    columns=[
                        "timestamp",
                        "symbol",
                        "sum_open_interest",
                        "sum_open_interest_value",
                        "cmc_circulating_supply",
                        "source",
                    ]
                )
            )
            if not oi_df.empty:
                # Keep API row on overlap if both archive+api provided.
                oi_df = oi_df.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp").reset_index(drop=True)
                oi_df = oi_df[(oi_df["timestamp"] >= start) & (oi_df["timestamp"] < end_exclusive)].copy()

            written_parts = 0
            written_rows = 0
            for month_start in _iter_months(start, end):
                month_end = _next_month(month_start)
                part = (
                    oi_df[(oi_df["timestamp"] >= month_start) & (oi_df["timestamp"] < month_end)].copy()
                    if not oi_df.empty
                    else pd.DataFrame(columns=oi_df.columns)
                )
                if part.empty and not int(args.force):
                    continue
                out_dir = (
                    DATA_ROOT
                    / "lake"
                    / "raw"
                    / "binance"
                    / "perp"
                    / symbol
                    / "open_interest"
                    / args.period
                    / f"year={month_start.year}"
                    / f"month={month_start.month:02d}"
                )
                out_path = out_dir / f"open_interest_{symbol}_{args.period}_{month_start.year}-{month_start.month:02d}.parquet"
                if not int(args.force) and _partition_has_rows(out_path):
                    continue
                path_written, storage = write_parquet(part, out_path)
                outputs.append({"path": str(path_written), "rows": int(len(part)), "storage": storage})
                written_parts += 1
                written_rows += int(len(part))

            symbol_stats["rows"] = int(len(oi_df))
            symbol_stats["written_rows"] = int(written_rows)
            symbol_stats["written_partitions"] = int(written_parts)
            stats["symbols"][symbol] = symbol_stats

            total_rows += int(len(oi_df))
            total_written_parts += int(written_parts)

            logging.info(
                "Open-interest ingest symbol=%s rows=%d written_rows=%d written_partitions=%d api_calls=%d",
                symbol,
                int(len(oi_df)),
                int(written_rows),
                int(written_parts),
                int(api_calls),
            )

        if int(args.fail_if_no_data) and total_rows == 0 and total_written_parts == 0:
            raise RuntimeError(
                "No open-interest rows ingested. Requested range may be outside available Binance data. "
                "Try --ingest_mode archive --period 5m and/or --fail_if_no_data 0."
            )

        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:  # pragma: no cover
        logging.exception("Open interest ingest failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
