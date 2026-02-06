from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List
from zipfile import ZipFile

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.http_utils import download_with_retries
from pipelines._lib.io_utils import ensure_dir, run_scoped_lake_path, write_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.url_utils import join_url
from pipelines._lib.validation import ensure_utc_timestamp


ARCHIVE_BASE = "https://data.binance.vision/data/futures/um"
SCHEMA_VERSION = "v1"


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


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _infer_epoch_unit(ts_series: pd.Series) -> str:
    vals = pd.to_numeric(ts_series, errors="coerce").dropna().astype("int64")
    if vals.empty:
        return "ms"
    med = int(vals.median())
    return "s" if med < 1_000_000_000_000 else "ms"


def _snap_to_8h_grid(ts: pd.Series) -> pd.Series:
    ts = pd.to_datetime(ts, utc=True).dt.round("1s")
    secs = (ts.view("int64") // 1_000_000_000).astype("int64")
    snap = ((secs + 4 * 3600) // (8 * 3600)) * (8 * 3600)
    return pd.to_datetime(snap, unit="s", utc=True)


def _read_funding_from_zip(path: Path, symbol: str, source: str) -> pd.DataFrame:
    with ZipFile(path) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            df = pd.read_csv(f)

            columns = {str(col).lower(): col for col in df.columns}
            ts_col = None

            for candidate in ["fundingtime", "funding_time", "calctime", "calc_time", "timestamp", "time"]:
                if candidate in columns:
                    ts_col = columns[candidate]
                    break

            if ts_col is None:
                f.seek(0)
                df = pd.read_csv(f, header=None)
                if df.shape[1] < 2:
                    raise ValueError(f"Unexpected fundingRate CSV format (cols={df.shape[1]}) in {csv_name}")
                ts_col = df.columns[0]

    df["_ts"] = pd.to_numeric(df[ts_col], errors="coerce")
    df = df.loc[df["_ts"].notna()].copy()
    unit = _infer_epoch_unit(df["_ts"])
    ts = pd.to_datetime(df["_ts"].astype("int64"), unit=unit, utc=True)
    ts = _snap_to_8h_grid(ts)

    out = pd.DataFrame(
        {
            "ts": ts,
            "symbol": symbol,
            "event_type": "FUNDING",
            "source": source,
        }
    )
    ensure_utc_timestamp(out["ts"], "ts")
    out = out.sort_values("ts").drop_duplicates(subset=["ts"])
    return out


def _download_partition(
    session: requests.Session,
    symbol: str,
    month_start: datetime,
    month_end_exclusive: datetime,
    req_start: datetime,
    req_end_exclusive: datetime,
    *,
    max_retries: int,
    backoff_sec: float,
) -> List[dict[str, object]]:
    covered_start = max(month_start, req_start)
    covered_end_exclusive = min(month_end_exclusive, req_end_exclusive)
    req_days = _iter_days(covered_start, covered_end_exclusive - timedelta(days=1))
    use_monthly = covered_start == month_start and covered_end_exclusive == month_end_exclusive

    monthly_url = join_url(ARCHIVE_BASE, "monthly", "fundingRate", symbol, f"{symbol}-fundingRate-{month_start.year}-{month_start.month:02d}.zip")
    daily_urls = [
        join_url(ARCHIVE_BASE, "daily", "fundingRate", symbol, f"{symbol}-fundingRate-{d.year}-{d.month:02d}-{d.day:02d}.zip")
        for d in req_days
    ]
    urls = [monthly_url] + daily_urls if use_monthly else daily_urls

    results: List[dict[str, object]] = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for i, url in enumerate(urls):
            out_zip = Path(tmpdir) / f"funding_{i}.zip"
            result = download_with_retries(
                url,
                out_zip,
                max_retries=max_retries,
                backoff_sec=backoff_sec,
                session=session,
            )
            if result.status == "ok":
                results.append({"url": url, "path": out_zip})
                if use_monthly:
                    break
            elif result.status == "not_found":
                results.append({"url": url, "path": None})
            else:
                raise RuntimeError(f"Failed to download {url}: {result.error}")

        resolved: List[dict[str, object]] = []
        for entry in results:
            if entry["path"] is None:
                resolved.append(entry)
                continue
            persist = Path(tempfile.gettempdir()) / f"funding_{symbol}_{month_start:%Y%m}_{hash(entry['url']) & 0xFFFF}.zip"
            Path(entry["path"]).replace(persist)
            resolved.append({"url": entry["url"], "path": persist})
        return resolved


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest Binance USD-M funding events (timestamps only)")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--max_retries", type=int, default=5)
    parser.add_argument("--retry_backoff_sec", type=float, default=2.0)
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

    funding_root = run_scoped_lake_path(DATA_ROOT, run_id, "microstructure", "funding")
    ensure_dir(funding_root)

    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    params = {
        "symbols": symbols,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "archive_base": ARCHIVE_BASE,
        "schema_version": SCHEMA_VERSION,
    }
    manifest = start_manifest("ingest_binance_um_funding_events", run_id, params, inputs, outputs)
    stats: Dict[str, object] = {"symbols": {}}

    try:
        session = requests.Session()
        all_sources: List[dict[str, object]] = []

        for symbol in symbols:
            frames: List[pd.DataFrame] = []
            archive_entries: List[dict[str, object]] = []
            for month_start in _iter_months(start, end):
                month_end_exclusive = _next_month(month_start)
                logging.info("Downloading funding events symbol=%s month=%s", symbol, month_start.strftime("%Y-%m"))
                downloads = _download_partition(
                    session,
                    symbol,
                    month_start,
                    month_end_exclusive,
                    start,
                    end + timedelta(days=1),
                    max_retries=args.max_retries,
                    backoff_sec=args.retry_backoff_sec,
                )
                for entry in downloads:
                    url = entry["url"]
                    path = entry["path"]
                    if path is None:
                        continue
                    sha = _hash_file(path)
                    archive_entries.append({"url": url, "sha256": sha})
                    try:
                        frames.append(_read_funding_from_zip(path, symbol, "archive"))
                    finally:
                        if path.exists():
                            path.unlink()

            if frames:
                data = pd.concat(frames, ignore_index=True)
                data = data.sort_values("ts").drop_duplicates(subset=["ts"])
            else:
                data = pd.DataFrame(columns=["ts", "symbol", "event_type", "source"])

            data = data[(data["ts"] >= start) & (data["ts"] < end + timedelta(days=1))].copy()
            if not data.empty:
                if data["ts"].duplicated().any():
                    raise ValueError(f"Duplicate funding timestamps for {symbol}")
                if not data["ts"].is_monotonic_increasing:
                    data = data.sort_values("ts")
                    if not data["ts"].is_monotonic_increasing:
                        raise ValueError(f"Funding timestamps not sorted for {symbol}")

            out_path, storage = write_parquet(data, funding_root / f"funding_events_{symbol}.parquet")
            outputs.append({"path": str(out_path), "rows": int(len(data)), "storage": storage})
            stats["symbols"][symbol] = {
                "rows": int(len(data)),
                "start_ts": data["ts"].min().isoformat() if not data.empty else None,
                "end_ts": data["ts"].max().isoformat() if not data.empty else None,
            }
            all_sources.extend(archive_entries)

        manifest_payload = {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "symbols": symbols,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "sources": all_sources,
        }
        (funding_root / "manifest.json").write_text(json.dumps(manifest_payload, indent=2, sort_keys=True), encoding="utf-8")

        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:  # noqa: BLE001
        logging.exception("Funding events ingest failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
