from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Make the project root importable.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.config import load_configs
from pipelines._lib.io_utils import ensure_dir, write_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.validation import ensure_utc_timestamp

# Default configuration constants.
DEFAULT_BASE_URL = "https://fapi.binance.com"
DEFAULT_INTERVAL = "15m"


def _parse_date(value: str) -> datetime:
    """Parse YYYY-MM-DD strings into timezone-aware UTC datetimes."""
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _utc_ms(ts: datetime) -> int:
    """Convert a datetime to milliseconds since epoch."""
    return int(ts.timestamp() * 1000)


def _build_session(max_retries: int, backoff_factor: float) -> requests.Session:
    """
    Build a requests session with retry logic.
    """
    session = requests.Session()
    retry = Retry(
        total=max_retries,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        backoff_factor=backoff_factor,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _binance_get(session: requests.Session, base_url: str, path: str, params: Dict[str, str], timeout: int) -> List:
    """
    Perform a GET request against the Binance API.
    """
    query = urlencode(params)
    url = f"{base_url}{path}?{query}"
    response = session.get(url, timeout=timeout)
    if response.status_code != 200:
        raise RuntimeError(f"Binance API error {response.status_code}: {response.text}")
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError(f"Unexpected response payload: {payload}")
    return payload


def _fetch_klines(
    session: requests.Session,
    base_url: str,
    interval: str,
    timeout: int,
    symbol: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """
    Fetch klines (candles) for a symbol from start to end.
    """
    rows = []
    limit = 1500
    cursor = start
    while cursor < end:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": str(_utc_ms(cursor)),
            "endTime": str(_utc_ms(end)),
            "limit": str(limit),
        }
        data = _binance_get(session, base_url, "/fapi/v1/klines", params, timeout)
        if not data:
            break
        rows.extend(data)
        last_open = data[-1][0]
        next_cursor = datetime.fromtimestamp(last_open / 1000, tz=timezone.utc) + timedelta(minutes=15)
        if next_cursor <= cursor:
            break
        cursor = next_cursor
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(
        rows,
        columns=[
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
        ],
    )
    df = df.drop(columns=["close_time", "ignore"])
    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df.drop(columns=["open_time"])
    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "trade_count",
        "taker_base_volume",
        "taker_quote_volume",
    ]
    df[numeric_cols] = df[numeric_cols].astype(float)
    df["symbol"] = symbol
    df = df[["timestamp", "symbol"] + numeric_cols]
    ensure_utc_timestamp(df["timestamp"], "timestamp")
    return df


def _fetch_funding(
    session: requests.Session,
    base_url: str,
    timeout: int,
    symbol: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """
    Fetch funding rates for a symbol from start to end.
    """
    rows = []
    limit = 1000
    cursor = start
    while cursor < end:
        params = {
            "symbol": symbol,
            "startTime": str(_utc_ms(cursor)),
            "endTime": str(_utc_ms(end)),
            "limit": str(limit),
        }
        data = _binance_get(session, base_url, "/fapi/v1/fundingRate", params, timeout)
        if not data:
            break
        rows.extend(data)
        last_time = int(data[-1]["fundingTime"])
        next_cursor = datetime.fromtimestamp(last_time / 1000, tz=timezone.utc) + timedelta(hours=8)
        if next_cursor <= cursor:
            break
        cursor = next_cursor
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)
    df["funding_rate"] = df["fundingRate"].astype(float)
    df["symbol"] = symbol
    df = df[["timestamp", "symbol", "funding_rate"]]
    ensure_utc_timestamp(df["timestamp"], "timestamp")
    return df


def _month_start(ts: datetime) -> datetime:
    """Get the first day of the month for a datetime."""
    return ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _next_month(ts: datetime) -> datetime:
    """Advance to the first day of the next month."""
    year = ts.year + (ts.month // 12)
    month = 1 if ts.month == 12 else ts.month + 1
    return ts.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)


def _iter_months(start: datetime, end: datetime) -> List[datetime]:
    """
    Generate a list of month start datetimes between start and end inclusive.
    """
    months: List[datetime] = []
    cursor = _month_start(start)
    while cursor <= end:
        months.append(cursor)
        cursor = _next_month(cursor)
    return months


def _split_by_month(df: pd.DataFrame) -> Dict[Tuple[int, int], pd.DataFrame]:
    """
    Split a DataFrame into groups keyed by (year, month).
    """
    if df.empty:
        return {}
    df = df.copy()
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    grouped: Dict[Tuple[int, int], pd.DataFrame] = {}
    for (year, month), group in df.groupby(["year", "month"], sort=True):
        grouped[(year, month)] = group.drop(columns=["year", "month"])
    return grouped


def _collect_stats(df: pd.DataFrame) -> Dict[str, object]:
    """
    Collect row count and start/end timestamps for a DataFrame.
    """
    if df.empty:
        return {"rows": 0, "start_ts": None, "end_ts": None}
    return {
        "rows": int(len(df)),
        "start_ts": df["timestamp"].min().isoformat(),
        "end_ts": df["timestamp"].max().isoformat(),
    }


def main() -> int:
    """
    Entry point for the ingestion stage.
    """
    parser = argparse.ArgumentParser(description="Ingest Binance perpetual 15m data")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    run_id = args.run_id
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    start = _parse_date(args.start)
    end = _parse_date(args.end)

    # Load configuration files.
    config_paths = ["project/configs/pipeline.yaml", "project/configs/venues/binance.yaml"]
    config_paths.extend(args.config)
    config = load_configs(config_paths)

    manifest = start_manifest(run_id, "ingest_binance_perp_15m", config_paths)
    manifest["parameters"] = {
        "base_url": config.get("base_url", DEFAULT_BASE_URL),
        "interval": config.get("interval", DEFAULT_INTERVAL),
        "timeout_seconds": config.get("timeout_seconds", 15),
        "max_retries": config.get("max_retries", 5),
        "backoff_factor": config.get("backoff_factor", 0.5),
        "symbols": symbols,
        "start": args.start,
        "end": args.end,
    }

    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    if args.log_path:
        outputs.append({"path": args.log_path, "rows": None, "start_ts": None, "end_ts": None})

    try:
        base_url = config.get("base_url", DEFAULT_BASE_URL)
        interval = config.get("interval", DEFAULT_INTERVAL)
        timeout_seconds = int(config.get("timeout_seconds", 15))
        max_retries = int(config.get("max_retries", 5))
        backoff_factor = float(config.get("backoff_factor", 0.5))
        session = _build_session(max_retries=max_retries, backoff_factor=backoff_factor)

        # Loop over symbols and months; save data and record stats.
        for symbol in symbols:
            ohlcv_frames: List[pd.DataFrame] = []
            funding_frames: List[pd.DataFrame] = []
            for month_start in _iter_months(start, end):
                month_end = _next_month(month_start)
                fetch_start = max(start, month_start)
                fetch_end = min(end + timedelta(days=1), month_end)

                # Paths for raw data storage.
                ohlcv_base = Path("project") / "lake" / "raw" / "binance" / "perp" / symbol / "ohlcv_15m"
                funding_base = Path("project") / "lake" / "raw" / "binance" / "perp" / symbol / "funding"
                ensure_dir(ohlcv_base)
                ensure_dir(funding_base)

                ohlcv_path = ohlcv_base / f"year={month_start.year}" / f"month={month_start.month:02d}" / f"ohlcv_{month_start.year}_{month_start.month:02d}.parquet"
                funding_path = funding_base / f"year={month_start.year}" / f"month={month_start.month:02d}" / f"funding_{month_start.year}_{month_start.month:02d}.parquet"

                # If both files exist, skip fetching.
                if ohlcv_path.exists() and funding_path.exists():
                    continue

                ohlcv = _fetch_klines(session, base_url, interval, timeout_seconds, symbol, fetch_start, fetch_end)
                funding = _fetch_funding(session, base_url, timeout_seconds, symbol, fetch_start, fetch_end)

                inputs.append({"path": f"api:binance:{symbol}:ohlcv", **_collect_stats(ohlcv)})
                inputs.append({"path": f"api:binance:{symbol}:funding", **_collect_stats(funding)})

                # Write monthly files if data is non-empty.
                if not ohlcv.empty:
                    ensure_dir(ohlcv_path.parent)
                    write_parquet(ohlcv, ohlcv_path)
                    outputs.append({"path": str(ohlcv_path), **_collect_stats(ohlcv)})
                if not funding.empty:
                    ensure_dir(funding_path.parent)
                    write_parquet(funding, funding_path)
                    outputs.append({"path": str(funding_path), **_collect_stats(funding)})

                ohlcv_frames.append(ohlcv)
                funding_frames.append(funding)

            # Consolidate all fetched data for the symbol, then split by month.
            ohlcv_all = pd.concat(ohlcv_frames, ignore_index=True) if ohlcv_frames else pd.DataFrame()
            funding_all = pd.concat(funding_frames, ignore_index=True) if funding_frames else pd.DataFrame()

            inputs.append({"path": f"api:binance:{symbol}:ohlcv", **_collect_stats(ohlcv_all)})
            inputs.append({"path": f"api:binance:{symbol}:funding", **_collect_stats(funding_all)})

            ohlcv_base_dir = Path("project") / "lake" / "raw" / "binance" / "perp" / symbol / "ohlcv_15m"
            funding_base_dir = Path("project") / "lake" / "raw" / "binance" / "perp" / symbol / "funding"
            ensure_dir(ohlcv_base_dir)
            ensure_dir(funding_base_dir)

            for (year, month), group in _split_by_month(ohlcv_all).items():
                out_path = ohlcv_base_dir / f"year={year}" / f"month={month:02d}" / f"ohlcv_{year}_{month:02d}.parquet"
                ensure_dir(out_path.parent)
                write_parquet(group, out_path)
                outputs.append({"path": str(out_path), **_collect_stats(group)})

            for (year, month), group in _split_by_month(funding_all).items():
                out_path = funding_base_dir / f"year={year}" / f"month={month:02d}" / f"funding_{year}_{month:02d}.parquet"
                ensure_dir(out_path.parent)
                write_parquet(group, out_path)
                outputs.append({"path": str(out_path), **_collect_stats(group)})

        finalize_manifest(manifest, inputs, outputs, "success")
        return 0
    except Exception as exc:
        finalize_manifest(manifest, inputs, outputs, "failed", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
