from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlencode

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.config import load_configs
from pipelines._lib.io_utils import ensure_dir, write_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.validation import ensure_utc_timestamp

DEFAULT_BASE_URL = "https://fapi.binance.com"
DEFAULT_INTERVAL = "15m"
BASE_URL = "https://fapi.binance.com"
INTERVAL = "15m"


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _month_key(ts: datetime) -> Tuple[int, int]:
    return ts.year, ts.month


def _month_start(ts: datetime) -> datetime:
    return ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _next_month(ts: datetime) -> datetime:
    year = ts.year + (ts.month // 12)
    month = 1 if ts.month == 12 else ts.month + 1
    return ts.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)


def _utc_ms(ts: datetime) -> int:
    return int(ts.timestamp() * 1000)


def _build_session(max_retries: int, backoff_factor: float) -> requests.Session:
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


def _binance_get(
    session: requests.Session,
    base_url: str,
    path: str,
    params: Dict[str, str],
    timeout: int,
) -> List:
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
def _binance_get(path: str, params: Dict[str, str]) -> List:
    query = urlencode(params)
    url = f"{BASE_URL}{path}?{query}"
    with urlopen(url) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def _fetch_klines(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    rows = []
    limit = 1500
    cursor = start
    while cursor < end:
        params = {
            "symbol": symbol,
            "interval": interval,
            "interval": INTERVAL,
            "startTime": str(_utc_ms(cursor)),
            "endTime": str(_utc_ms(end)),
            "limit": str(limit),
        }
        data = _binance_get(session, base_url, "/fapi/v1/klines", params, timeout)
        data = _binance_get("/fapi/v1/klines", params)
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
def _fetch_funding(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
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
        data = _binance_get("/fapi/v1/fundingRate", params)
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


def _iter_months(start: datetime, end: datetime) -> List[datetime]:
    months = []
    cursor = _month_start(start)
    while cursor <= end:
        months.append(cursor)
        cursor = _next_month(cursor)
    return months


def _write_monthly(df: pd.DataFrame, base_dir: Path, month: datetime, prefix: str) -> Optional[Path]:
    if df.empty:
        return None
    year = month.year
    month_num = month.month
    path = base_dir / f"year={year}" / f"month={month_num:02d}" / f"{prefix}_{year}_{month_num:02d}.parquet"
    if path.exists():
        return path
    write_parquet(df, path)
    return path


def _checkpoint_path(base_dir: Path, month: datetime, prefix: str) -> Path:
    year = month.year
    month_num = month.month
    return (
        base_dir
        / "_checkpoints"
        / f"year={year}"
        / f"month={month_num:02d}"
        / f"{prefix}_checkpoint_{year}_{month_num:02d}.json"
    )


def _write_checkpoint(path: Path, payload: Dict[str, object]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
def _split_by_month(df: pd.DataFrame) -> Dict[Tuple[int, int], pd.DataFrame]:
    if df.empty:
        return {}
    df = df.copy()
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    grouped = {}
    for (year, month), group in df.groupby(["year", "month"], sort=True):
        grouped[(year, month)] = group.drop(columns=["year", "month"])
    return grouped


def _collect_stats(df: pd.DataFrame) -> Dict[str, object]:
    if df.empty:
        return {"rows": 0, "start_ts": None, "end_ts": None}
    return {
        "rows": int(len(df)),
        "start_ts": df["timestamp"].min().isoformat(),
        "end_ts": df["timestamp"].max().isoformat(),
    }


def main() -> int:
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
    inputs = []
    outputs = []
    if args.log_path:
        outputs.append({"path": args.log_path, "rows": None, "start_ts": None, "end_ts": None})

    try:
        base_url = config.get("base_url", DEFAULT_BASE_URL)
        interval = config.get("interval", DEFAULT_INTERVAL)
        timeout_seconds = int(config.get("timeout_seconds", 15))
        max_retries = int(config.get("max_retries", 5))
        backoff_factor = float(config.get("backoff_factor", 0.5))
        session = _build_session(max_retries=max_retries, backoff_factor=backoff_factor)

        for symbol in symbols:
    manifest = start_manifest(run_id, "ingest_binance_perp_15m", ["project/configs/pipeline.yaml"])
    inputs = []
    outputs = []

    try:
        for symbol in symbols:
            ohlcv_frames = []
            funding_frames = []
            for month_start in _iter_months(start, end):
                month_end = _next_month(month_start)
                fetch_start = max(start, month_start)
                fetch_end = min(end + timedelta(days=1), month_end)
                ohlcv_base = Path("project") / "lake" / "raw" / "binance" / "perp" / symbol / "ohlcv_15m"
                funding_base = Path("project") / "lake" / "raw" / "binance" / "perp" / symbol / "funding"
                ensure_dir(ohlcv_base)
                ensure_dir(funding_base)

                ohlcv_path = ohlcv_base / f"year={month_start.year}" / f"month={month_start.month:02d}" / f"ohlcv_{month_start.year}_{month_start.month:02d}.parquet"
                funding_path = funding_base / f"year={month_start.year}" / f"month={month_start.month:02d}" / f"funding_{month_start.year}_{month_start.month:02d}.parquet"
                ohlcv_checkpoint = _checkpoint_path(ohlcv_base, month_start, "ohlcv")
                funding_checkpoint = _checkpoint_path(funding_base, month_start, "funding")

                if ohlcv_path.exists() and funding_path.exists() and ohlcv_checkpoint.exists() and funding_checkpoint.exists():
                    continue

                ohlcv = _fetch_klines(
                    session,
                    base_url,
                    interval,
                    timeout_seconds,
                    symbol,
                    fetch_start,
                    fetch_end,
                )
                funding = _fetch_funding(
                    session,
                    base_url,
                    timeout_seconds,
                    symbol,
                    fetch_start,
                    fetch_end,
                )

                inputs.append({"path": f"api:binance:{symbol}:ohlcv", **_collect_stats(ohlcv)})
                inputs.append({"path": f"api:binance:{symbol}:funding", **_collect_stats(funding)})

                if not ohlcv.empty:
                    path = _write_monthly(ohlcv, ohlcv_base, month_start, "ohlcv")
                    if path:
                        outputs.append({"path": str(path), **_collect_stats(ohlcv)})
                    _write_checkpoint(
                        ohlcv_checkpoint,
                        {
                            "symbol": symbol,
                            "start": fetch_start.isoformat(),
                            "end": fetch_end.isoformat(),
                            "interval": interval,
                            "base_url": base_url,
                            "fetched_rows": int(len(ohlcv)),
                            "last_open_time": ohlcv["timestamp"].max().isoformat(),
                            "retrieved_at": datetime.now(timezone.utc).isoformat(),
                        },
                    )

                if not funding.empty:
                    path = _write_monthly(funding, funding_base, month_start, "funding")
                    if path:
                        outputs.append({"path": str(path), **_collect_stats(funding)})
                    _write_checkpoint(
                        funding_checkpoint,
                        {
                            "symbol": symbol,
                            "start": fetch_start.isoformat(),
                            "end": fetch_end.isoformat(),
                            "base_url": base_url,
                            "fetched_rows": int(len(funding)),
                            "last_funding_time": funding["timestamp"].max().isoformat(),
                            "retrieved_at": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                ohlcv_frames.append(_fetch_klines(symbol, fetch_start, fetch_end))
                funding_frames.append(_fetch_funding(symbol, fetch_start, fetch_end))
            ohlcv = pd.concat(ohlcv_frames, ignore_index=True) if ohlcv_frames else pd.DataFrame()
            funding = pd.concat(funding_frames, ignore_index=True) if funding_frames else pd.DataFrame()

            inputs.append({"path": f"api:binance:{symbol}:ohlcv", **_collect_stats(ohlcv)})
            inputs.append({"path": f"api:binance:{symbol}:funding", **_collect_stats(funding)})

            ohlcv_base = Path("project") / "lake" / "raw" / "binance" / "perp" / symbol / "ohlcv_15m"
            funding_base = Path("project") / "lake" / "raw" / "binance" / "perp" / symbol / "funding"
            ensure_dir(ohlcv_base)
            ensure_dir(funding_base)

            for (year, month), group in _split_by_month(ohlcv).items():
                month_marker = datetime(year, month, 1, tzinfo=timezone.utc)
                path = _write_monthly(group, ohlcv_base, month_marker, "ohlcv")
                if path:
                    outputs.append({"path": str(path), **_collect_stats(group)})

            for (year, month), group in _split_by_month(funding).items():
                month_marker = datetime(year, month, 1, tzinfo=timezone.utc)
                path = _write_monthly(group, funding_base, month_marker, "funding")
                if path:
                    outputs.append({"path": str(path), **_collect_stats(group)})

        finalize_manifest(manifest, inputs, outputs, "success")
        return 0
    except Exception as exc:
        finalize_manifest(manifest, inputs, outputs, "failed", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
