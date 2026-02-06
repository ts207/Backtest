from __future__ import annotations

import argparse
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


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _next_month(ts: datetime) -> datetime:
    year = ts.year + (ts.month // 12)
    month = 1 if ts.month == 12 else ts.month + 1
    return ts.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)


def _iter_months(start: datetime, end: datetime) -> List[datetime]:
    months: List[datetime] = []
    cursor = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
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


def _read_csv_from_zip(path: Path) -> pd.DataFrame:
    with ZipFile(path) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            return pd.read_csv(f)


def _read_headerless_csv_from_zip(path: Path) -> pd.DataFrame:
    with ZipFile(path) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            return pd.read_csv(f, header=None)


def _to_utc_ts(series: pd.Series, *, unit: str | None = None) -> pd.Series:
    if unit is not None:
        ts = pd.to_datetime(pd.to_numeric(series, errors="coerce"), unit=unit, utc=True, errors="coerce")
    else:
        ts = pd.to_datetime(series, utc=True, errors="coerce")
    return ts


def _parse_aggtrades(path: Path, symbol: str) -> pd.DataFrame:
    df = _read_csv_from_zip(path)
    columns_lower = {str(c).lower(): c for c in df.columns}
    if "p" in columns_lower and "q" in columns_lower and "t" in columns_lower:
        price_col = columns_lower["p"]
        qty_col = columns_lower["q"]
        ts_col = columns_lower["t"]
    elif {"price", "quantity", "transact_time"}.issubset(columns_lower):
        price_col = columns_lower["price"]
        qty_col = columns_lower["quantity"]
        ts_col = columns_lower["transact_time"]
    elif {"price", "qty", "time"}.issubset(columns_lower):
        price_col = columns_lower["price"]
        qty_col = columns_lower["qty"]
        ts_col = columns_lower["time"]
    else:
        # Binance dump fallback (headerless): [aggId, price, qty, firstId, lastId, T, m]
        df = _read_headerless_csv_from_zip(path)
        if df.shape[1] < 6:
            raise ValueError(f"Unexpected aggTrades schema in {path}")
        price_col = df.columns[1]
        qty_col = df.columns[2]
        ts_col = df.columns[5]

    out = pd.DataFrame(
        {
            "ts": _to_utc_ts(df[ts_col], unit="ms"),
            "symbol": symbol,
            "price": pd.to_numeric(df[price_col], errors="coerce"),
            "size": pd.to_numeric(df[qty_col], errors="coerce"),
        }
    )
    out = out.dropna(subset=["ts", "price", "size"])
    out = out[(out["price"] > 0.0) & (out["size"] > 0.0)]
    ensure_utc_timestamp(out["ts"], "ts")
    return out


def _parse_bookticker(path: Path, symbol: str) -> pd.DataFrame:
    df = _read_csv_from_zip(path)
    columns_lower = {str(c).lower(): c for c in df.columns}

    price_keys = [
        ("b", "a", "b", "a"),
        ("bidprice", "askprice", "bidqty", "askqty"),
        ("bid_price", "ask_price", "bid_qty", "ask_qty"),
    ]
    ts_candidates = ["e", "eventtime", "event_time", "t", "transactiontime", "transaction_time", "time"]
    bid_col = ask_col = bid_sz_col = ask_sz_col = None
    for bp, ap, bs, ass in price_keys:
        if {bp, ap, bs, ass}.issubset(columns_lower):
            bid_col = columns_lower[bp]
            ask_col = columns_lower[ap]
            bid_sz_col = columns_lower[bs]
            ask_sz_col = columns_lower[ass]
            break

    ts_col = None
    for candidate in ts_candidates:
        if candidate in columns_lower:
            ts_col = columns_lower[candidate]
            break

    if bid_col is None or ask_col is None or bid_sz_col is None or ask_sz_col is None:
        # Binance dump fallback (headerless): [updateId,bidPx,bidSz,askPx,askSz,T,E]
        df = _read_headerless_csv_from_zip(path)
        if df.shape[1] < 6:
            raise ValueError(f"Unexpected bookTicker schema in {path}")
        bid_col = df.columns[1]
        bid_sz_col = df.columns[2]
        ask_col = df.columns[3]
        ask_sz_col = df.columns[4]
        ts_col = df.columns[6] if df.shape[1] > 6 else df.columns[5]

    out = pd.DataFrame(
        {
            "ts": _to_utc_ts(df[ts_col], unit="ms"),
            "symbol": symbol,
            "bid_px": pd.to_numeric(df[bid_col], errors="coerce"),
            "ask_px": pd.to_numeric(df[ask_col], errors="coerce"),
            "bid_sz": pd.to_numeric(df[bid_sz_col], errors="coerce"),
            "ask_sz": pd.to_numeric(df[ask_sz_col], errors="coerce"),
        }
    )
    out = out.dropna(subset=["ts", "bid_px", "ask_px", "bid_sz", "ask_sz"])
    out = out[(out["bid_px"] > 0.0) & (out["ask_px"] > 0.0) & (out["ask_px"] >= out["bid_px"])]
    ensure_utc_timestamp(out["ts"], "ts")
    return out


def _download_partition(
    session: requests.Session,
    base: str,
    stream: str,
    symbol: str,
    month: datetime,
    month_start: datetime,
    month_end_exclusive: datetime,
    req_start: datetime,
    req_end_exclusive: datetime,
    *,
    max_retries: int,
    backoff_sec: float,
) -> list[Path]:
    covered_start = max(month_start, req_start)
    covered_end_exclusive = min(month_end_exclusive, req_end_exclusive)
    req_days = _iter_days(covered_start, covered_end_exclusive - timedelta(days=1))
    use_monthly = covered_start == month_start and covered_end_exclusive == month_end_exclusive

    monthly_url = join_url(base, "monthly", stream, symbol, f"{symbol}-{stream}-{month.year}-{month.month:02d}.zip")
    daily_urls = [
        join_url(base, "daily", stream, symbol, f"{symbol}-{stream}-{d.year}-{d.month:02d}-{d.day:02d}.zip")
        for d in req_days
    ]
    urls = [monthly_url] + daily_urls if use_monthly else daily_urls

    files: list[Path] = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for i, url in enumerate(urls):
            out_zip = Path(tmpdir) / f"{stream}_{i}.zip"
            result = download_with_retries(
                url,
                out_zip,
                max_retries=max_retries,
                backoff_sec=backoff_sec,
                session=session,
            )
            if result.status == "ok":
                persist_path = Path(tmpdir) / f"persist_{stream}_{i}.zip"
                out_zip.replace(persist_path)
                files.append(persist_path)
                if use_monthly:
                    break
        # copy back to stable temp outside context
        stable: list[Path] = []
        for i, f in enumerate(files):
            target = Path(tempfile.gettempdir()) / f"abma_{symbol}_{stream}_{month:%Y%m}_{i}.zip"
            f.replace(target)
            stable.append(target)
        return stable


def _build_utc_daily_calendar(start: datetime, end: datetime) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for d in _iter_days(start, end):
        open_ts = d.replace(hour=0, minute=0, second=0, microsecond=0)
        close_ts = open_ts + timedelta(days=1) - timedelta(seconds=1)
        rows.append(
            {
                "session_date": open_ts.date(),
                "exchange_tz": "UTC",
                "open_ts": pd.Timestamp(open_ts),
                "close_ts": pd.Timestamp(close_ts),
            }
        )
    return pd.DataFrame(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest Binance USD-M aggTrades + bookTicker for ABMA v1")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True, help="YYYY-MM-DD UTC")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD UTC")
    parser.add_argument("--out_root", default="")
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

    out_root = Path(args.out_root) if args.out_root else run_scoped_lake_path(DATA_ROOT, run_id, "microstructure")
    ensure_dir(out_root)
    trades_root = out_root / "trades"
    quotes_root = out_root / "quotes"
    calendar_root = out_root / "session_calendar"
    ensure_dir(trades_root)
    ensure_dir(quotes_root)
    ensure_dir(calendar_root)

    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    params = {
        "symbols": symbols,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "archive_base": ARCHIVE_BASE,
        "out_root": str(out_root),
    }
    manifest = start_manifest("ingest_binance_um_abma_l1", run_id, params, inputs, outputs)
    stats: Dict[str, object] = {"symbols": {}}

    try:
        months = _iter_months(start, end)
        session = requests.Session()
        for symbol in symbols:
            symbol_trades: List[pd.DataFrame] = []
            symbol_quotes: List[pd.DataFrame] = []
            for month in months:
                month_start = month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                month_end_exclusive = _next_month(month_start)
                logging.info("Downloading ABMA microstructure archives symbol=%s month=%s", symbol, month.strftime("%Y-%m"))

                agg_paths = _download_partition(
                    session,
                    ARCHIVE_BASE,
                    "aggTrades",
                    symbol,
                    month,
                    month_start,
                    month_end_exclusive,
                    start,
                    end + timedelta(days=1),
                    max_retries=args.max_retries,
                    backoff_sec=args.retry_backoff_sec,
                )
                book_paths = _download_partition(
                    session,
                    ARCHIVE_BASE,
                    "bookTicker",
                    symbol,
                    month,
                    month_start,
                    month_end_exclusive,
                    start,
                    end + timedelta(days=1),
                    max_retries=args.max_retries,
                    backoff_sec=args.retry_backoff_sec,
                )

                for p in agg_paths:
                    try:
                        symbol_trades.append(_parse_aggtrades(p, symbol))
                    finally:
                        if p.exists():
                            p.unlink()
                for p in book_paths:
                    try:
                        symbol_quotes.append(_parse_bookticker(p, symbol))
                    finally:
                        if p.exists():
                            p.unlink()

            trades_df = pd.concat(symbol_trades, ignore_index=True) if symbol_trades else pd.DataFrame(columns=["ts", "symbol", "price", "size"])
            quotes_df = pd.concat(symbol_quotes, ignore_index=True) if symbol_quotes else pd.DataFrame(columns=["ts", "symbol", "bid_px", "ask_px", "bid_sz", "ask_sz"])

            trades_df = trades_df[(trades_df["ts"] >= start) & (trades_df["ts"] < end + timedelta(days=1))]
            quotes_df = quotes_df[(quotes_df["ts"] >= start) & (quotes_df["ts"] < end + timedelta(days=1))]
            trades_df = trades_df.sort_values("ts").drop_duplicates(subset=["ts", "symbol", "price", "size"]).reset_index(drop=True)
            quotes_df = quotes_df.sort_values("ts").drop_duplicates(subset=["ts", "symbol", "bid_px", "ask_px", "bid_sz", "ask_sz"]).reset_index(drop=True)

            trades_path, trades_fmt = write_parquet(trades_df, trades_root / f"trades_{symbol}.parquet")
            quotes_path, quotes_fmt = write_parquet(quotes_df, quotes_root / f"quotes_{symbol}.parquet")
            outputs.append({"path": str(trades_path), "rows": int(len(trades_df)), "format": trades_fmt})
            outputs.append({"path": str(quotes_path), "rows": int(len(quotes_df)), "format": quotes_fmt})
            stats["symbols"][symbol] = {"trades_rows": int(len(trades_df)), "quotes_rows": int(len(quotes_df))}

        calendar = _build_utc_daily_calendar(start, end)
        cal_path, cal_fmt = write_parquet(calendar, calendar_root / "session_calendar.parquet")
        outputs.append({"path": str(cal_path), "rows": int(len(calendar)), "format": cal_fmt})
        stats["calendar_rows"] = int(len(calendar))

        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:  # noqa: BLE001
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
