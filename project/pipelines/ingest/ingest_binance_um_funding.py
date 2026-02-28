from __future__ import annotations

import argparse
import io
import logging
import os
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir, run_scoped_lake_path, write_parquet  # type: ignore


BINANCE_VISION_BASE = "https://data.binance.vision"


@dataclass(frozen=True)
class MonthKey:
    year: int
    month: int  # 1..12

    def yyyymm(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def _month_range(start: pd.Timestamp, end_exclusive: pd.Timestamp) -> List[MonthKey]:
    cur = pd.Timestamp(start.year, start.month, 1, tz="UTC")
    out: List[MonthKey] = []
    while cur < end_exclusive:
        out.append(MonthKey(cur.year, cur.month))
        cur = (cur + pd.offsets.MonthBegin(1)).tz_convert("UTC")
    return out


def _download_zip(url: str, *, timeout: int = 60) -> bytes:
    r = requests.get(url, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"Failed download: status={r.status_code} url={url}")
    return r.content


def _read_zip_csv(zip_bytes: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not names:
            raise RuntimeError("Zip contains no CSV")
        # Binance monthly archives usually contain exactly one CSV
        with zf.open(names[0]) as f:
            return pd.read_csv(f)


def _infer_ts_unit(x: pd.Series) -> str:
    # seconds ~1e9, ms ~1e12, us ~1e15, ns ~1e18
    v = float(pd.to_numeric(x, errors="coerce").dropna().iloc[0])
    if v > 1e17:
        return "ns"
    if v > 1e14:
        return "us"
    if v > 1e11:
        return "ms"
    return "s"


def _normalize_funding_df(df: pd.DataFrame, *, symbol: str) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.rename(columns={c: c.strip() for c in df.columns})

    # Binance Vision schema variants:
    #   classic: fundingTime, fundingRate, markPrice
    #   newer:   calc_time, last_funding_rate, funding_interval_hours
    ts_col = None
    for c in ("timestamp", "fundingTime", "funding_time", "time", "calc_time", "calcTime"):
        if c in df.columns:
            ts_col = c
            break
    if ts_col is None:
        raise ValueError(f"Funding CSV missing timestamp column for {symbol}. cols={list(df.columns)}")

    rate_col = None
    for c in ("funding_rate_scaled", "fundingRate", "funding_rate", "last_funding_rate", "funding"):
        if c in df.columns:
            rate_col = c
            break
    if rate_col is None:
        raise ValueError(f"Funding CSV missing rate column for {symbol}. cols={list(df.columns)}")

    ts_raw = pd.to_numeric(df[ts_col], errors="coerce")
    ts_unit = _infer_ts_unit(ts_raw.dropna())
    ts = pd.to_datetime(ts_raw, unit=ts_unit, utc=True, errors="coerce").astype("datetime64[ns, UTC]")

    build: dict = {
        "timestamp": ts,
        "funding_rate_scaled": pd.to_numeric(df[rate_col], errors="coerce"),
    }
    # Preserve interval column when present (interval can vary on Binance)
    if "funding_interval_hours" in df.columns:
        build["funding_interval_hours"] = pd.to_numeric(df["funding_interval_hours"], errors="coerce")

    out = pd.DataFrame(build).dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    # Drop duplicate timestamps (keep last)
    if out["timestamp"].duplicated(keep=False).any():
        out = out.drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)

    return out


def _funding_month_url(symbol: str, mk: MonthKey) -> str:
    # Binance Vision: futures/um/monthly/fundingRate/<SYMBOL>/<SYMBOL>-fundingRate-YYYY-MM.zip
    return (
        f"{BINANCE_VISION_BASE}/data/futures/um/monthly/fundingRate/"
        f"{symbol}/{symbol}-fundingRate-{mk.yyyymm()}.zip"
    )


def _write_raw_funding(
    df: pd.DataFrame,
    *,
    run_id: str,
    symbol: str,
    mk: MonthKey,
) -> None:
    # run-scoped raw path
    run_raw_root = run_scoped_lake_path(
        DATA_ROOT, run_id, "raw", "binance", "perp", symbol, "fundingRate", f"year={mk.year:04d}", f"month={mk.month:02d}"
    )
    # compat raw path
    compat_raw_root = (
        DATA_ROOT
        / "lake"
        / "raw"
        / "binance"
        / "perp"
        / symbol
        / "fundingRate"
        / f"year={mk.year:04d}"
        / f"month={mk.month:02d}"
    )

    fname = f"fundingRate_{symbol}_{mk.yyyymm()}.parquet"

    run_path = run_raw_root / fname
    compat_path = compat_raw_root / fname

    ensure_dir(run_path.parent)
    ensure_dir(compat_path.parent)

    write_parquet(df, run_path)
    write_parquet(df, compat_path)

    logging.info("Wrote %d rows to %s", len(df), run_path)
    logging.info("Wrote %d rows to %s", len(df), compat_path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest Binance UM perpetual funding rates (monthly archives)")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True, help="Comma-separated, e.g. BTCUSDT,ETHUSDT")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive day range like other stages)")
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    start = pd.Timestamp(args.start, tz="UTC").floor("D")
    end_exclusive = pd.Timestamp(args.end, tz="UTC").floor("D") + pd.Timedelta(days=1)

    handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=handlers, format="%(asctime)s %(levelname)s %(message)s")

    months = _month_range(start, end_exclusive)

    for symbol in symbols:
        for mk in months:
            url = _funding_month_url(symbol, mk)
            logging.info("Downloading monthly archive %s", url)

            zip_bytes = _download_zip(url)
            raw_df = _read_zip_csv(zip_bytes)
            df = _normalize_funding_df(raw_df, symbol=symbol)

            # Restrict to requested range (still write monthly partition)
            df = df[(df["timestamp"] >= start) & (df["timestamp"] < end_exclusive)].copy()
            if df.empty:
                # Still OK: month exists but outside requested range or has no rows after filter.
                logging.info("No funding rows in requested window for %s %s; skipping write.", symbol, mk.yyyymm())
                continue

            _write_raw_funding(df, run_id=args.run_id, symbol=symbol, mk=mk)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())