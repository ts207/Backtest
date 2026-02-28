"""
monitor_data_freshness.py
=========================
Checks that cleaned bar data for each symbol is sufficiently up-to-date.
Exits with code 1 if any symbol is stalier than --max_staleness_bars.

Usage:
    python project/scripts/monitor_data_freshness.py \\
        --symbols BTCUSDT,ETHUSDT \\
        --timeframe 5m \\
        --max_staleness_bars 3
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))

TIMEFRAME_MINUTES = {"1m": 1, "5m": 5, "15m": 15, "1h": 60}


def check_symbol(symbol: str, timeframe: str, max_staleness_bars: int) -> tuple[bool, str]:
    bar_dir = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / f"bars_{timeframe}"
    files = sorted(bar_dir.rglob("*.parquet")) if bar_dir.exists() else []
    if not files:
        return False, f"{symbol}: no cleaned bars found at {bar_dir}"

    try:
        df = pd.read_parquet(files[-1], columns=["timestamp"])
        if df.empty:
            return False, f"{symbol}: last parquet file is empty"
        latest_ts = pd.to_datetime(df["timestamp"], utc=True).max()
    except Exception as exc:
        return False, f"{symbol}: failed to read latest parquet — {exc}"

    now = pd.Timestamp.utcnow()
    interval_min = TIMEFRAME_MINUTES.get(timeframe, 5)
    staleness_bars = int((now - latest_ts).total_seconds() / 60 / interval_min)
    ok = staleness_bars <= max_staleness_bars
    status = "OK" if ok else "STALE"
    msg = (
        f"{symbol}: {status} | latest_bar={latest_ts.isoformat()} "
        f"| staleness={staleness_bars} bars | max={max_staleness_bars}"
    )
    return ok, msg


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor cleaned bar data freshness.")
    parser.add_argument("--symbols", required=True, help="Comma-separated symbol list")
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--max_staleness_bars", type=int, default=3)
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    all_ok = True
    for symbol in symbols:
        ok, msg = check_symbol(symbol, args.timeframe, args.max_staleness_bars)
        level = "INFO" if ok else "ERROR"
        print(f"[freshness][{level}] {msg}", file=sys.stdout if ok else sys.stderr)
        if not ok:
            all_ok = False
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
