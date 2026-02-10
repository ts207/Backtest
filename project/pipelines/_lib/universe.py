from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set

import pandas as pd

from pipelines._lib.io_utils import choose_partition_dir, list_parquet_files, read_parquet, run_scoped_lake_path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
DEFAULT_TOP10_SEED = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "LTCUSDT",
]


def parse_symbols_arg(symbols_arg: str) -> List[str]:
    return [s.strip() for s in str(symbols_arg).split(",") if s.strip()]


def discover_available_symbols(data_root: Path, run_id: str) -> List[str]:
    run_scoped_root = run_scoped_lake_path(data_root, run_id, "cleaned", "perp")
    fallback_root = data_root / "lake" / "cleaned" / "perp"

    symbols: Set[str] = set()
    for root in (run_scoped_root, fallback_root):
        if not root.exists():
            continue
        for path in root.iterdir():
            if path.is_dir():
                symbols.add(path.name)
    return sorted(symbols)


def _load_symbol_cleaned_bars(data_root: Path, run_id: str, symbol: str) -> pd.DataFrame:
    candidates = [
        run_scoped_lake_path(data_root, run_id, "cleaned", "perp", symbol, "bars_15m"),
        data_root / "lake" / "cleaned" / "perp" / symbol / "bars_15m",
    ]
    bars_dir = choose_partition_dir(candidates)
    files = list_parquet_files(bars_dir) if bars_dir else []
    if not files:
        return pd.DataFrame()
    bars = read_parquet(files)
    if bars.empty:
        return bars
    if "timestamp" not in bars.columns or "close" not in bars.columns or "volume" not in bars.columns:
        return pd.DataFrame()
    bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True, format="mixed")
    bars = bars.dropna(subset=["timestamp", "close", "volume"]).copy()
    bars["close"] = pd.to_numeric(bars["close"], errors="coerce")
    bars["volume"] = pd.to_numeric(bars["volume"], errors="coerce")
    bars = bars.dropna(subset=["close", "volume"])
    if bars.empty:
        return bars
    bars = bars.sort_values("timestamp").reset_index(drop=True)
    bars["symbol"] = symbol
    bars["dollar_volume"] = bars["close"] * bars["volume"]
    return bars[["timestamp", "symbol", "dollar_volume"]]


def resolve_requested_symbols(
    symbols_arg: str,
    *,
    data_root: Path,
    run_id: str,
    seed_symbols: Sequence[str] | None = None,
) -> List[str]:
    parsed = parse_symbols_arg(symbols_arg)
    if parsed and all(s.upper() != "TOP10" for s in parsed):
        return parsed

    available = discover_available_symbols(data_root, run_id)
    seed = list(seed_symbols) if seed_symbols else list(DEFAULT_TOP10_SEED)
    prioritized = [s for s in seed if s in available]
    if len(prioritized) >= 10:
        return prioritized[:10]

    # Fill from remaining discovered symbols if seed symbols are missing.
    for symbol in available:
        if symbol not in prioritized:
            prioritized.append(symbol)
        if len(prioritized) >= 10:
            break
    return prioritized[:10]


def compute_monthly_top_n_symbols(
    *,
    data_root: Path,
    run_id: str,
    symbols: Iterable[str],
    top_n: int,
    lookback_days: int,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    fallback_seed: Sequence[str] | None = None,
) -> Dict[str, List[str]]:
    """
    Compute monthly top-N symbols by trailing lookback dollar volume.
    Returns mapping month_key (YYYY-MM) -> symbols.
    """
    frames: List[pd.DataFrame] = []
    symbols_list = [s for s in symbols if s]
    for symbol in symbols_list:
        frame = _load_symbol_cleaned_bars(data_root, run_id, symbol)
        if not frame.empty:
            frames.append(frame)

    if not frames:
        seed = list(fallback_seed) if fallback_seed else list(DEFAULT_TOP10_SEED)
        months = pd.period_range(start=start_ts.to_period("M"), end=end_ts.to_period("M"), freq="M")
        return {str(period): seed[:top_n] for period in months}

    full = pd.concat(frames, ignore_index=True)
    full["date"] = full["timestamp"].dt.floor("D")
    daily = full.groupby(["date", "symbol"], as_index=False)["dollar_volume"].sum()

    start_utc = pd.Timestamp(start_ts).tz_convert("UTC")
    end_utc = pd.Timestamp(end_ts).tz_convert("UTC")
    start_month = start_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0, nanosecond=0)
    end_month = end_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0, nanosecond=0)
    month_starts = pd.date_range(start=start_month, end=end_month, freq="MS", tz="UTC")
    monthly_map: Dict[str, List[str]] = {}

    seed = list(fallback_seed) if fallback_seed else list(DEFAULT_TOP10_SEED)
    for month_start in month_starts:
        window_start = month_start - timedelta(days=int(lookback_days))
        window = daily[(daily["date"] >= window_start) & (daily["date"] < month_start)]
        if window.empty:
            ranked = [s for s in seed if s in symbols_list][:top_n]
        else:
            ranked_df = (
                window.groupby("symbol", as_index=False)["dollar_volume"]
                .sum()
                .sort_values("dollar_volume", ascending=False)
            )
            ranked = ranked_df["symbol"].head(top_n).tolist()
            if len(ranked) < top_n:
                for s in seed:
                    if s in symbols_list and s not in ranked:
                        ranked.append(s)
                    if len(ranked) >= top_n:
                        break

        monthly_map[month_start.strftime("%Y-%m")] = ranked[:top_n]

    return monthly_map
