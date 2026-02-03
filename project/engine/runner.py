from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from engine.pnl import compute_pnl, compute_returns
from pipelines._lib.io_utils import ensure_dir, list_parquet_files, read_parquet
from pipelines._lib.validation import ensure_utc_timestamp
from strategies.registry import get_strategy

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class StrategyResult:
    name: str
    data: pd.DataFrame


def _load_symbol_data(project_root: Path, symbol: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    features_dir = project_root / "lake" / "features" / "perp" / symbol / "15m" / "features_v1"
    bars_dir = project_root / "lake" / "cleaned" / "perp" / symbol / "bars_15m"
    feature_files = list_parquet_files(features_dir)
    bars_files = list_parquet_files(bars_dir)
    features = read_parquet(feature_files)
    bars = read_parquet(bars_files)
    if features.empty or bars.empty:
        raise ValueError(f"Missing data for {symbol}")

    features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)
    bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
    ensure_utc_timestamp(features["timestamp"], "timestamp")
    ensure_utc_timestamp(bars["timestamp"], "timestamp")

    features = features.sort_values("timestamp").reset_index(drop=True)
    bars = bars.sort_values("timestamp").reset_index(drop=True)
    return bars, features


def _validate_positions(series: pd.Series) -> None:
    if series.index.tz is None:
        raise ValueError("Positions index must be tz-aware UTC timestamps.")
    invalid = ~series.isin([-1, 0, 1])
    if invalid.any():
        bad_vals = series[invalid].unique()
        raise ValueError(f"Positions must be in {{-1,0,1}}. Found: {bad_vals}")


def _strategy_returns(
    symbol: str,
    bars: pd.DataFrame,
    features: pd.DataFrame,
    strategy_name: str,
    params: Dict[str, object],
    cost_bps: float,
) -> StrategyResult:
    strategy = get_strategy(strategy_name)
    positions = strategy.generate_positions(bars, features, params)
    timestamp_index = pd.DatetimeIndex(bars["timestamp"])
    positions = positions.reindex(timestamp_index).fillna(0).astype(int)
    _validate_positions(positions)

    close = bars.set_index("timestamp")["close"].astype(float)
    ret = compute_returns(close)
    pnl = compute_pnl(positions, ret, cost_bps)

    df = pd.DataFrame(
        {
            "timestamp": ret.index,
            "symbol": symbol,
            "pos": positions.values,
            "ret": ret.values,
            "pnl": pnl.values,
        }
    )
    return StrategyResult(name=strategy_name, data=df)


def _aggregate_strategy(results: Iterable[pd.DataFrame]) -> pd.DataFrame:
    combined = pd.concat(list(results), ignore_index=True)
    combined = combined.sort_values(["timestamp", "symbol"]).reset_index(drop=True)
    return combined


def _aggregate_portfolio(strategy_frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    per_strategy = {}
    for strategy_name, frame in strategy_frames.items():
        grouped = frame.groupby("timestamp", sort=True).agg(
            strategy_ret=("ret", "sum"),
            strategy_pnl=("pnl", "sum"),
        )
        grouped = grouped.sort_index()
        per_strategy[strategy_name] = grouped

    if not per_strategy:
        return pd.DataFrame(columns=["timestamp", "portfolio_ret", "portfolio_pnl"])

    aligned = pd.concat(per_strategy, axis=1).fillna(0.0)
    portfolio_ret = aligned.xs("strategy_ret", axis=1, level=1).sum(axis=1)
    portfolio_pnl = aligned.xs("strategy_pnl", axis=1, level=1).sum(axis=1)
    portfolio = pd.DataFrame(
        {
            "timestamp": portfolio_ret.index,
            "portfolio_ret": portfolio_ret.values,
            "portfolio_pnl": portfolio_pnl.values,
        }
    )
    return portfolio.sort_values("timestamp").reset_index(drop=True)


def _entry_count(frame: pd.DataFrame) -> int:
    pos = frame.set_index("timestamp")["pos"]
    prior = pos.shift(1).fillna(0)
    entries = ((prior == 0) & (pos != 0)).sum()
    return int(entries)


def _summarize_pnl(series: pd.Series) -> Dict[str, float]:
    total = float(series.sum())
    avg = float(series.mean()) if len(series) else 0.0
    std = float(series.std()) if len(series) else 0.0
    return {"total_pnl": total, "mean_pnl": avg, "std_pnl": std}


def run_engine(
    run_id: str,
    symbols: List[str],
    strategies: List[str],
    params: Dict[str, object],
    cost_bps: float,
    project_root: Path = PROJECT_ROOT,
) -> Dict[str, object]:
    engine_dir = project_root / "runs" / run_id / "engine"
    ensure_dir(engine_dir)

    strategy_frames: Dict[str, pd.DataFrame] = {}
    metrics: Dict[str, object] = {"strategies": {}}

    for strategy_name in strategies:
        symbol_results: List[pd.DataFrame] = []
        for symbol in symbols:
            bars, features = _load_symbol_data(project_root, symbol)
            result = _strategy_returns(symbol, bars, features, strategy_name, params, cost_bps)
            symbol_results.append(result.data)

        combined = _aggregate_strategy(symbol_results)
        strategy_frames[strategy_name] = combined
        out_path = engine_dir / f"strategy_returns_{strategy_name}.csv"
        combined.to_csv(out_path, index=False)

        pnl_series = combined.groupby("timestamp")["pnl"].sum()
        summary = _summarize_pnl(pnl_series)
        entries = sum(_entry_count(frame) for frame in symbol_results)
        metrics["strategies"][strategy_name] = {**summary, "entries": entries}

    portfolio = _aggregate_portfolio(strategy_frames)
    portfolio_path = engine_dir / "portfolio_returns.csv"
    portfolio.to_csv(portfolio_path, index=False)

    portfolio_summary = _summarize_pnl(portfolio["portfolio_pnl"]) if not portfolio.empty else _summarize_pnl(pd.Series(dtype=float))
    metrics["portfolio"] = portfolio_summary

    metrics_path = engine_dir / "metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "engine_dir": engine_dir,
        "strategy_frames": strategy_frames,
        "portfolio": portfolio,
        "metrics": metrics,
    }
