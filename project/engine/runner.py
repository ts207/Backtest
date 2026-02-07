from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np

import pandas as pd

from engine.pnl import compute_pnl, compute_returns
from pipelines._lib.io_utils import (
    choose_partition_dir,
    ensure_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
)
from pipelines._lib.validation import ensure_utc_timestamp
from features.funding_persistence import FP_DEF_VERSION
from strategies.registry import get_strategy

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
LOGGER = logging.getLogger(__name__)


@dataclass
class StrategyResult:
    name: str
    data: pd.DataFrame
    diagnostics: Dict[str, object]


_CONTEXT_COLUMNS = [
    "fp_def_version",
    "fp_active",
    "fp_age_bars",
    "fp_event_id",
    "fp_enter_ts",
    "fp_exit_ts",
    "fp_severity",
    "fp_norm_due",
]


def _load_context_data(data_root: Path, symbol: str, run_id: str, timeframe: str = "15m") -> pd.DataFrame:
    context_candidates = [
        run_scoped_lake_path(data_root, run_id, "context", "funding_persistence", symbol),
        data_root / "features" / "context" / "funding_persistence" / symbol,
    ]
    context_dir = choose_partition_dir(context_candidates)
    if context_dir is None:
        context_dir = context_candidates[0]
    context_files = list_parquet_files(context_dir)
    if not context_files:
        return pd.DataFrame(columns=["timestamp", *_CONTEXT_COLUMNS])

    context = read_parquet(context_files)
    if context.empty:
        return pd.DataFrame(columns=["timestamp", *_CONTEXT_COLUMNS])

    if "timestamp" not in context.columns:
        raise ValueError(f"Context data missing timestamp for {symbol}: {context_dir}")

    context["timestamp"] = pd.to_datetime(context["timestamp"], utc=True)
    ensure_utc_timestamp(context["timestamp"], "timestamp")
    context = context.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
    if "fp_def_version" not in context.columns:
        context["fp_def_version"] = FP_DEF_VERSION

    for col in _CONTEXT_COLUMNS:
        if col not in context.columns:
            context[col] = np.nan

    if "fp_active" in context.columns:
        context["fp_active"] = pd.to_numeric(context["fp_active"], errors="coerce").fillna(0).astype("int64")
    if "fp_age_bars" in context.columns:
        context["fp_age_bars"] = pd.to_numeric(context["fp_age_bars"], errors="coerce").fillna(0).astype("int64")
    if "fp_norm_due" in context.columns:
        context["fp_norm_due"] = pd.to_numeric(context["fp_norm_due"], errors="coerce").fillna(0).astype("int64")
    return context[["timestamp", *_CONTEXT_COLUMNS]]


def _apply_context_defaults(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "fp_def_version" not in out.columns:
        out["fp_def_version"] = FP_DEF_VERSION
    out["fp_def_version"] = out["fp_def_version"].fillna(FP_DEF_VERSION)

    for col, default in [("fp_active", 0), ("fp_age_bars", 0), ("fp_norm_due", 0), ("fp_severity", 0.0), ("fp_event_id", None), ("fp_enter_ts", pd.NaT), ("fp_exit_ts", pd.NaT)]:
        if col not in out.columns:
            out[col] = default

    out["fp_active"] = pd.to_numeric(out["fp_active"], errors="coerce").fillna(0).astype("int64")
    out["fp_age_bars"] = pd.to_numeric(out["fp_age_bars"], errors="coerce").fillna(0).astype("int64")
    out["fp_norm_due"] = pd.to_numeric(out["fp_norm_due"], errors="coerce").fillna(0).astype("int64")

    inactive = out["fp_active"] == 0
    out.loc[inactive, "fp_age_bars"] = 0
    out.loc[inactive, "fp_event_id"] = None
    out.loc[inactive, "fp_enter_ts"] = pd.NaT
    out.loc[inactive, "fp_exit_ts"] = pd.NaT
    out["fp_severity"] = pd.to_numeric(out["fp_severity"], errors="coerce").fillna(0.0).astype("float64")
    out.loc[inactive, "fp_severity"] = 0.0
    return out


def _join_context_features(features: pd.DataFrame, context: pd.DataFrame) -> pd.DataFrame:
    joined = features.merge(context, on="timestamp", how="left", validate="one_to_one")
    return _apply_context_defaults(joined)



def _load_symbol_data(data_root: Path, symbol: str, run_id: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    feature_candidates = [
        run_scoped_lake_path(data_root, run_id, "features", "perp", symbol, "15m", "features_v1"),
        data_root / "lake" / "features" / "perp" / symbol / "15m" / "features_v1",
    ]
    bars_candidates = [
        run_scoped_lake_path(data_root, run_id, "cleaned", "perp", symbol, "bars_15m"),
        data_root / "lake" / "cleaned" / "perp" / symbol / "bars_15m",
    ]
    features_dir = choose_partition_dir(feature_candidates)
    bars_dir = choose_partition_dir(bars_candidates)
    feature_files = list_parquet_files(features_dir) if features_dir else []
    bars_files = list_parquet_files(bars_dir) if bars_dir else []
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

    context = _load_context_data(data_root, symbol, run_id=run_id, timeframe="15m")
    features = _join_context_features(features, context)
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
    required_features = getattr(strategy, "required_features", []) or []

    positions = strategy.generate_positions(bars, features, params)
    timestamp_index = pd.DatetimeIndex(bars["timestamp"])
    positions = positions.reindex(timestamp_index).fillna(0).astype(int)
    _validate_positions(positions)

    bars_indexed = bars.set_index("timestamp")
    close = bars_indexed["close"].astype(float)
    high = bars_indexed["high"].astype(float)
    low = bars_indexed["low"].astype(float)
    ret = compute_returns(close)
    nan_ret_mask = ret.isna()
    forced_flat_bars = int((nan_ret_mask & (positions != 0)).sum())
    positions = positions.mask(nan_ret_mask, 0).astype(int)
    pnl = compute_pnl(positions, ret, cost_bps)
    if nan_ret_mask.any():
        LOGGER.info(
            "Gap-safe returns forced flat %s bars for %s/%s.",
            forced_flat_bars,
            symbol,
            strategy_name,
        )

    features_indexed = features.set_index("timestamp")
    features_aligned = features_indexed.reindex(ret.index)
    missing_feature_columns = [col for col in required_features if col not in features_aligned.columns]
    if required_features and not missing_feature_columns:
        missing_feature_mask = features_aligned[required_features].isna().any(axis=1)
    else:
        missing_feature_mask = pd.Series(True if required_features else False, index=ret.index)

    high_96 = features_aligned["high_96"] if "high_96" in features_aligned.columns else pd.Series(index=ret.index, dtype=float)
    low_96 = features_aligned["low_96"] if "low_96" in features_aligned.columns else pd.Series(index=ret.index, dtype=float)

    df = pd.DataFrame(
        {
            "timestamp": ret.index,
            "symbol": symbol,
            "pos": positions.values,
            "ret": ret.values,
            "pnl": pnl.values,
            "close": close.reindex(ret.index).values,
            "high": high.reindex(ret.index).values,
            "low": low.reindex(ret.index).values,
            "high_96": high_96.values,
            "low_96": low_96.values,
            "fp_def_version": features_aligned["fp_def_version"].values if "fp_def_version" in features_aligned.columns else FP_DEF_VERSION,
            "fp_active": features_aligned["fp_active"].fillna(0).astype(int).values if "fp_active" in features_aligned.columns else np.zeros(len(ret), dtype=int),
            "fp_age_bars": features_aligned["fp_age_bars"].fillna(0).astype(int).values if "fp_age_bars" in features_aligned.columns else np.zeros(len(ret), dtype=int),
            "fp_norm_due": features_aligned["fp_norm_due"].fillna(0).astype(int).values if "fp_norm_due" in features_aligned.columns else np.zeros(len(ret), dtype=int),
        }
    )
    total_bars = int(len(ret))
    diagnostics = {
        "total_bars": total_bars,
        "nan_return_bars": int(nan_ret_mask.sum()),
        "forced_flat_bars": forced_flat_bars,
        "missing_feature_bars": int(missing_feature_mask.sum()),
        "nan_return_pct": float(nan_ret_mask.mean()) if total_bars else 0.0,
        "forced_flat_pct": float(forced_flat_bars / total_bars) if total_bars else 0.0,
        "missing_feature_pct": float(missing_feature_mask.mean()) if total_bars else 0.0,
        "missing_feature_columns": missing_feature_columns,
    }
    return StrategyResult(name=strategy_name, data=df, diagnostics=diagnostics)


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


def _overlay_binding_stats(
    overlays: List[str],
    symbol: str,
    frame: pd.DataFrame,
) -> Dict[str, object]:
    entries = _entry_count(frame) if not frame.empty else 0
    per_overlay = []
    for name in overlays:
        per_overlay.append(
            {
                "overlay": name,
                "symbol": symbol,
                "blocked_entries": 0,
                "delayed_entries": 0,
                "changed_bars": 0,
                "entry_count": int(entries),
            }
        )
    return {
        "symbol": symbol,
        "overlays": overlays,
        "binding_stats": per_overlay,
    }


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
    data_root: Path = DATA_ROOT,
) -> Dict[str, object]:
    engine_dir = data_root / "runs" / run_id / "engine"
    ensure_dir(engine_dir)

    strategy_frames: Dict[str, pd.DataFrame] = {}
    metrics: Dict[str, object] = {"strategies": {}}

    overlays = [str(o).strip() for o in params.get("overlays", []) if str(o).strip()]

    for strategy_name in strategies:
        symbol_results: List[StrategyResult] = []
        for symbol in symbols:
            bars, features = _load_symbol_data(data_root, symbol, run_id=run_id)
            result = _strategy_returns(symbol, bars, features, strategy_name, params, cost_bps)
            symbol_results.append(result)

        combined = _aggregate_strategy([res.data for res in symbol_results])
        strategy_frames[strategy_name] = combined
        out_path = engine_dir / f"strategy_returns_{strategy_name}.csv"
        combined.to_csv(out_path, index=False)

        pnl_series = combined.groupby("timestamp")["pnl"].sum()
        summary = _summarize_pnl(pnl_series)
        entries = sum(_entry_count(res.data) for res in symbol_results)
        diagnostics_total = {
            "total_bars": sum(res.diagnostics["total_bars"] for res in symbol_results),
            "nan_return_bars": sum(res.diagnostics["nan_return_bars"] for res in symbol_results),
            "forced_flat_bars": sum(res.diagnostics["forced_flat_bars"] for res in symbol_results),
            "missing_feature_bars": sum(res.diagnostics["missing_feature_bars"] for res in symbol_results),
        }
        total_bars = diagnostics_total["total_bars"]
        diagnostics = {
            **diagnostics_total,
            "nan_return_pct": float(diagnostics_total["nan_return_bars"] / total_bars) if total_bars else 0.0,
            "forced_flat_pct": float(diagnostics_total["forced_flat_bars"] / total_bars) if total_bars else 0.0,
            "missing_feature_pct": float(diagnostics_total["missing_feature_bars"] / total_bars) if total_bars else 0.0,
            "missing_feature_columns": sorted(
                {col for res in symbol_results for col in res.diagnostics.get("missing_feature_columns", [])}
            ),
        }
        metrics["strategies"][strategy_name] = {**summary, "entries": entries}
        metrics.setdefault("diagnostics", {}).setdefault("strategies", {})[strategy_name] = diagnostics
        symbol_bindings = []
        for res in symbol_results:
            sym = str(res.data["symbol"].iloc[0]) if not res.data.empty and "symbol" in res.data.columns else "unknown"
            symbol_bindings.append(_overlay_binding_stats(overlays, sym, res.data))
        metrics.setdefault("overlay_bindings", {})[strategy_name] = {
            "applied_overlays": overlays,
            "symbols": symbol_bindings,
        }

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
        "diagnostics": metrics.get("diagnostics", {}),
    }
