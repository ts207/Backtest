from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np

import pandas as pd

from engine.pnl import compute_pnl_components, compute_returns
from engine.execution_model import estimate_transaction_cost_bps
from events.registry import load_registry_flags, build_event_feature_frame
from engine.risk_allocator import RiskLimits, allocate_position_scales
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
from pipelines._lib.timeframe_constants import BARS_PER_YEAR_BY_TIMEFRAME

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
LOGGER = logging.getLogger(__name__)

# Bars-per-year lookup for Sharpe annualization (canonical source in pipelines._lib).
BARS_PER_YEAR: Dict[str, int] = dict(BARS_PER_YEAR_BY_TIMEFRAME)

# Default execution timeframe — matches the research pipeline's feature lake layout.
# Changing this requires the feature builder and cleaned bar builder to produce data
# at the matching granularity before any backtest run.
_DEFAULT_TIMEFRAME = "5m"


@dataclass
class StrategyResult:
    name: str
    data: pd.DataFrame
    diagnostics: Dict[str, object]
    strategy_metadata: Dict[str, object]
    trace: pd.DataFrame


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


def _dedupe_timestamp_rows(frame: pd.DataFrame, *, label: str) -> Tuple[pd.DataFrame, int]:
    if frame.empty or "timestamp" not in frame.columns:
        return frame, 0
    out = frame.sort_values("timestamp").copy()
    dupes = int(out["timestamp"].duplicated(keep="last").sum())
    if dupes > 0:
        LOGGER.warning("Dropping %s duplicate timestamp rows for %s (keeping last).", dupes, label)
        out = out.drop_duplicates(subset=["timestamp"], keep="last")
    return out.reset_index(drop=True), dupes


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
    context, _ = _dedupe_timestamp_rows(context, label=f"context:{symbol}:{timeframe}")
    if "fp_def_version" not in context.columns:
        context["fp_def_version"] = FP_DEF_VERSION

    for col in _CONTEXT_COLUMNS:
        if col not in context.columns:
            context[col] = np.nan

    if "fp_active" in context.columns:
        context["fp_active"] = context["fp_active"].fillna(0).astype(int)
    if "fp_age_bars" in context.columns:
        context["fp_age_bars"] = context["fp_age_bars"].fillna(0).astype(int)
    if "fp_norm_due" in context.columns:
        context["fp_norm_due"] = context["fp_norm_due"].fillna(0).astype(int)
    return context[["timestamp", *_CONTEXT_COLUMNS]]


def _apply_context_defaults(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "fp_def_version" not in out.columns:
        out["fp_def_version"] = FP_DEF_VERSION
    out["fp_def_version"] = out["fp_def_version"].fillna(FP_DEF_VERSION)

    for col, default in [("fp_active", 0), ("fp_age_bars", 0), ("fp_norm_due", 0), ("fp_severity", 0.0), ("fp_event_id", None), ("fp_enter_ts", pd.NaT), ("fp_exit_ts", pd.NaT)]:
        if col not in out.columns:
            out[col] = default

    out["fp_active"] = out["fp_active"].fillna(0).astype(int)
    out["fp_age_bars"] = out["fp_age_bars"].fillna(0).astype(int)
    out["fp_norm_due"] = out["fp_norm_due"].fillna(0).astype(int)

    inactive = out["fp_active"] == 0
    out.loc[inactive, "fp_age_bars"] = 0
    out.loc[inactive, "fp_event_id"] = None
    out.loc[inactive, "fp_enter_ts"] = pd.NaT
    out.loc[inactive, "fp_exit_ts"] = pd.NaT
    out["fp_severity"] = out["fp_severity"].fillna(0.0).astype(float)
    out.loc[inactive, "fp_severity"] = 0.0
    return out


def _join_context_features(features: pd.DataFrame, context: pd.DataFrame) -> pd.DataFrame:
    joined = features.merge(context, on="timestamp", how="left", validate="one_to_one")
    return _apply_context_defaults(joined)


def _merge_event_flags(features: pd.DataFrame, event_flags: pd.DataFrame | None) -> pd.DataFrame:
    if event_flags is None or event_flags.empty:
        return features
    out = features.copy()
    flags = event_flags.copy()
    flags["timestamp"] = pd.to_datetime(flags["timestamp"], utc=True, errors="coerce")
    flags = flags.dropna(subset=["timestamp"]).drop_duplicates(subset=["timestamp"], keep="last")
    flag_cols = [col for col in flags.columns if col not in {"timestamp", "symbol"}]
    if not flag_cols:
        return out
    merged = out.merge(flags[["timestamp", *flag_cols]], on="timestamp", how="left")
    for col in flag_cols:
        merged[col] = merged[col].fillna(False).astype(bool)
    return merged


def _merge_event_features(features: pd.DataFrame, event_features: pd.DataFrame | None) -> pd.DataFrame:
    if event_features is None or event_features.empty:
        return features
    out = features.copy()
    ef = event_features.copy()
    ef["timestamp"] = pd.to_datetime(ef["timestamp"], utc=True, errors="coerce")
    ef = ef.dropna(subset=["timestamp"]).drop_duplicates(subset=["timestamp"], keep="last")
    
    # Merge event-specific features (e.g., stress_score, severity)
    merged = out.merge(ef, on="timestamp", how="left")
    
    # Forward-fill event features for a small window (e.g. 12 bars) to allow
    # DSL strategies with decision lag to still see the event magnitude.
    # TODO(research): Consider making this limit configurable via strategy params.
    event_cols = [c for c in ef.columns if c != "timestamp"]
    if event_cols:
        merged[event_cols] = merged[event_cols].ffill(limit=12)
        
    return merged


def _load_universe_snapshots(data_root: Path, run_id: str) -> pd.DataFrame:
    candidates = [
        data_root / "lake" / "runs" / run_id / "metadata" / "universe_snapshots",
        data_root / "lake" / "metadata" / "universe_snapshots",
    ]
    src = choose_partition_dir(candidates)
    files = list_parquet_files(src) if src else []
    if not files:
        return pd.DataFrame(columns=["symbol", "listing_start", "listing_end"])
    frame = read_parquet(files)
    if frame.empty:
        return pd.DataFrame(columns=["symbol", "listing_start", "listing_end"])
    needed = {"symbol", "listing_start", "listing_end"}
    if not needed.issubset(set(frame.columns)):
        return pd.DataFrame(columns=["symbol", "listing_start", "listing_end"])
    frame = frame[["symbol", "listing_start", "listing_end"]].copy()
    frame["listing_start"] = pd.to_datetime(frame["listing_start"], utc=True, errors="coerce")
    frame["listing_end"] = pd.to_datetime(frame["listing_end"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["symbol", "listing_start", "listing_end"]).copy()
    frame["symbol"] = frame["symbol"].astype(str)
    return frame


def _symbol_eligibility_mask(timestamps: pd.Series, symbol: str, snapshots: pd.DataFrame) -> pd.Series:
    ts_index = pd.DatetimeIndex(pd.to_datetime(timestamps, utc=True, errors="coerce"))
    if ts_index.hasnans:
        raise ValueError("Eligibility timestamps must be valid UTC datetimes.")

    if snapshots.empty:
        return pd.Series(True, index=ts_index, dtype=bool)

    symbol_key = str(symbol).strip().upper()
    rows = snapshots[snapshots["symbol"].astype(str).str.upper() == symbol_key]
    if rows.empty:
        return pd.Series(False, index=ts_index, dtype=bool)

    mask_values = np.zeros(len(ts_index), dtype=bool)
    for row in rows.itertuples(index=False):
        mask_values |= (ts_index >= row.listing_start) & (ts_index <= row.listing_end)
    return pd.Series(mask_values, index=ts_index, dtype=bool)


def _is_carry_strategy(strategy_name: str, strategy_metadata: Dict[str, object]) -> bool:
    family = str(strategy_metadata.get("family", "")).strip().lower() if isinstance(strategy_metadata, dict) else ""
    if family == "carry":
        return True
    name = str(strategy_name).strip().lower()
    return "carry" in name or "funding_extreme_reversal" in name



def _load_symbol_data(
    data_root: Path,
    symbol: str,
    run_id: str,
    start_ts: pd.Timestamp | None = None,
    end_ts: pd.Timestamp | None = None,
    event_flags: pd.DataFrame | None = None,
    event_features: pd.DataFrame | None = None,
    timeframe: str = _DEFAULT_TIMEFRAME,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    feature_candidates = [
        run_scoped_lake_path(data_root, run_id, "features", "perp", symbol, timeframe, "features_v1"),
        data_root / "lake" / "features" / "perp" / symbol / timeframe / "features_v1",
    ]
    bars_candidates = [
        run_scoped_lake_path(data_root, run_id, "cleaned", "perp", symbol, f"bars_{timeframe}"),
        data_root / "lake" / "cleaned" / "perp" / symbol / f"bars_{timeframe}",
    ]
    features_dir = choose_partition_dir(feature_candidates)
    bars_dir = choose_partition_dir(bars_candidates)
    feature_files = list_parquet_files(features_dir) if features_dir else []
    bars_files = list_parquet_files(bars_dir) if bars_dir else []
    features = read_parquet(feature_files)
    bars = read_parquet(bars_files)
    if features.empty or bars.empty:
        raise ValueError(
            f"Missing data for {symbol} (timeframe={timeframe}). "
            f"Expected features at {feature_candidates[0]} and bars at {bars_candidates[0]}. "
            "Verify the feature builder ran with the matching timeframe."
        )

    features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)
    bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
    ensure_utc_timestamp(features["timestamp"], "timestamp")
    ensure_utc_timestamp(bars["timestamp"], "timestamp")

    features, _ = _dedupe_timestamp_rows(features, label=f"features:{symbol}:{timeframe}")
    bars, _ = _dedupe_timestamp_rows(bars, label=f"bars:{symbol}:{timeframe}")
    if start_ts is not None:
        features = features[features["timestamp"] >= start_ts].copy()
        bars = bars[bars["timestamp"] >= start_ts].copy()
    if end_ts is not None:
        features = features[features["timestamp"] <= end_ts].copy()
        bars = bars[bars["timestamp"] <= end_ts].copy()
    if features.empty or bars.empty:
        raise ValueError(f"No data in selected window for {symbol}")

    context = _load_context_data(data_root, symbol, run_id=run_id, timeframe=timeframe)
    if start_ts is not None:
        context = context[context["timestamp"] >= start_ts].copy()
    if end_ts is not None:
        context = context[context["timestamp"] <= end_ts].copy()
    features = _join_context_features(features, context)
    if isinstance(event_flags, pd.DataFrame):
        flags = event_flags.copy()
        if start_ts is not None:
            # --- EVENT FLAGS TIMESTAMP GUARD PATCH ---

            # Some runs (e.g., offline smoke) may produce empty or schema-less event flag frames.

            # Fail closed by treating missing/invalid flags as absent.

            if flags is None or (hasattr(flags, "empty") and flags.empty) or ("timestamp" not in getattr(flags, "columns", [])):

                flags = None

            else:

                flags = flags[flags["timestamp"] >= start_ts].copy()

            # --- END PATCH ---
        if end_ts is not None and flags is not None:
            flags = flags[flags["timestamp"] <= end_ts].copy()
        features = _merge_event_flags(features, flags)
    
    if isinstance(event_features, pd.DataFrame):
        ef = event_features.copy()
        if start_ts is not None and not ef.empty and "timestamp" in ef.columns:
            ef = ef[ef["timestamp"] >= start_ts].copy()
        if end_ts is not None and not ef.empty and "timestamp" in ef.columns:
            ef = ef[ef["timestamp"] <= end_ts].copy()
        features = _merge_event_features(features, ef)
        
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
    eligibility_mask: pd.Series | None = None,
) -> StrategyResult:
    strategy = get_strategy(strategy_name)
    required_features = getattr(strategy, "required_features", []) or []

    positions = strategy.generate_positions(bars, features, params)
    signal_events = positions.attrs.get("signal_events", []) if hasattr(positions, "attrs") else []
    strategy_metadata = positions.attrs.get("strategy_metadata", {}) if hasattr(positions, "attrs") else {}
    timestamp_index = pd.DatetimeIndex(bars["timestamp"])
    positions = positions.reindex(timestamp_index).fillna(0).astype(int)
    _validate_positions(positions)
    requested_position_scale = float(params.get("position_scale", 1.0)) if isinstance(params, dict) else 1.0
    if not np.isfinite(requested_position_scale) or requested_position_scale < 0.0:
        raise ValueError(f"Invalid position_scale for {strategy_name}: {requested_position_scale}")

    # --- EXECUTION LATENCY PATCH ---
    # Shift positions to simulate execution latency (default 1 bar = next open/close).
    # pos[T] generated at T becomes effective at T+lag.
    execution_lag = int(params.get("execution_lag_bars", 1)) if isinstance(params, dict) else 1
    if execution_lag > 0:
        # Fill with 0 (flat) for the initial bars where signal is unknown.
        positions = positions.shift(execution_lag).fillna(0).astype(int)
    # --- END PATCH ---

    entry_reason_map: Dict[pd.Timestamp, str] = {}
    exit_reason_map: Dict[pd.Timestamp, str] = {}
    if isinstance(signal_events, list):
        for evt in signal_events:
            if not isinstance(evt, dict):
                continue
            try:
                ts = pd.Timestamp(evt.get("timestamp"))
            except Exception:
                continue
            if ts.tz is None:
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_convert("UTC")
            reason = str(evt.get("reason", ""))
            if evt.get("event") == "entry":
                entry_reason_map[ts] = reason
            elif evt.get("event") == "exit":
                exit_reason_map[ts] = reason

    bars_indexed = bars.set_index("timestamp")
    close = bars_indexed["close"].astype(float)
    high = bars_indexed["high"].astype(float)
    low = bars_indexed["low"].astype(float)
    ret = compute_returns(close)
    nan_ret_mask = ret.isna()
    forced_flat_bars = int((nan_ret_mask & (positions != 0)).sum())
    positions = positions.mask(nan_ret_mask, 0).astype(int)
    if eligibility_mask is not None:
        eligibility_mask = eligibility_mask.reindex(ret.index).fillna(False).astype(bool)
        ineligible_bars = int((~eligibility_mask).sum())
        positions = positions.where(eligibility_mask, 0).astype(int)
    else:
        ineligible_bars = 0

    features_indexed = features.set_index("timestamp")
    features_aligned = features_indexed.reindex(ret.index)
    
    # --- OVERLAY BINDING FIX ---
    # Apply overlays to the current (possibly shifted) positions.
    positions, overlay_stats = _apply_runtime_overlays(
        positions=positions,
        features_aligned=features_aligned,
        overlay_runtime=params.get("overlay_runtime", {}) if isinstance(params, dict) else {},
    )
    # --- END FIX ---

    funding_series = (
        pd.to_numeric(features_indexed.get("funding_rate_realized", pd.Series(0.0, index=ret.index)).reindex(ret.index), errors="coerce")
        .fillna(0.0)
        .astype(float)
    )
    borrow_series = (
        pd.to_numeric(features_indexed.get("borrow_rate_scaled", pd.Series(0.0, index=ret.index)).reindex(ret.index), errors="coerce")
        .fillna(0.0)
        .astype(float)
    )

    use_carry_components = _is_carry_strategy(strategy_name, strategy_metadata)
    scaled_positions = positions.astype(float) * requested_position_scale
    turnover = (scaled_positions - scaled_positions.shift(1).fillna(0.0)).abs()
    execution_cfg = dict(params.get("execution_model", {})) if isinstance(params, dict) and isinstance(params.get("execution_model", {}), dict) else {}
    
    # --- COST PRIORITY FIX ---
    # Allow explicit 0 in execution_model to override cost_bps.
    if "base_fee_bps" not in execution_cfg:
        execution_cfg["base_fee_bps"] = float(cost_bps) / 2.0
    if "base_slippage_bps" not in execution_cfg:
        execution_cfg["base_slippage_bps"] = float(cost_bps) / 2.0
    # --- END FIX ---
    
    frame_for_cost = pd.DataFrame(
        {
            "spread_bps": pd.to_numeric(features_aligned.get("spread_bps", pd.Series(0.0, index=ret.index)), errors="coerce").fillna(0.0),
            "atr_14": pd.to_numeric(features_aligned.get("atr_14", pd.Series(np.nan, index=ret.index)), errors="coerce"),
            "quote_volume": pd.to_numeric(features_aligned.get("quote_volume", pd.Series(np.nan, index=ret.index)), errors="coerce"),
            "close": close.reindex(ret.index).astype(float),
            "high": high.reindex(ret.index).astype(float),
            "low": low.reindex(ret.index).astype(float),
        },
        index=ret.index,
    )
    dynamic_cost_bps = estimate_transaction_cost_bps(frame=frame_for_cost, turnover=turnover, config=execution_cfg)
    pnl_components = compute_pnl_components(
        scaled_positions,
        ret,
        dynamic_cost_bps,
        funding_rate=funding_series if use_carry_components else None,
        borrow_rate=borrow_series if use_carry_components else None,
    )
    pnl = pnl_components["pnl"]
    if nan_ret_mask.any():
        LOGGER.info(
            "Gap-safe returns forced flat %s bars for %s/%s.",
            forced_flat_bars,
            symbol,
            strategy_name,
        )

    missing_feature_columns = [col for col in required_features if col not in features_aligned.columns]
    present_required = [col for col in required_features if col in features_aligned.columns]
    if present_required:
        missing_feature_mask = features_aligned[present_required].isna().any(axis=1)
    else:
        missing_feature_mask = pd.Series(False, index=ret.index)

    high_96 = features_aligned["high_96"] if "high_96" in features_aligned.columns else pd.Series(index=ret.index, dtype=float)
    low_96 = features_aligned["low_96"] if "low_96" in features_aligned.columns else pd.Series(index=ret.index, dtype=float)

    df = pd.DataFrame(
        {
            "timestamp": ret.index,
            "symbol": symbol,
            "pos": positions.values,
            "requested_position_scale": float(requested_position_scale),
            "allocated_position_scale": float(requested_position_scale),
            "dynamic_cost_bps": dynamic_cost_bps.values,
            "ret": (ret * requested_position_scale).values,
            "pnl": pnl.values,
            "gross_pnl": pnl_components["gross_pnl"].values,
            "trading_cost": pnl_components["trading_cost"].values,
            "funding_pnl": pnl_components["funding_pnl"].values,
            "borrow_cost": pnl_components["borrow_cost"].values,
            "close": close.reindex(ret.index).values,
            "high": high.reindex(ret.index).values,
            "low": low.reindex(ret.index).values,
            "high_96": high_96.values,
            "low_96": low_96.values,
            "fp_def_version": features_aligned["fp_def_version"].values if "fp_def_version" in features_aligned.columns else FP_DEF_VERSION,
            "fp_active": features_aligned["fp_active"].fillna(0).astype(int).values if "fp_active" in features_aligned.columns else np.zeros(len(ret), dtype=int),
            "fp_age_bars": features_aligned["fp_age_bars"].fillna(0).astype(int).values if "fp_age_bars" in features_aligned.columns else np.zeros(len(ret), dtype=int),
            "fp_norm_due": features_aligned["fp_norm_due"].fillna(0).astype(int).values if "fp_norm_due" in features_aligned.columns else np.zeros(len(ret), dtype=int),
            "entry_reason": [entry_reason_map.get(t, "") for t in ret.index],
            "exit_signal_reason": [exit_reason_map.get(t, "") for t in ret.index],
            "position_scale": float(requested_position_scale),
        }
    )
    total_bars = int(len(ret))
    diagnostics = {
        "total_bars": total_bars,
        "nan_return_bars": int(nan_ret_mask.sum()),
        "forced_flat_bars": forced_flat_bars,
        "missing_feature_bars": int(missing_feature_mask.sum()),
        "ineligible_universe_bars": int(ineligible_bars),
        "overlay_stats": overlay_stats,
        "effective_avg_cost_bps": float(dynamic_cost_bps.mean()) if len(dynamic_cost_bps) else 0.0,
        "nan_return_pct": float(nan_ret_mask.mean()) if total_bars else 0.0,
        "forced_flat_pct": float(forced_flat_bars / total_bars) if total_bars else 0.0,
        "missing_feature_pct": float(missing_feature_mask.mean()) if total_bars else 0.0,
        "missing_feature_columns": missing_feature_columns,
    }
    prior_pos = df["pos"].shift(1).fillna(0).astype(int)
    trace = pd.DataFrame(
        {
            "timestamp": df["timestamp"],
            "symbol": symbol,
            "strategy_id": strategy_name,
            "state": np.where(df["pos"] == 0, "flat", "in_position"),
            "entry_candidate": ((prior_pos == 0) & (df["pos"] != 0)).astype(int),
            "entry_allowed": ((prior_pos == 0) & (df["pos"] != 0)).astype(int),
            "entry_block_reason": "",
            "condition_hits": "{}",
            "overlay_actions": "{}",
            "exit_candidate": ((prior_pos != 0) & (df["pos"] == 0)).astype(int),
            "exit_reason": df["exit_signal_reason"].fillna("").astype(str),
            "stop_price": np.nan,
            "target_price": np.nan,
            "requested_scale": df["requested_position_scale"].astype(float),
            "allocated_scale": df["allocated_position_scale"].astype(float),
        }
    )
    return StrategyResult(
        name=strategy_name,
        data=df,
        diagnostics=diagnostics,
        strategy_metadata=strategy_metadata if isinstance(strategy_metadata, dict) else {},
        trace=trace,
    )


def _apply_allocator_to_strategy_frames(
    strategy_frames: Dict[str, pd.DataFrame],
    *,
    params: Dict[str, object],
    params_by_strategy: Optional[Dict[str, Dict[str, object]]] = None,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, float]]:
    if not strategy_frames:
        return strategy_frames, {"requested_gross": 0.0, "allocated_gross": 0.0, "clipped_fraction": 0.0}

    alloc_cfg = dict(params.get("risk_allocator", {})) if isinstance(params.get("risk_allocator", {}), dict) else {}
    target_vol_val = alloc_cfg.get("target_annual_vol")
    max_dd_val = alloc_cfg.get("max_drawdown_limit")
    limits = RiskLimits(
        max_portfolio_gross=float(alloc_cfg.get("max_portfolio_gross", 1.0)),
        max_symbol_gross=float(alloc_cfg.get("max_symbol_gross", 1.0)),
        max_strategy_gross=float(alloc_cfg.get("max_strategy_gross", 1.0)),
        max_new_exposure_per_bar=float(alloc_cfg.get("max_new_exposure_per_bar", 1.0)),
        target_annual_vol=float(target_vol_val) if target_vol_val is not None else None,
        max_drawdown_limit=float(max_dd_val) if max_dd_val is not None else None,
    )
    if any(v <= 0.0 for v in [limits.max_portfolio_gross, limits.max_symbol_gross, limits.max_strategy_gross, limits.max_new_exposure_per_bar]):
        return strategy_frames, {"requested_gross": 0.0, "allocated_gross": 0.0, "clipped_fraction": 0.0}

    out = {k: v.copy() for k, v in strategy_frames.items()}
    global_pnl_series: pd.Series | None = None
    if limits.target_annual_vol is not None or limits.max_drawdown_limit is not None:
        unscaled_portfolio = _aggregate_portfolio(out)
        if not unscaled_portfolio.empty and "timestamp" in unscaled_portfolio.columns:
            unscaled_portfolio = unscaled_portfolio.sort_values("timestamp")
            idx = pd.DatetimeIndex(pd.to_datetime(unscaled_portfolio["timestamp"], utc=True))
            global_pnl_series = pd.Series(
                pd.to_numeric(unscaled_portfolio["portfolio_pnl"], errors="coerce").fillna(0.0).values, 
                index=idx, dtype=float
            )
    symbols = sorted(
        {
            str(sym)
            for frame in out.values()
            if not frame.empty and "symbol" in frame.columns
            for sym in frame["symbol"].dropna().astype(str).unique().tolist()
        }
    )
    agg_diag = {"requested_gross": 0.0, "allocated_gross": 0.0, "clipped_fraction": 0.0}
    for symbol in symbols:
        pos_by_strategy: Dict[str, pd.Series] = {}
        req_scale_by_strategy: Dict[str, pd.Series] = {}
        for strategy_name in sorted(out.keys()):
            frame = out[strategy_name]
            sub = frame[frame["symbol"].astype(str) == symbol].copy() if (not frame.empty and "symbol" in frame.columns) else pd.DataFrame()
            if sub.empty:
                continue
            sub = sub.sort_values("timestamp")
            idx = pd.DatetimeIndex(pd.to_datetime(sub["timestamp"], utc=True))
            pos_by_strategy[strategy_name] = pd.Series(pd.to_numeric(sub["pos"], errors="coerce").fillna(0.0).values, index=idx, dtype=float)
            requested = pd.to_numeric(sub.get("requested_position_scale", sub.get("position_scale", 1.0)), errors="coerce").fillna(1.0)
            req_scale_by_strategy[strategy_name] = pd.Series(requested.values, index=idx, dtype=float)
        if not pos_by_strategy:
            continue
        alloc_scales, diag = allocate_position_scales(
            raw_positions_by_strategy=pos_by_strategy,
            requested_scale_by_strategy=req_scale_by_strategy,
            limits=limits,
            portfolio_pnl_series=global_pnl_series,
        )
        agg_diag["requested_gross"] += float(diag.get("requested_gross", 0.0))
        agg_diag["allocated_gross"] += float(diag.get("allocated_gross", 0.0))
        for strategy_name, scale_series in alloc_scales.items():
            frame = out[strategy_name]
            mask = frame["symbol"].astype(str) == symbol
            if not mask.any():
                continue
            sub = frame.loc[mask].copy().sort_values("timestamp")
            idx = pd.DatetimeIndex(pd.to_datetime(sub["timestamp"], utc=True))
            new_scale = scale_series.reindex(idx).fillna(0.0).values.astype(float)
            old_scale = pd.to_numeric(sub.get("position_scale", 1.0), errors="coerce").fillna(1.0).values.astype(float)
            ratio = np.divide(new_scale, np.where(old_scale == 0.0, np.nan, old_scale))
            ratio = np.nan_to_num(ratio, nan=0.0, posinf=0.0, neginf=0.0)
            for col in ["ret", "pnl", "gross_pnl", "trading_cost", "funding_pnl", "borrow_cost"]:
                if col in sub.columns:
                    sub[col] = pd.to_numeric(sub[col], errors="coerce").fillna(0.0).values * ratio
            sub["allocated_position_scale"] = new_scale
            sub["position_scale"] = new_scale
            out[strategy_name].loc[sub.index, :] = sub
    req = float(agg_diag["requested_gross"])
    alloc = float(agg_diag["allocated_gross"])
    agg_diag["clipped_fraction"] = 0.0 if req <= 0 else float(max(0.0, (req - alloc) / req))
    return out, agg_diag


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
    overlay_stats: Dict[str, Dict[str, int]] | None = None,
) -> Dict[str, object]:
    entries = _entry_count(frame) if not frame.empty else 0
    overlay_stats = overlay_stats or {}
    per_overlay = []
    for name in overlays:
        stats = overlay_stats.get(name, {})
        per_overlay.append(
            {
                "overlay": name,
                "symbol": symbol,
                "blocked_entries": int(stats.get("blocked_entries", 0)),
                "delayed_entries": int(stats.get("delayed_entries", 0)),
                "changed_bars": int(stats.get("changed_bars", 0)),
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


def _mev_risk_bps(features_aligned: pd.DataFrame) -> pd.Series:
    if "mev_risk_bps" in features_aligned.columns:
        return pd.to_numeric(features_aligned["mev_risk_bps"], errors="coerce").fillna(0.0).abs()
    if {"liquidation_notional", "oi_notional"}.issubset(set(features_aligned.columns)):
        liq = pd.to_numeric(features_aligned["liquidation_notional"], errors="coerce").fillna(0.0).abs()
        oi = pd.to_numeric(features_aligned["oi_notional"], errors="coerce").fillna(0.0).abs()
        safe_oi = oi.replace(0.0, np.nan)
        risk = ((liq / safe_oi) * 10_000.0).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        return risk.abs()
    return pd.Series(0.0, index=features_aligned.index, dtype=float)


def _apply_runtime_overlays(
    positions: pd.Series,
    features_aligned: pd.DataFrame,
    overlay_runtime: Dict[str, Dict[str, Any]] | None,
) -> Tuple[pd.Series, Dict[str, Dict[str, int]]]:
    if not overlay_runtime:
        return positions.astype(int), {}

    out = positions.astype(int).copy()
    stats: Dict[str, Dict[str, int]] = {}
    for overlay_name, runtime_cfg in overlay_runtime.items():
        if not isinstance(runtime_cfg, dict):
            continue
        runtime_type = str(runtime_cfg.get("type", "")).strip().lower()
        if runtime_type != "mev_aware_risk_filter":
            continue

        risk_bps = _mev_risk_bps(features_aligned).reindex(out.index).fillna(0.0)
        throttle_start = float(runtime_cfg.get("throttle_start_bps", 12.0))
        block_threshold = float(runtime_cfg.get("block_threshold_bps", 25.0))
        before = out.copy()
        prior = before.shift(1).fillna(0).astype(int)
        entry_mask = (prior == 0) & (before != 0)
        delayed_entries_mask = entry_mask & (risk_bps >= throttle_start) & (risk_bps < block_threshold)
        blocked_entries_mask = entry_mask & (risk_bps >= block_threshold)
        gated_entry_mask = delayed_entries_mask | blocked_entries_mask
        out.loc[gated_entry_mask] = 0

        forced_flat_mask = (before != 0) & (risk_bps >= block_threshold)
        out.loc[forced_flat_mask] = 0
        changed_mask = out != before
        stats[overlay_name] = {
            "blocked_entries": int(blocked_entries_mask.sum()),
            "delayed_entries": int(delayed_entries_mask.sum()),
            "changed_bars": int(changed_mask.sum()),
        }

    return out.astype(int), stats


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
    params_by_strategy: Optional[Dict[str, Dict[str, object]]] = None,
    start_ts: pd.Timestamp | None = None,
    end_ts: pd.Timestamp | None = None,
    timeframe: str = _DEFAULT_TIMEFRAME,
) -> Dict[str, object]:
    engine_dir = data_root / "runs" / run_id / "engine"
    ensure_dir(engine_dir)

    strategy_frames: Dict[str, pd.DataFrame] = {}
    strategy_traces: Dict[str, pd.DataFrame] = {}
    metrics: Dict[str, object] = {"strategies": {}}
    event_flags_cache: Dict[Tuple[str, str], pd.DataFrame] = {}
    event_features_cache: Dict[Tuple[str, str], pd.DataFrame] = {}

    overlays = [str(o).strip() for o in params.get("overlays", []) if str(o).strip()]
    universe_snapshots = _load_universe_snapshots(data_root, run_id)

    for strategy_name in strategies:
        symbol_results: List[StrategyResult] = []
        for symbol in symbols:
            strategy_params = params_by_strategy.get(strategy_name, params) if params_by_strategy else params
            event_flags = pd.DataFrame()
            event_features = pd.DataFrame()
            if strategy_name.startswith("dsl_interpreter_v1"):
                blueprint_run_id = str(
                    strategy_params.get("dsl_blueprint", {}).get("run_id", run_id)
                    if isinstance(strategy_params.get("dsl_blueprint", {}), dict)
                    else run_id
                )
                cache_key = (blueprint_run_id, str(symbol).upper())
                if cache_key not in event_flags_cache:
                    event_flags_cache[cache_key] = load_registry_flags(
                        data_root=data_root,
                        run_id=blueprint_run_id,
                        symbol=symbol,
                    )
                event_flags = event_flags_cache[cache_key]
                
                if cache_key not in event_features_cache:
                    event_features_cache[cache_key] = build_event_feature_frame(
                        data_root=data_root,
                        run_id=blueprint_run_id,
                        symbol=symbol,
                    )
                event_features = event_features_cache[cache_key]

            bars, features = _load_symbol_data(
                data_root,
                symbol,
                run_id=run_id,
                start_ts=start_ts,
                end_ts=end_ts,
                event_flags=event_flags,
                event_features=event_features,
                timeframe=timeframe,
            )
            eligibility_mask = _symbol_eligibility_mask(bars["timestamp"], symbol, universe_snapshots)
            result = _strategy_returns(
                symbol,
                bars,
                features,
                strategy_name,
                strategy_params,
                cost_bps,
                eligibility_mask=eligibility_mask,
            )
            symbol_results.append(result)

        combined = _aggregate_strategy([res.data for res in symbol_results])
        strategy_frames[strategy_name] = combined
        out_path = engine_dir / f"strategy_returns_{strategy_name}.csv"
        combined.to_csv(out_path, index=False)
        traces = [res.trace for res in symbol_results if isinstance(res.trace, pd.DataFrame) and not res.trace.empty]
        if traces:
            strategy_traces[strategy_name] = pd.concat(traces, ignore_index=True).sort_values(["timestamp", "symbol"]).reset_index(drop=True)
        else:
            strategy_traces[strategy_name] = pd.DataFrame(
                columns=[
                    "timestamp",
                    "symbol",
                    "strategy_id",
                    "state",
                    "entry_candidate",
                    "entry_allowed",
                    "entry_block_reason",
                    "condition_hits",
                    "overlay_actions",
                    "exit_candidate",
                    "exit_reason",
                    "stop_price",
                    "target_price",
                    "requested_scale",
                    "allocated_scale",
                ]
            )

        pnl_series = combined.groupby("timestamp")["pnl"].sum()
        summary = _summarize_pnl(pnl_series)
        entries = sum(_entry_count(res.data) for res in symbol_results)
        diagnostics_total = {
            "total_bars": sum(res.diagnostics["total_bars"] for res in symbol_results),
            "nan_return_bars": sum(res.diagnostics["nan_return_bars"] for res in symbol_results),
            "forced_flat_bars": sum(res.diagnostics["forced_flat_bars"] for res in symbol_results),
            "missing_feature_bars": sum(res.diagnostics["missing_feature_bars"] for res in symbol_results),
            "ineligible_universe_bars": sum(res.diagnostics.get("ineligible_universe_bars", 0) for res in symbol_results),
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
        metadata_candidates = [res.strategy_metadata for res in symbol_results if res.strategy_metadata]
        if metadata_candidates:
            metrics.setdefault("strategy_metadata", {})[strategy_name] = {
                **metadata_candidates[0],
                "symbols": [
                    str(res.data["symbol"].iloc[0])
                    for res in symbol_results
                    if not res.data.empty and "symbol" in res.data.columns
                ],
            }

        metrics.setdefault("diagnostics", {}).setdefault("strategies", {})[strategy_name] = diagnostics
        symbol_bindings = []
        for res in symbol_results:
            sym = str(res.data["symbol"].iloc[0]) if not res.data.empty and "symbol" in res.data.columns else "unknown"
            per_symbol_overlay_stats = (
                res.diagnostics.get("overlay_stats", {})
                if isinstance(res.diagnostics, dict) and isinstance(res.diagnostics.get("overlay_stats", {}), dict)
                else {}
            )
            symbol_bindings.append(
                _overlay_binding_stats(overlays, sym, res.data, overlay_stats=per_symbol_overlay_stats)
            )
        metrics.setdefault("overlay_bindings", {})[strategy_name] = {
            "applied_overlays": overlays,
            "symbols": symbol_bindings,
        }

    strategy_frames, allocator_diag = _apply_allocator_to_strategy_frames(
        strategy_frames=strategy_frames,
        params=params,
        params_by_strategy=params_by_strategy,
    )

    kill_cfg = dict(params.get("kill_switches", {})) if isinstance(params.get("kill_switches"), dict) else {}
    if kill_cfg:
        temp_portfolio = _aggregate_portfolio(strategy_frames)
        if not temp_portfolio.empty and "portfolio_pnl" in temp_portfolio.columns:
            try:
                from engine.kill_switches import evaluate_kill_switches
            except ImportError:
                evaluate_kill_switches = None
                
            if evaluate_kill_switches:
                combined_cost = pd.Series(dtype=float)
                for df in strategy_frames.values():
                    if "dynamic_cost_bps" in df.columns:
                        cost = df.groupby("timestamp")["dynamic_cost_bps"].mean()
                        if combined_cost.empty:
                            combined_cost = cost
                        else:
                            combined_cost = pd.concat([combined_cost, cost], axis=1).mean(axis=1)
                
                pnl_series = pd.Series(
                    temp_portfolio["portfolio_pnl"].values, 
                    index=pd.DatetimeIndex(pd.to_datetime(temp_portfolio["timestamp"], utc=True)), 
                    dtype=float
                )
                
                ks_state = evaluate_kill_switches(
                    portfolio_pnl_series=pnl_series,
                    slippage_bps_series=combined_cost,
                    config=kill_cfg,
                )
                
                if ks_state.is_triggered and ks_state.trigger_timestamp is not None:
                    LOGGER.warning("Kill switch triggered at %s: %s", ks_state.trigger_timestamp, ks_state.trigger_reason)
                    metrics.setdefault("diagnostics", {})["kill_switch"] = {
                        "triggered": True,
                        "reason": str(ks_state.trigger_reason),
                        "timestamp": ks_state.trigger_timestamp.isoformat(),
                    }
                    for strategy_name, frame in strategy_frames.items():
                        if not frame.empty and "timestamp" in frame.columns:
                            ts_col = pd.to_datetime(frame["timestamp"], utc=True)
                            after_trigger = ts_col > ks_state.trigger_timestamp
                            frame.loc[after_trigger, "pos"] = 0.0
                            if "allocated_position_scale" in frame.columns:
                                frame.loc[after_trigger, "allocated_position_scale"] = 0.0
                            if "requested_position_scale" in frame.columns:
                                frame.loc[after_trigger, "requested_position_scale"] = 0.0
                            for col in ["pnl", "gross_pnl", "trading_cost", "funding_pnl", "borrow_cost", "ret"]:
                                if col in frame.columns:
                                    frame.loc[after_trigger, col] = 0.0
                            strategy_frames[strategy_name] = frame
    for strategy_name, frame in strategy_frames.items():
        out_path = engine_dir / f"strategy_returns_{strategy_name}.csv"
        frame.to_csv(out_path, index=False)
        trace = strategy_traces.get(strategy_name, pd.DataFrame())
        if not trace.empty and "allocated_scale" in trace.columns and "allocated_position_scale" in frame.columns:
            scale_lookup = frame[["timestamp", "symbol", "allocated_position_scale"]].copy()
            scale_lookup["timestamp"] = pd.to_datetime(scale_lookup["timestamp"], utc=True)
            trace = trace.copy()
            trace["timestamp"] = pd.to_datetime(trace["timestamp"], utc=True)
            trace = trace.merge(scale_lookup, on=["timestamp", "symbol"], how="left")
            trace["allocated_scale"] = pd.to_numeric(trace["allocated_position_scale"], errors="coerce").fillna(trace["allocated_scale"])
            trace = trace.drop(columns=["allocated_position_scale"])
            strategy_traces[strategy_name] = trace

    for strategy_name, trace in strategy_traces.items():
        trace_path = engine_dir / f"strategy_trace_{strategy_name}.csv"
        trace.to_csv(trace_path, index=False)

    for strategy_name, frame in strategy_frames.items():
        pnl_series = frame.groupby("timestamp")["pnl"].sum() if (not frame.empty and "pnl" in frame.columns) else pd.Series(dtype=float)
        metrics["strategies"][strategy_name] = {
            **_summarize_pnl(pnl_series),
            "entries": _entry_count(frame) if not frame.empty else 0,
        }

    portfolio = _aggregate_portfolio(strategy_frames)
    portfolio_path = engine_dir / "portfolio_returns.csv"
    portfolio.to_csv(portfolio_path, index=False)

    portfolio_summary = _summarize_pnl(portfolio["portfolio_pnl"]) if not portfolio.empty else _summarize_pnl(pd.Series(dtype=float))
    metrics["portfolio"] = portfolio_summary
    metrics.setdefault("diagnostics", {})["allocator"] = allocator_diag

    metrics_path = engine_dir / "metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "engine_dir": engine_dir,
        "strategy_frames": strategy_frames,
        "strategy_traces": strategy_traces,
        "portfolio": portfolio,
        "metrics": metrics,
        "diagnostics": metrics.get("diagnostics", {}),
    }
