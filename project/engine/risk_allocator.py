from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class RiskLimits:
    max_portfolio_gross: float = 1.0
    max_symbol_gross: float = 1.0
    max_strategy_gross: float = 1.0
    max_new_exposure_per_bar: float = 1.0
    target_annual_vol: float | None = None
    max_drawdown_limit: float | None = None


def _as_float_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0).astype(float)


def allocate_position_scales(
    raw_positions_by_strategy: Dict[str, pd.Series],
    requested_scale_by_strategy: Dict[str, pd.Series],
    limits: RiskLimits,
    portfolio_pnl_series: pd.Series | None = None,
) -> Tuple[Dict[str, pd.Series], Dict[str, float]]:
    """
    Deterministically allocate per-bar scales under strategy/symbol/portfolio caps.

    The allocator is deterministic by strategy id sort order and proportionally rescales
    strategies that exceed the configured gross limits.
    """
    if not raw_positions_by_strategy:
        return {}, {
            "requested_gross": 0.0,
            "allocated_gross": 0.0,
            "clipped_fraction": 0.0,
        }

    ordered = sorted(raw_positions_by_strategy.keys())
    aligned_index = None
    for key in ordered:
        idx = raw_positions_by_strategy[key].index
        aligned_index = idx if aligned_index is None else aligned_index.union(idx)
    if aligned_index is None:
        raise ValueError("aligned_index must not be None")

    requested: Dict[str, pd.Series] = {}
    for key in ordered:
        pos = _as_float_series(raw_positions_by_strategy[key]).reindex(aligned_index).fillna(0.0)
        scale = _as_float_series(requested_scale_by_strategy.get(key, pd.Series(1.0, index=aligned_index))).reindex(aligned_index).fillna(1.0)
        scale = scale.clip(lower=0.0)
        requested[key] = (pos * scale).astype(float)

    requested_gross = sum(s.abs() for s in requested.values())
    allocated = {key: s.copy() for key, s in requested.items()}

    for key in ordered:
        max_s = float(max(0.0, limits.max_strategy_gross))
        gross = allocated[key].abs()
        ratio = np.where(gross > max_s, max_s / gross.replace(0.0, np.nan), 1.0)
        ratio_series = pd.Series(ratio, index=aligned_index).replace([np.inf, -np.inf], np.nan).fillna(1.0).clip(lower=0.0, upper=1.0)
        allocated[key] = allocated[key] * ratio_series

    symbol_cap = float(max(0.0, limits.max_symbol_gross))
    symbol_gross = sum(s.abs() for s in allocated.values())
    symbol_ratio = np.where(symbol_gross > symbol_cap, symbol_cap / symbol_gross.replace(0.0, np.nan), 1.0)
    symbol_ratio_series = pd.Series(symbol_ratio, index=aligned_index).replace([np.inf, -np.inf], np.nan).fillna(1.0).clip(lower=0.0, upper=1.0)
    for key in ordered:
        allocated[key] = allocated[key] * symbol_ratio_series

    vol_scale_series = pd.Series(1.0, index=aligned_index)
    if limits.target_annual_vol is not None and portfolio_pnl_series is not None:
        pnl = portfolio_pnl_series.reindex(aligned_index).fillna(0.0)
        roll_std = pnl.rolling(window=5760, min_periods=288).std()
        ann_vol = roll_std * np.sqrt(105120)
        vol_scale = (limits.target_annual_vol / ann_vol.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(1.0)
        vol_scale_series = vol_scale.clip(lower=0.0, upper=2.0)

    dd_scale_series = pd.Series(1.0, index=aligned_index)
    if limits.max_drawdown_limit is not None and portfolio_pnl_series is not None:
        pnl = portfolio_pnl_series.reindex(aligned_index).fillna(0.0)
        equity = (1.0 + pnl).cumprod()
        peak = equity.cummax().replace(0.0, np.nan)
        drawdown = ((peak - equity) / peak).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        dd_factor = (limits.max_drawdown_limit - drawdown) / limits.max_drawdown_limit
        dd_scale_series = dd_factor.clip(lower=0.0, upper=1.0)

    dynamic_overlay_series = (vol_scale_series * dd_scale_series).fillna(1.0)
    for key in ordered:
        allocated[key] = allocated[key] * dynamic_overlay_series

    portfolio_cap = float(max(0.0, limits.max_portfolio_gross))
    portfolio_gross = sum(s.abs() for s in allocated.values())
    portfolio_ratio = np.where(portfolio_gross > portfolio_cap, portfolio_cap / portfolio_gross.replace(0.0, np.nan), 1.0)
    portfolio_ratio_series = (
        pd.Series(portfolio_ratio, index=aligned_index).replace([np.inf, -np.inf], np.nan).fillna(1.0).clip(lower=0.0, upper=1.0)
    )
    for key in ordered:
        allocated[key] = allocated[key] * portfolio_ratio_series

    max_new = float(max(0.0, limits.max_new_exposure_per_bar))
    for key in ordered:
        series = allocated[key].copy()
        prior = series.shift(1).fillna(0.0)
        delta = series - prior
        delta_abs = delta.abs()
        ratio = np.where(delta_abs > max_new, max_new / delta_abs.replace(0.0, np.nan), 1.0)
        ratio_series = pd.Series(ratio, index=aligned_index).replace([np.inf, -np.inf], np.nan).fillna(1.0).clip(lower=0.0, upper=1.0)
        allocated[key] = prior + (delta * ratio_series)

    allocated_gross = sum(s.abs() for s in allocated.values())
    req_total = float(requested_gross.sum())
    alloc_total = float(allocated_gross.sum())
    clipped_fraction = 0.0 if req_total <= 0 else float(max(0.0, (req_total - alloc_total) / req_total))

    out_scale_by_strategy: Dict[str, pd.Series] = {}
    for key in ordered:
        pos = _as_float_series(raw_positions_by_strategy[key]).reindex(aligned_index).fillna(0.0)
        denom = pos.replace(0.0, np.nan).abs()
        scale = (allocated[key].abs() / denom).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        out_scale_by_strategy[key] = scale.astype(float)

    return out_scale_by_strategy, {
        "requested_gross": req_total,
        "allocated_gross": alloc_total,
        "clipped_fraction": clipped_fraction,
    }
