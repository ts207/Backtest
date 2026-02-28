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
    max_correlated_gross: float | None = None  # cap on same-direction gross across all strategies


def _as_float_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0).astype(float)


def allocate_position_scales(
    raw_positions_by_strategy: Dict[str, pd.Series],
    requested_scale_by_strategy: Dict[str, pd.Series],
    limits: RiskLimits,
    portfolio_pnl_series: pd.Series | None = None,
    regime_series: pd.Series | None = None,
    regime_scale_map: Dict[str, float] | None = None,
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

    if not requested:
        requested_gross = pd.Series(0.0, index=aligned_index)
    else:
        requested_gross = pd.DataFrame(requested).abs().sum(axis=1)
    allocated = {key: s.copy() for key, s in requested.items()}

    for key in ordered:
        max_s = float(max(0.0, limits.max_strategy_gross))
        gross = allocated[key].abs()
        
        # Where gross > max_s, ratio = max_s / gross. Else 1.0.
        # Ensure gross is non-zero when taking reciprocal.
        safe_gross = gross.replace(0.0, np.nan)
        ratio_series = (max_s / safe_gross).where(gross > max_s, 1.0)
        ratio_series = ratio_series.replace([np.inf, -np.inf], np.nan).fillna(1.0).clip(lower=0.0, upper=1.0)

        allocated[key] = allocated[key] * ratio_series

    symbol_cap = float(max(0.0, limits.max_symbol_gross))
    if not allocated:
        symbol_gross = pd.Series(0.0, index=aligned_index)
    else:
        symbol_gross = pd.DataFrame(allocated).abs().sum(axis=1)
    safe_symbol_gross = symbol_gross.replace(0.0, np.nan)
    symbol_ratio_series = (symbol_cap / safe_symbol_gross).where(symbol_gross > symbol_cap, 1.0)
    symbol_ratio_series = symbol_ratio_series.replace([np.inf, -np.inf], np.nan).fillna(1.0).clip(lower=0.0, upper=1.0)
    for key in ordered:
        allocated[key] = allocated[key] * symbol_ratio_series

    # Correlated gross cap: if all strategies are pointing the same direction
    # (all long or all short), limit the combined same-direction exposure.
    if limits.max_correlated_gross is not None:
        corr_cap = float(max(0.0, limits.max_correlated_gross))
        if not allocated:
            net_direction = pd.Series(0.0, index=aligned_index)
            same_dir_gross = pd.Series(0.0, index=aligned_index)
        else:
            df_alloc = pd.DataFrame(allocated)
            net_direction = df_alloc.sum(axis=1)  # positive = net long, negative = net short
            same_dir_gross = df_alloc.abs().sum(axis=1)
        # Only clip when all strategies are directionally concordant (net == gross)
        fully_concordant = (net_direction.abs() - same_dir_gross).abs() < 1e-9
        corr_ratio = pd.Series(1.0, index=aligned_index)
        needs_clip = fully_concordant & (same_dir_gross > corr_cap)
        safe_gross = same_dir_gross.replace(0.0, np.nan)
        corr_ratio_series = (corr_cap / safe_gross).where(needs_clip, 1.0)
        corr_ratio_series = corr_ratio_series.replace([np.inf, -np.inf], np.nan).fillna(1.0).clip(lower=0.0, upper=1.0)
        for key in ordered:
            allocated[key] = allocated[key] * corr_ratio_series

    # Regime-conditional sizing: scale positions down per-bar according to the
    # active market regime. regime_scale_map maps regime labels to scale factors
    # (e.g. {"HIGH_VOL": 0.6, "CHOP": 0.5}). Bars with unknown regimes default to 1.0.
    if regime_series is not None and regime_scale_map:
        regime_aligned = regime_series.reindex(aligned_index).astype(str)
        regime_scale_vals = regime_aligned.map(regime_scale_map).fillna(1.0).clip(lower=0.0, upper=1.0)
        for key in ordered:
            allocated[key] = allocated[key] * regime_scale_vals

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
    if not allocated:
        portfolio_gross = pd.Series(0.0, index=aligned_index)
    else:
        portfolio_gross = pd.DataFrame(allocated).abs().sum(axis=1)
    safe_portfolio_gross = portfolio_gross.replace(0.0, np.nan)
    portfolio_ratio_series = (portfolio_cap / safe_portfolio_gross).where(portfolio_gross > portfolio_cap, 1.0)
    portfolio_ratio_series = portfolio_ratio_series.replace([np.inf, -np.inf], np.nan).fillna(1.0).clip(lower=0.0, upper=1.0)
    for key in ordered:
        allocated[key] = allocated[key] * portfolio_ratio_series

    max_new = float(max(0.0, limits.max_new_exposure_per_bar))
    for key in ordered:
        # Autoregressive clamping: we must walk forward because the allowed position
        # at t depends on the actually-taken position at t-1.
        raw = allocated[key].values
        clamped = np.zeros_like(raw)
        prior = 0.0
        for i in range(len(raw)):
            target = raw[i]
            delta = target - prior
            if abs(delta) > max_new:
                delta = np.sign(delta) * max_new
            clamped[i] = prior + delta
            prior = clamped[i]
        allocated[key] = pd.Series(clamped, index=aligned_index)

    if not allocated:
        allocated_gross = pd.Series(0.0, index=aligned_index)
    else:
        allocated_gross = pd.DataFrame(allocated).abs().sum(axis=1)
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
