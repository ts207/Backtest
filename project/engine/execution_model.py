from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd


def estimate_transaction_cost_bps(
    frame: pd.DataFrame,
    turnover: pd.Series,
    config: Dict[str, float],
) -> pd.Series:
    """
    Estimate per-bar transaction cost in bps from spread/volatility/liquidity proxies.
    """
    idx = turnover.index
    turnover = pd.to_numeric(turnover.reindex(idx), errors="coerce").fillna(0.0).abs()

    base_fee_bps = float(config.get("base_fee_bps", 0.0))
    base_slippage_bps = float(config.get("base_slippage_bps", 0.0))
    spread_weight = float(config.get("spread_weight", 0.0))
    volatility_weight = float(config.get("volatility_weight", 0.0))
    liquidity_weight = float(config.get("liquidity_weight", 0.0))
    impact_weight = float(config.get("impact_weight", 0.0))
    cap_bps = float(config.get("max_cost_bps_cap", 150.0))

    spread = pd.to_numeric(frame.get("spread_bps", pd.Series(0.0, index=idx)).reindex(idx), errors="coerce").fillna(0.0).abs()
    atr = pd.to_numeric(frame.get("atr_14", pd.Series(np.nan, index=idx)).reindex(idx), errors="coerce")
    close = pd.to_numeric(frame.get("close", pd.Series(np.nan, index=idx)).reindex(idx), errors="coerce")
    high = pd.to_numeric(frame.get("high", pd.Series(np.nan, index=idx)).reindex(idx), errors="coerce")
    low = pd.to_numeric(frame.get("low", pd.Series(np.nan, index=idx)).reindex(idx), errors="coerce")
    quote_vol = pd.to_numeric(frame.get("quote_volume", pd.Series(np.nan, index=idx)).reindex(idx), errors="coerce")

    range_bps = (((high - low) / close.replace(0.0, np.nan)) * 10000.0).replace([np.inf, -np.inf], np.nan)
    atr_bps = ((atr / close.replace(0.0, np.nan)) * 10000.0).replace([np.inf, -np.inf], np.nan)
    vol_bps = atr_bps.fillna(range_bps).fillna(0.0).abs()

    liq_scale = (1.0 / quote_vol.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)
    liq_scale = liq_scale.fillna(liq_scale.median() if liq_scale.notna().any() else 0.0).clip(lower=0.0)
    if float(liq_scale.max()) > 0:
        liq_scale = liq_scale / float(liq_scale.max())

    available_vol = quote_vol.replace(0.0, np.nan).fillna(1e6).clip(lower=1.0)
    participation_rate = (turnover / available_vol).clip(lower=0.0)
    
    impact_sqrt = np.sqrt(participation_rate)
    
    max_part = max(1e-4, float(config.get("max_participation_rate", 0.10)))
    participation_penalty = np.exp(np.clip((participation_rate - max_part) / max_part, 0.0, 5.0)) - 1.0

    dynamic = (
        (spread_weight * spread)
        + (volatility_weight * vol_bps)
        + (liquidity_weight * (liq_scale * 10.0))
        + (impact_weight * (impact_sqrt * 10.0 + participation_penalty * 50.0))
    )
    cost_bps = (base_fee_bps + base_slippage_bps + dynamic).clip(lower=0.0, upper=max(0.0, cap_bps))
    return cost_bps.astype(float)
