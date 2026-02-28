from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

_LOG = logging.getLogger(__name__)


def estimate_transaction_cost_bps(
    frame: pd.DataFrame,
    turnover: pd.Series,
    config: Dict[str, float],
) -> pd.Series:
    """
    Estimate per-bar transaction cost in bps from spread/volatility/liquidity proxies.
    Supports 'static' and 'dynamic' cost models.
    """
    idx = turnover.index
    turnover = pd.to_numeric(turnover.reindex(idx), errors="coerce").fillna(0.0).abs()

    model_type = str(config.get("cost_model", "static")).strip().lower()
    min_tob_coverage = float(config.get("min_tob_coverage", 0.0))
    
    base_fee_bps = float(config.get("base_fee_bps", 0.0))
    base_slippage_bps = float(config.get("base_slippage_bps", 0.0))
    cap_bps = float(config.get("max_cost_bps_cap", 150.0))

    # Static path: fee + slippage
    if model_type == "static":
        return pd.Series(base_fee_bps + base_slippage_bps, index=idx).clip(lower=0.0, upper=cap_bps)

    # Dynamic path: fee + spread-based slippage + impact
    spread_weight = float(config.get("spread_weight", 0.5))
    volatility_weight = float(config.get("volatility_weight", 0.1))
    liquidity_weight = float(config.get("liquidity_weight", 0.1))
    impact_weight = float(config.get("impact_weight", 0.1))

    tob_coverage = pd.to_numeric(frame.get("tob_coverage", pd.Series(0.0, index=idx)), errors="coerce").reindex(idx).fillna(0.0)
    spread = pd.to_numeric(frame.get("spread_bps", pd.Series(np.nan, index=idx)), errors="coerce").reindex(idx)
    depth = pd.to_numeric(frame.get("depth_usd", pd.Series(np.nan, index=idx)), errors="coerce").reindex(idx)
    
    # Fallback logic: if ToB coverage is too low or data is missing, use static fallback
    use_dynamic = (tob_coverage >= min_tob_coverage) & spread.notna()
    
    # Calculate components
    atr = pd.to_numeric(frame.get("atr_14", pd.Series(np.nan, index=idx)), errors="coerce").reindex(idx)
    close = pd.to_numeric(frame.get("close", pd.Series(np.nan, index=idx)), errors="coerce").reindex(idx)
    high = pd.to_numeric(frame.get("high", pd.Series(np.nan, index=idx)), errors="coerce").reindex(idx)
    low = pd.to_numeric(frame.get("low", pd.Series(np.nan, index=idx)), errors="coerce").reindex(idx)
    quote_vol = pd.to_numeric(frame.get("quote_volume", pd.Series(np.nan, index=idx)), errors="coerce").reindex(idx)

    range_bps = (((high - low) / close.replace(0.0, np.nan)) * 10000.0).replace([np.inf, -np.inf], np.nan)
    atr_bps = ((atr / close.replace(0.0, np.nan)) * 10000.0).replace([np.inf, -np.inf], np.nan)
    vol_bps = atr_bps.fillna(range_bps).fillna(0.0).abs()

    # Impact calculation
    # Use depth_usd if available, otherwise fallback to quote_volume proxy
    available_liquidity = depth.fillna(quote_vol).replace(0.0, np.nan).fillna(1e6).clip(lower=1.0)
    participation_rate = (turnover / available_liquidity).clip(lower=0.0)
    impact_sqrt = np.sqrt(participation_rate)
    
    max_part = max(1e-4, float(config.get("max_participation_rate", 0.10)))
    participation_penalty = np.exp(np.clip((participation_rate - max_part) / max_part, 0.0, 5.0)) - 1.0

    liq_scale = (1.0 / available_liquidity).replace([np.inf, -np.inf], np.nan)
    liq_scale = liq_scale.fillna(liq_scale.median() if liq_scale.notna().any() else 0.0).clip(lower=0.0)
    if float(liq_scale.max()) > 0:
        liq_scale = liq_scale / float(liq_scale.max())

    dynamic_slippage = (
        (spread_weight * spread.fillna(base_slippage_bps))
        + (volatility_weight * vol_bps)
        + (liquidity_weight * (liq_scale * 10.0))
        + (impact_weight * (impact_sqrt * 10.0 + participation_penalty * 50.0))
    )
    
    # Final cost resolution: fee + (dynamic if available else base_slippage)
    cost_bps = base_fee_bps + np.where(use_dynamic, dynamic_slippage, base_slippage_bps)
    
    return pd.Series(cost_bps, index=idx).clip(lower=0.0, upper=cap_bps).astype(float)


def load_calibration_config(
    symbol: str,
    *,
    calibration_dir,
    base_config: dict,
) -> dict:
    """
    Merge per-symbol calibration JSON (if present) over base_config.
    Keys in the calibration file override base_config; absent or None-valued keys
    are preserved from base. Returns a merged dict safe to pass to
    estimate_transaction_cost_bps as the config argument.
    """
    path = Path(calibration_dir) / f"{symbol}.json"
    merged = dict(base_config)
    if path.exists():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                _LOG.warning("Calibration file %s is not a JSON object; ignoring.", path)
            else:
                merged.update({k: v for k, v in raw.items() if v is not None})
        except json.JSONDecodeError as exc:
            _LOG.warning("Malformed calibration JSON at %s: %s; using base_config.", path, exc)
    return merged
