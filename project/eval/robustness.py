from __future__ import annotations

import logging
from typing import Dict

import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)


def block_bootstrap_pnl(
    pnl_series: pd.Series,
    block_size_bars: int = 576,  # roughly 2 days of 5m bars
    n_iterations: int = 1000,
) -> Dict[str, float]:
    """
    Perform a stationary block bootstrap on the PnL series.
    Returns 5th, 50th, and 95th percentiles of the annualized return and max drawdown.
    """
    pnl = pd.to_numeric(pnl_series, errors="coerce").dropna().values
    n = len(pnl)
    if n < block_size_bars or n == 0:
        return {}
        
    n_blocks = int(np.ceil(n / block_size_bars))
    
    annualized_returns = []
    max_drawdowns = []
    
    for _ in range(n_iterations):
        # Sample starting indices for blocks with replacement
        start_indices = np.random.randint(0, n - block_size_bars + 1, size=n_blocks)
        blocks = [pnl[start:start + block_size_bars] for start in start_indices]
        bootstrapped_pnl = np.concatenate(blocks)[:n]
        
        # Calculate annualized return
        mean_ret = np.mean(bootstrapped_pnl)
        ann_ret = mean_ret * 105120  # 5m bars per year
        annualized_returns.append(ann_ret)
        
        # Calculate max drawdown
        equity = np.cumprod(1.0 + bootstrapped_pnl)
        peak = np.maximum.accumulate(equity)
        dd = (peak - equity) / peak
        max_drawdowns.append(np.max(dd))
        
    return {
        "bootstrap_return_p05": float(np.percentile(annualized_returns, 5)),
        "bootstrap_return_p50": float(np.percentile(annualized_returns, 50)),
        "bootstrap_return_p95": float(np.percentile(annualized_returns, 95)),
        "bootstrap_drawdown_p05": float(np.percentile(max_drawdowns, 5)),  # Best case DD
        "bootstrap_drawdown_p50": float(np.percentile(max_drawdowns, 50)),
        "bootstrap_drawdown_p95": float(np.percentile(max_drawdowns, 95)), # Worst case DD
    }


def simulate_parameter_perturbation(
    pnl_series: pd.Series,
    noise_std_dev: float = 0.05,
    n_iterations: int = 100,
) -> Dict[str, float]:
    """
    Simulates parameter perturbation by injecting random normally-distributed noise 
    scaled by the strategy's own standard deviation.
    Used to proxy how fragile the strategy edge is to minor timing/pricing changes.
    """
    pnl = pd.to_numeric(pnl_series, errors="coerce").dropna().values
    if len(pnl) == 0:
        return {}
        
    base_std = np.std(pnl)
    if base_std == 0:
        return {}

    annualized_returns = []
    for _ in range(n_iterations):
        noise = np.random.normal(0, base_std * noise_std_dev, size=len(pnl))
        perturbed_pnl = pnl + noise
        
        mean_ret = np.mean(perturbed_pnl)
        ann_ret = mean_ret * 105120
        annualized_returns.append(ann_ret)

    return {
        "perturbation_return_p05": float(np.percentile(annualized_returns, 5)),
        "perturbation_return_p50": float(np.percentile(annualized_returns, 50)),
        "perturbation_return_p95": float(np.percentile(annualized_returns, 95)),
        "perturbation_degradation_pct": float(np.percentile(annualized_returns, 5) / (np.mean(pnl) * 105120) - 1.0) if np.mean(pnl) > 0 else 0.0,
        "fraction_positive": float(np.mean([r > 0.0 for r in annualized_returns])),
    }


def analyze_regime_segmentation(
    pnl_series: pd.Series,
    volatility_series: pd.Series,
    q_high: float = 0.75,
    q_low: float = 0.25,
) -> Dict[str, float]:
    """
    Segments PnL based on market regimes (e.g., high volatility vs low volatility),
    assuming volatility_series is aligned with pnl_series.
    """
    pnl = pd.to_numeric(pnl_series, errors="coerce")
    vol = pd.to_numeric(volatility_series, errors="coerce")
    
    df = pd.DataFrame({"pnl": pnl, "vol": vol}).dropna()
    if df.empty:
        return {}
        
    high_thresh = df["vol"].quantile(q_high)
    low_thresh = df["vol"].quantile(q_low)
    
    high_vol_pnl = df.loc[df["vol"] >= high_thresh, "pnl"]
    low_vol_pnl = df.loc[df["vol"] <= low_thresh, "pnl"]
    mid_vol_pnl = df.loc[(df["vol"] > low_thresh) & (df["vol"] < high_thresh), "pnl"]

    # Annualization factor for subset
    def _ann_ret(subset: pd.Series) -> float:
        if subset.empty:
            return 0.0
        return float(subset.mean() * 105120)

    return {
        "high_vol_regime_annualized": _ann_ret(high_vol_pnl),
        "low_vol_regime_annualized": _ann_ret(low_vol_pnl),
        "mid_vol_regime_annualized": _ann_ret(mid_vol_pnl),
        "high_vol_exposure_fraction": float(len(high_vol_pnl) / len(df)),
    }
