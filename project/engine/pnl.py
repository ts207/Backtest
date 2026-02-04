from __future__ import annotations

import pandas as pd


def compute_returns(close: pd.Series) -> pd.Series:
    """
    Compute simple returns from close prices.
    """
    return close.pct_change(fill_method=None)


def compute_pnl(pos: pd.Series, ret: pd.Series, cost_bps: float) -> pd.Series:
    """
    Compute per-bar PnL with next-bar returns and turnover costs.
    pnl = pos.shift(1) * ret - abs(pos - pos.shift(1)) * (cost_bps / 10000.0)
    NaN returns are treated as gap bars and set to 0 PnL.
    """
    aligned_pos = pos.reindex(ret.index).fillna(0.0)
    prior_pos = aligned_pos.shift(1).fillna(0.0)
    turnover = (aligned_pos - prior_pos).abs() * (cost_bps / 10000.0)
    pnl = prior_pos * ret - turnover
    nan_ret = ret.isna()
    if nan_ret.any():
        pnl = pnl.copy()
        pnl[nan_ret] = 0.0
    return pnl
