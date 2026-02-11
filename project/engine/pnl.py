from __future__ import annotations

import pandas as pd


def compute_returns(close: pd.Series) -> pd.Series:
    """
    Compute simple returns from close prices.
    """
    ret = close.pct_change()
    return ret.fillna(0.0)


def compute_pnl(pos: pd.Series, ret: pd.Series, cost_bps: float) -> pd.Series:
    """
    Compute per-bar PnL with next-bar returns and turnover costs.
    pnl = pos.shift(1) * ret - abs(pos - pos.shift(1)) * (cost_bps / 10000.0)
    """
    aligned_pos = pos.reindex(ret.index).fillna(0.0)
    prior_pos = aligned_pos.shift(1).fillna(0.0)
    turnover = (aligned_pos - prior_pos).abs() * (cost_bps / 10000.0)
    pnl = prior_pos * ret - turnover
    return pnl
