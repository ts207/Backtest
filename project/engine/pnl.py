from __future__ import annotations

from typing import Dict

import pandas as pd


def compute_returns(close: pd.Series) -> pd.Series:
    """
    Compute simple returns from close prices.
    """
    return close.pct_change(fill_method=None)


def compute_pnl_components(
    pos: pd.Series,
    ret: pd.Series,
    cost_bps: float,
    funding_rate: pd.Series | None = None,
    borrow_rate: pd.Series | None = None,
) -> pd.DataFrame:
    """
    Compute per-bar PnL components with next-bar returns and turnover costs.

    Components:
    - gross_pnl: prior position * return
    - trading_cost: turnover * cost_bps
    - funding_pnl: carry transfer from funding rates (positive for beneficial carry)
    - borrow_cost: borrowing cost charged on short exposure
    - pnl: net of all components

    NaN returns are treated as gap bars and set to 0 across all components.
    """
    aligned_pos = pos.reindex(ret.index).fillna(0.0).astype(float)
    prior_pos = aligned_pos.shift(1).fillna(0.0)

    gross_pnl = prior_pos * ret
    trading_cost = (aligned_pos - prior_pos).abs() * (float(cost_bps) / 10000.0)

    if funding_rate is None:
        funding_rate_aligned = pd.Series(0.0, index=ret.index, dtype=float)
    else:
        funding_rate_aligned = pd.to_numeric(funding_rate.reindex(ret.index), errors="coerce").fillna(0.0).astype(float)

    if borrow_rate is None:
        borrow_rate_aligned = pd.Series(0.0, index=ret.index, dtype=float)
    else:
        borrow_rate_aligned = pd.to_numeric(borrow_rate.reindex(ret.index), errors="coerce").fillna(0.0).astype(float)

    # Longs pay positive funding, shorts receive positive funding.
    funding_pnl = -prior_pos * funding_rate_aligned
    # Borrow cost only applies to short exposure magnitude.
    borrow_cost = prior_pos.clip(upper=0.0).abs() * borrow_rate_aligned

    pnl = gross_pnl - trading_cost + funding_pnl - borrow_cost

    nan_ret = ret.isna()
    if nan_ret.any():
        gross_pnl = gross_pnl.copy()
        trading_cost = trading_cost.copy()
        funding_pnl = funding_pnl.copy()
        borrow_cost = borrow_cost.copy()
        pnl = pnl.copy()
        gross_pnl[nan_ret] = 0.0
        trading_cost[nan_ret] = 0.0
        funding_pnl[nan_ret] = 0.0
        borrow_cost[nan_ret] = 0.0
        pnl[nan_ret] = 0.0

    return pd.DataFrame(
        {
            "gross_pnl": gross_pnl,
            "trading_cost": trading_cost,
            "funding_pnl": funding_pnl,
            "borrow_cost": borrow_cost,
            "pnl": pnl,
        },
        index=ret.index,
    )


def compute_pnl(
    pos: pd.Series,
    ret: pd.Series,
    cost_bps: float,
    funding_rate: pd.Series | None = None,
    borrow_rate: pd.Series | None = None,
) -> pd.Series:
    components = compute_pnl_components(
        pos=pos,
        ret=ret,
        cost_bps=cost_bps,
        funding_rate=funding_rate,
        borrow_rate=borrow_rate,
    )
    return components["pnl"]
