from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd


def compute_returns(close: pd.Series) -> pd.Series:
    """
    Compute simple close-to-close returns.
    Uses fill_method=None to ensure gaps resulting in NaN returns are propagated and not smoothed.
    """
    return close.pct_change(fill_method=None)


def compute_returns_next_open(
    close: pd.Series,
    open_: pd.Series,
    positions: pd.Series,
) -> pd.Series:
    """
    Compute blended returns for next-open execution mode.

    - At entry bars (prior_pos == 0, current pos != 0): return = open[t+1] / close[t] - 1
      (representing fill at next bar's open rather than current close).
    - At hold/exit bars: return = close[t] / close[t-1] - 1 (standard close-to-close).
    - NaN propagation is preserved for gap bars.

    Args:
        close: Close price series indexed by timestamp.
        open_: Open price series indexed by timestamp (aligned to same index).
        positions: Position series (integer -1/0/1) indexed by timestamp.

    Returns:
        Blended returns series aligned to close.index.
    """
    aligned_pos = positions.reindex(close.index).fillna(0).astype(int)
    prior_pos = aligned_pos.shift(1).fillna(0).astype(int)
    is_entry = (prior_pos == 0) & (aligned_pos != 0)

    # Close-to-close returns (standard)
    cc_ret = close.pct_change(fill_method=None)

    # Entry bar: open[t+1] / close[t] - 1
    next_open = open_.shift(-1)
    safe_close = close.replace(0.0, np.nan)
    entry_ret = (next_open / safe_close - 1.0).replace([np.inf, -np.inf], np.nan)

    # Blend: entry bars use next_open return; everything else uses close-to-close
    blended = cc_ret.copy()
    blended[is_entry] = entry_ret[is_entry]
    return blended


def compute_funding_pnl_event_aligned(
    pos: pd.Series,
    funding_rate: pd.Series,
    funding_hours: tuple[int, ...] = (0, 8, 16),
) -> pd.Series:
    """
    Apply funding only on bars whose timestamp falls on a funding event hour (0, 8, 16 UTC).
    Position is the prior-bar position (signal held going into the event timestamp).

    Args:
        pos: Position series (float, signed) indexed by UTC timestamp.
        funding_rate: Per-event funding rate series aligned to the same index.
        funding_hours: UTC hours at which funding is charged.

    Returns:
        Per-bar funding PnL series (positive = beneficial for longs receiving negative funding).
    """
    aligned_pos = pos.reindex(funding_rate.index).fillna(0.0).astype(float)
    prior_pos = aligned_pos.shift(1).fillna(0.0)

    rate_aligned = pd.to_numeric(funding_rate.reindex(pos.index), errors="coerce").fillna(0.0)

    is_funding_bar = pd.Series(False, index=pos.index)
    if hasattr(pos.index, "hour"):
        is_funding_bar = pd.Series(
            pos.index.hour.isin(funding_hours) & (pos.index.minute == 0),
            index=pos.index,
        )

    # Longs pay positive funding; shorts receive it.
    raw_funding = -prior_pos * rate_aligned
    return raw_funding.where(is_funding_bar, 0.0)


def compute_pnl_components(
    pos: pd.Series,
    ret: pd.Series,
    cost_bps: float | pd.Series,
    funding_rate: pd.Series | None = None,
    borrow_rate: pd.Series | None = None,
    use_event_aligned_funding: bool = False,
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
    if isinstance(cost_bps, pd.Series):
        cost_bps_aligned = pd.to_numeric(cost_bps.reindex(ret.index), errors="coerce").fillna(0.0).astype(float)
    else:
        cost_bps_aligned = pd.Series(float(cost_bps), index=ret.index, dtype=float)
    trading_cost = (aligned_pos - prior_pos).abs() * (cost_bps_aligned / 10000.0)

    if funding_rate is None:
        funding_rate_aligned = pd.Series(0.0, index=ret.index, dtype=float)
    else:
        funding_rate_aligned = pd.to_numeric(funding_rate.reindex(ret.index), errors="coerce").fillna(0.0).astype(float)

    if borrow_rate is None:
        borrow_rate_aligned = pd.Series(0.0, index=ret.index, dtype=float)
    else:
        borrow_rate_aligned = pd.to_numeric(borrow_rate.reindex(ret.index), errors="coerce").fillna(0.0).astype(float)

    # Longs pay positive funding, shorts receive positive funding.
    if use_event_aligned_funding and funding_rate is not None:
        funding_pnl = compute_funding_pnl_event_aligned(
            pos=pos.reindex(ret.index).fillna(0.0).astype(float),
            funding_rate=funding_rate_aligned,
        )
    else:
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
    cost_bps: float | pd.Series,
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

