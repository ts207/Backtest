from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


@dataclass
class KillSwitchState:
    """
    State tracking for early abort conditions in live trading or walk-forwards.
    """
    is_triggered: bool = False
    trigger_reason: Optional[str] = None
    trigger_timestamp: Optional[pd.Timestamp] = None
    metrics: Dict[str, float] = field(default_factory=dict)
    
    def trigger(self, reason: str, ts: pd.Timestamp) -> None:
        if not self.is_triggered:
            self.is_triggered = True
            self.trigger_reason = reason
            self.trigger_timestamp = ts


def evaluate_kill_switches(
    portfolio_pnl_series: pd.Series,
    slippage_bps_series: pd.Series,
    config: Dict[str, object],
) -> KillSwitchState:
    """
    Vectorized evaluation of kill switches across the backtest/walk-forward window.
    If a kill condition is met, returns a triggered state with the earliest timestamp.
    """
    state = KillSwitchState()
    trigger_events = []
    
    # Check max intraday loss (or max drawdown from peak)
    max_loss_limit = float(config.get("max_intraday_loss_limit", np.inf))
    if max_loss_limit < np.inf and not portfolio_pnl_series.empty:
        INITIAL_EQUITY = 10000.0  # Common backtest initial equity
        equity = INITIAL_EQUITY + portfolio_pnl_series.fillna(0.0).cumsum()
        peak = equity.cummax()
        dd = (peak - equity) / peak
        
        breaches = dd[dd > max_loss_limit]
        if not breaches.empty:
            trigger_events.append(("Max loss limit breached", breaches.index[0], float(breaches.iloc[0])))

    # Check anomalous slippage 
    max_slip_limit = float(config.get("max_anomalous_slippage_bps", np.inf))
    if max_slip_limit < np.inf and not slippage_bps_series.empty:
        # Check rolling mean slippage over a fixed window (e.g., 288 bars = 1 day).
        # Require at least 288 bars before triggering to avoid single-bar noise.
        roll_slip = slippage_bps_series.fillna(0.0).rolling(window=288, min_periods=288).mean()
        breaches = roll_slip[roll_slip > max_slip_limit]
        if not breaches.empty:
            trigger_events.append(("Anomalous slippage", breaches.index[0], float(breaches.iloc[0])))
            
    if trigger_events:
        # Sort by timestamp to find the absolute earliest trigger
        trigger_events.sort(key=lambda x: x[1])
        first_event = trigger_events[0]
        state.trigger(first_event[0] + f" ({first_event[2]:.4f})", pd.Timestamp(first_event[1]))

    return state
