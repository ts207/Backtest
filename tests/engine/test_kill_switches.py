import pandas as pd
import pytest
import numpy as np

import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from engine.kill_switches import KillSwitchState, evaluate_kill_switches


def test_max_intraday_loss_trigger():
    # Simulate a steady PnL curve that suddenly drops
    idx = pd.date_range("2026-01-01", periods=6, freq="5min", tz="UTC")
    pnl = pd.Series([100.0, 200.0, 100.0, -500.0, -600.0, 100.0], index=idx)
    # Peak index 2 (equity = 1.01 * 1.02 * 1.01 ~= 1.0405)
    # Then drops significantly.
    config = {"max_intraday_loss_limit": 0.04}
    
    state = evaluate_kill_switches(
        portfolio_pnl_series=pnl,
        slippage_bps_series=pd.Series(dtype=float),
        config=config,
    )
    
    assert state.is_triggered is True
    assert "Max loss limit breached" in state.trigger_reason
    assert state.trigger_timestamp == idx[3]   # Bar 3 is where DD first exceeds 4%


def test_anomalous_slippage_trigger():
    # Simulate high sustained slippage
    idx = pd.date_range("2026-01-01", periods=500, freq="5min", tz="UTC")
    pnl = pd.Series([0.0] * 500, index=idx)
    # 300 bars of slippage = 50 bps
    slippage = pd.Series([10.0] * 200 + [60.0] * 300, index=idx)
    config = {"max_anomalous_slippage_bps": 50.0}
    
    state = evaluate_kill_switches(
        portfolio_pnl_series=pnl,
        slippage_bps_series=slippage,
        config=config,
    )
    
    assert state.is_triggered is True
    assert "Anomalous slippage" in state.trigger_reason
    # Should trigger when rolling 288 bar mean (with min 288 bars) first exceeds 50.0.
    # First valid reading is bar 287. Mean at that point = (88*10 + 200*60)/288 ~= 44.7, still below 50.
    # Mean crosses 50 when high-slip (60 bps) bars dominate enough of the 288 window.
    assert state.trigger_timestamp is not None


def test_respects_earliest_trigger():
    idx = pd.date_range("2026-01-01", periods=505, freq="5min", tz="UTC")
    pnl = pd.Series([100.0, 200.0, 100.0, -1500.0, -600.0] + [0.0]*500, index=idx)
    slippage = pd.Series([60.0] * 505, index=idx)
    config = {
        "max_intraday_loss_limit": 0.04,
        "max_anomalous_slippage_bps": 50.0,
    }
    
    state = evaluate_kill_switches(
        portfolio_pnl_series=pnl,
        slippage_bps_series=slippage,
        config=config,
    )
    
    assert state.is_triggered is True
    # The PnL breach happens at bar 3. The slippage breach happens later.
    assert state.trigger_timestamp == idx[3]
    assert "Max loss limit breached" in state.trigger_reason
