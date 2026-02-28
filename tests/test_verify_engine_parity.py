from __future__ import annotations
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "project"))


import numpy as np
import pandas as pd
import pytest
from scripts.verify_engine_parity import calculate_trade_parity

def test_calculate_trade_parity_success():
    # 1. Setup synthetic data
    timestamps = pd.to_datetime([
        "2024-01-01 00:00:00",
        "2024-01-01 00:05:00",
        "2024-01-01 00:10:00",
        "2024-01-01 00:15:00",
        "2024-01-01 00:20:00",
        "2024-01-01 00:25:00",
    ], utc=True)
    
    # Close prices: 100, 101, 102, 103, 104, 105
    bars = pd.DataFrame({
        "timestamp": timestamps,
        "close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    })
    
    # Features (same timestamps)
    features = pd.DataFrame({"timestamp": timestamps})
    
    # Signal at T=0
    sig_ts = timestamps[0]
    effective_lag = 1
    horizon_bars = 2
    
    # Expected:
    # entry_pos = 0 + 1 = 1 (T=00:05, Close=101)
    # future_pos = 1 + 2 = 3 (T=00:15, Close=103)
    
    # Engine DF
    # pos effective at T=00:05 (index 1) until T=00:10 (index 2)
    # Total 2 bars.
    engine_df = pd.DataFrame({
        "timestamp": timestamps,
        "pos": [0, 1, 1, 0, 0, 0],
        "ret": bars["close"].pct_change().fillna(0.0)
    })
    
    # Run parity
    res = calculate_trade_parity(
        sig_ts=sig_ts,
        entry_ts=timestamps[1],
        last_held_ts=timestamps[2],
        bars=bars,
        engine_df=engine_df,
        direction=1,
        horizon_bars=horizon_bars
    )
    
    assert res["timing_ok"] is True
    # Research LogRet: log(102/101)
    # entry_pos=1, future_pos=1+2=3. Close[3]=103? Wait!
    # If horizon=2, bars getting PnL are 2, 3.
    # log(Close[3]/Close[1]) = log(103/101)
    assert pytest.approx(res["research_logret"]) == np.log(103.0 / 101.0)
    # Engine LogRet: sum(log(1 + ret_i)) for i in [2, 3]
    # sum(log(1+ret)) for 2, 3 = log(Close[3]/Close[1])
    assert pytest.approx(res["engine_logret"]) == np.log(103.0 / 101.0)
    assert pytest.approx(res["cum_logret_diff"]) == 0.0

def test_calculate_trade_parity_timing_failure():
    # Setup same as above
    timestamps = pd.to_datetime([
        "2024-01-01 00:00:00", "2024-01-01 00:05:00", "2024-01-01 00:10:00",
        "2024-01-01 00:15:00", "2024-01-01 00:20:00", "2024-01-01 00:25:00",
    ], utc=True)
    bars = pd.DataFrame({"timestamp": timestamps, "close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]})
    sig_ts = timestamps[0]
    horizon_bars = 2
    
    # Engine DF with WRONG TIMING (shifted by 1 bar)
    # Expected last held at index 3, but engine held until index 4
    engine_df = pd.DataFrame({
        "timestamp": timestamps,
        "pos": [0, 1, 1, 1, 1, 0], 
        "ret": bars["close"].pct_change().fillna(0.0)
    })
    
    res = calculate_trade_parity(
        sig_ts=sig_ts,
        entry_ts=timestamps[1],
        last_held_ts=timestamps[4], # Wrong exit
        bars=bars,
        engine_df=engine_df,
        direction=1,
        horizon_bars=horizon_bars
    )
    
    assert res["timing_ok"] is False

def test_calculate_trade_parity_float_noise():
    # Setup same
    timestamps = pd.to_datetime([
        "2024-01-01 00:00:00", "2024-01-01 00:05:00", "2024-01-01 00:10:00",
        "2024-01-01 00:15:00", "2024-01-01 00:20:00", "2024-01-01 00:25:00",
    ], utc=True)
    bars = pd.DataFrame({"timestamp": timestamps, "close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]})
    sig_ts = timestamps[0]
    horizon_bars = 2
    
    # Engine DF with identical timing but small noise in returns
    engine_df = pd.DataFrame({
        "timestamp": timestamps,
        "pos": [0, 1, 1, 0, 0, 0],
        "ret": bars["close"].pct_change().fillna(0.0) + 1e-10
    })
    
    res = calculate_trade_parity(
        sig_ts=sig_ts,
        entry_ts=timestamps[1],
        last_held_ts=timestamps[2],
        bars=bars,
        engine_df=engine_df,
        direction=1,
        horizon_bars=horizon_bars,
        tol_abs=1e-3
    )
    
    assert res["timing_ok"] is True
    # Should be non-zero but very small
    assert abs(res["cum_logret_diff"]) > 0.0
    assert abs(res["cum_logret_diff"]) < 1e-6
