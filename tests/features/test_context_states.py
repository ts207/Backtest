import numpy as np
import pandas as pd
import pytest
from features.context_states import (
    calculate_ms_vol_state,
    calculate_ms_liq_state,
    calculate_ms_oi_state,
    calculate_ms_funding_state,
    encode_context_state_code,
)

def test_calculate_ms_vol_state():
    rv_pct = pd.Series([10.0, 50.0, 80.0, 98.0])
    states = calculate_ms_vol_state(rv_pct)
    assert states.iloc[0] == 0.0 # LOW
    assert states.iloc[1] == 1.0 # MID
    assert states.iloc[2] == 2.0 # HIGH
    assert states.iloc[3] == 3.0 # SHOCK

def test_calculate_ms_liq_state():
    # Sine wave ensures we hit all quantiles
    vol = pd.Series(np.sin(np.linspace(0, 10, 500)) + 2.0)
    states = calculate_ms_liq_state(vol, window=100)
    
    # Peak of sine is max -> FLUSH (2.0)
    # Trough of sine is min -> THIN (0.0)
    # Somewhere in between -> NORMAL (1.0)
    
    assert 2.0 in states.values
    assert 0.0 in states.values
    assert 1.0 in states.values

def test_calculate_ms_oi_state():
    # Constant OI delta -> z-score will be 0 (STABLE)
    oi_delta = pd.Series([10.0] * 100)
    states = calculate_ms_oi_state(oi_delta, window=50)
    assert states.iloc[-1] == 1.0
    
    # Large spike
    oi_delta.iloc[-1] = 1000.0
    states = calculate_ms_oi_state(oi_delta, window=50)
    assert states.iloc[-1] == 2.0 # ACCEL

def test_calculate_ms_funding_state():
    # Persistent positive funding
    fnd = pd.Series([0.5] * 100)
    states = calculate_ms_funding_state(fnd, window=20)
    assert states.iloc[-1] == 1.0 # PERSISTENT
    
    # Extreme positive funding
    fnd_ext = pd.Series([5.0] * 100)
    states_ext = calculate_ms_funding_state(fnd_ext, window=5)
    assert states_ext.iloc[-1] == 2.0 # EXTREME

def test_encode_context_state_code():
    vol = pd.Series([3.0])
    liq = pd.Series([0.0])
    oi = pd.Series([2.0])
    fnd = pd.Series([1.0])
    code = encode_context_state_code(vol, liq, oi, fnd)
    assert code.iloc[0] == 3021.0
