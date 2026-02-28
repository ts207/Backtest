import pytest
import numpy as np
from strategies.dsl_interpreter_v1 import generate_positions_numba

def test_dsl_numba_cooldown_reentry_lockout():
    n_bars = 10
    
    timestamps_arr = np.arange(n_bars, dtype=np.int64)
    close_arr = np.array([100.0] * n_bars, dtype=np.float64)
    high_arr = np.array([101.0] * n_bars, dtype=np.float64)
    low_arr = np.array([99.0] * n_bars, dtype=np.float64)
    
    eligible = np.zeros(n_bars, dtype=bool)
    eligible[0] = True
    eligible[6] = True
    
    entry_sides = np.ones(n_bars, dtype=np.float64) # long
    
    invalidation_mask = np.zeros(n_bars, dtype=bool)
    invalidation_mask[2] = True
    invalidation_mask[8] = True
    
    stop_offsets = np.ones(n_bars, dtype=np.float64) * 5.0 # Stop at 95
    target_offsets = np.ones(n_bars, dtype=np.float64) * 5.0 # Target at 105
    trailing_offsets = np.zeros(n_bars, dtype=np.float64)

    positions, timestamps_out, codes = generate_positions_numba(
        timestamps=timestamps_arr,
        close=close_arr,
        high=high_arr,
        low=low_arr,
        eligible=eligible,
        entry_sides=entry_sides,
        stop_offsets=stop_offsets,
        target_offsets=target_offsets,
        allow_intrabar_exits=False,
        cooldown_bars=2,
        reentry_lockout_bars=3,
        arm_bars_base=0,
        time_stop_bars=10,
        break_even_r=0.0,
        trailing_stop_type=0, # none
        trailing_offsets=trailing_offsets,
        invalidation_mask=invalidation_mask
    )
    
    # Bar 0: Eligible
    # Bar 1: Execute entry
    # Bar 2: Invalidation! -> state=3, cooldown=2+3=5
    # Bar 5: state=0
    # Bar 6: Eligible
    # Bar 7: Execute entry
    # Bar 8: Invalidation! -> state=3
    assert positions[0] == 0.0
    assert positions[1] == 1.0
    assert positions[2] == 0.0
    assert positions[3] == 0.0
    assert positions[4] == 0.0
    assert positions[5] == 0.0
    assert positions[6] == 0.0
    assert positions[7] == 1.0
    assert positions[8] == 0.0
    

def test_dsl_numba_trailing_stop():
    n_bars = 6
    timestamps_arr = np.arange(n_bars, dtype=np.int64)
    # 0: eligible, 1: enter @ 100, 2: 110, 3: 115, 4: 105, 5: 100
    close_arr = np.array([100.0, 100.0, 110.0, 115.0, 105.0, 100.0])
    high_arr = close_arr.copy()
    low_arr = close_arr.copy()
    
    eligible = np.zeros(n_bars, dtype=bool)
    eligible[0] = True
    entry_sides = np.ones(n_bars, dtype=np.float64)
    invalidation_mask = np.zeros(n_bars, dtype=bool)
    
    stop_offsets = np.ones(n_bars) * 20.0 
    target_offsets = np.ones(n_bars) * 100.0 
    trailing_offsets = np.ones(n_bars) * 5.0
    
    positions, timestamps_out, codes = generate_positions_numba(
        timestamps=timestamps_arr,
        close=close_arr,
        high=high_arr,
        low=low_arr,
        eligible=eligible,
        entry_sides=entry_sides,
        stop_offsets=stop_offsets,
        target_offsets=target_offsets,
        allow_intrabar_exits=True,
        cooldown_bars=0,
        reentry_lockout_bars=0,
        arm_bars_base=0,
        time_stop_bars=10,
        break_even_r=0.0,
        trailing_stop_type=1, # atr (or just non-zero)
        trailing_offsets=trailing_offsets,
        invalidation_mask=invalidation_mask
    )
    
    assert positions[0] == 0.0 # armed
    assert positions[1] == 1.0 # start
    assert positions[2] == 1.0 # 110
    assert positions[3] == 1.0 # 115, trail stop goes to 110
    assert positions[4] == 0.0 # 105 hits trail stop 110, exit


def test_dsl_numba_break_even_r():
    n_bars = 6
    timestamps_arr = np.arange(n_bars, dtype=np.int64)
    # 0: eligible, 1: enter @ 100, 2: 110 (hits 1R), 3: 105, 4: 95 (hits 100 BE stop)
    close_arr = np.array([100.0, 100.0, 110.0, 105.0, 95.0, 90.0]) 
    high_arr = close_arr.copy()
    low_arr = close_arr.copy()
    
    eligible = np.zeros(n_bars, dtype=bool)
    eligible[0] = True
    entry_sides = np.ones(n_bars, dtype=np.float64)
    invalidation_mask = np.zeros(n_bars, dtype=bool)
    
    # Risk is 10pts
    stop_offsets = np.ones(n_bars) * 10.0 
    target_offsets = np.ones(n_bars) * 100.0 
    trailing_offsets = np.zeros(n_bars)
    
    positions, timestamps_out, codes = generate_positions_numba(
        timestamps=timestamps_arr,
        close=close_arr,
        high=high_arr,
        low=low_arr,
        eligible=eligible,
        entry_sides=entry_sides,
        stop_offsets=stop_offsets,
        target_offsets=target_offsets,
        allow_intrabar_exits=True,
        cooldown_bars=0,
        reentry_lockout_bars=0,
        arm_bars_base=0,
        time_stop_bars=10,
        break_even_r=1.0,  # Move to break-even after +1R (+10pts = 110)
        trailing_stop_type=0,
        trailing_offsets=trailing_offsets,
        invalidation_mask=invalidation_mask
    )
    
    assert positions[0] == 0.0
    assert positions[1] == 1.0 # enters @ 100, stop 90
    assert positions[2] == 1.0 # reaches 110 (+1R) -> stop moves to 100
    assert positions[3] == 1.0 # 105 safe
    assert positions[4] == 0.0 # 95 hits new 100 break-even stop
