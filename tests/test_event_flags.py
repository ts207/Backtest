from __future__ import annotations
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "project"))


import numpy as np
import pandas as pd
import pytest
from pathlib import Path
from events.registry import build_event_flags, _active_signal_column

def test_build_event_flags_correctness():
    # 1. Setup small synthetic grid (1 day, 5m bars = 288 bars)
    ts = pd.date_range("2024-01-01", periods=10, freq="5min", tz="UTC")
    symbols = ["BTCUSDT", "ETHUSDT"]
    
    # Mock symbols timestamps loader
    import events.registry as registry
    original_loader = registry._load_symbol_timestamps
    registry._load_symbol_timestamps = lambda data_root, run_id, symbol, timeframe: pd.Series(ts)
    
    # 2. Define events
    event_data = [
        # Normal event: snapped to T=00:05
        {
            "signal_column": "vol_shock_relaxation_event",
            "timestamp": ts[1] - pd.Timedelta(seconds=10), # 00:04:50 -> snaps to 00:05:00
            "signal_ts": ts[1] - pd.Timedelta(seconds=10),
            "enter_ts": ts[1],
            "exit_ts": ts[3], # Active for indices 1, 2, 3
            "symbol": "BTCUSDT"
        },
        # ALL broadcast event
        {
            "signal_column": "absorption_event",
            "timestamp": ts[5],
            "signal_ts": ts[5],
            "enter_ts": ts[5],
            "exit_ts": ts[5], # Active for index 5 only
            "symbol": "ALL"
        }
    ]
    events = pd.DataFrame(event_data)
    # Add required columns
    events["run_id"] = "test"
    events["event_type"] = "MOCK"
    events["event_id"] = [f"ev_{i}" for i in range(len(events))]
    events["features_at_event"] = "{}"
    
    # 3. Run vectorized implementation
    flags = build_event_flags(
        events=events,
        symbols=symbols,
        data_root=Path("/tmp"),
        run_id="test"
    )
    
    # 4. Assertions
    # BTCUSDT check
    btc = flags[flags["symbol"] == "BTCUSDT"].sort_values("timestamp")
    
    # vol_shock_relaxation_event at index 1
    assert btc.iloc[1]["vol_shock_relaxation_event"] == True
    assert btc.iloc[0]["vol_shock_relaxation_event"] == False
    
    # vol_shock_relaxation_active for [1, 2, 3]
    active_col = _active_signal_column("vol_shock_relaxation_event")
    assert btc.iloc[1][active_col] == True
    assert btc.iloc[2][active_col] == True
    assert btc.iloc[3][active_col] == True
    assert btc.iloc[4][active_col] == False
    
    # ETHUSDT check (ALL broadcast)
    eth = flags[flags["symbol"] == "ETHUSDT"].sort_values("timestamp")
    assert eth.iloc[5]["absorption_event"] == True
    assert eth.iloc[5][_active_signal_column("absorption_event")] == True
    
    # Restore loader
    registry._load_symbol_timestamps = original_loader

if __name__ == "__main__":
    pytest.main([__file__])
