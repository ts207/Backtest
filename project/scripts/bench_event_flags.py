from __future__ import annotations

import time
from pathlib import Path
import numpy as np
import pandas as pd
import sys
import os

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(WORKSPACE_ROOT))

from project.events.registry import build_event_flags, REGISTRY_BACKED_SIGNALS

def bench():
    # Setup synthetic data
    n_bars = 100000
    n_events = 5000
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    
    ts = pd.date_range("2024-01-01", periods=n_bars, freq="5min", tz="UTC")
    
    # Mock events
    event_list = []
    signals = list(REGISTRY_BACKED_SIGNALS)
    if not signals:
        signals = ["absorption_event", "vol_shock_event"]
        
    for i in range(n_events):
        sym = np.random.choice(symbols + ["ALL"])
        sig_col = np.random.choice(signals)
        sig_ts = np.random.choice(ts)
        
        event_list.append({
            "run_id": "bench",
            "event_type": "MOCK",
            "signal_column": sig_col,
            "timestamp": sig_ts,
            "enter_ts": sig_ts,
            "detected_ts": sig_ts,
            "signal_ts": sig_ts,
            "exit_ts": sig_ts + pd.Timedelta(minutes=30),
            "symbol": sym,
            "event_id": f"ev_{i}",
            "features_at_event": "{}"
        })
    
    events = pd.DataFrame(event_list)
    
    # Mock data root and timestamp loader
    # We'll monkeypatch _load_symbol_timestamps to return our synthetic grid
    import project.events.registry as registry
    original_loader = registry._load_symbol_timestamps
    registry._load_symbol_timestamps = lambda data_root, run_id, symbol, timeframe: pd.Series(ts)
    
    print(f"Benchmarking build_event_flags with {n_bars} bars and {n_events} events across {len(symbols)} symbols...")
    
    start = time.perf_counter()
    flags = build_event_flags(
        events=events,
        symbols=symbols,
        data_root=Path("/tmp"),
        run_id="bench"
    )
    end = time.perf_counter()
    
    print(f"Elapsed: {end - start:.4f} seconds")
    print(f"Output shape: {flags.shape}")
    
    # Restore
    registry._load_symbol_timestamps = original_loader

if __name__ == "__main__":
    bench()
