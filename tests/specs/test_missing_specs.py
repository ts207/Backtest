import os
from pathlib import Path

def test_event_specs_exist():
    root = Path("spec/events")
    # Volatility
    assert (root / "vol_shock_relaxation.yaml").exists()
    assert (root / "vol_aftershock_window.yaml").exists()
    assert (root / "range_compression_breakout_window.yaml").exists()

    # Liquidity
    assert (root / "liquidity_refill_lag_window.yaml").exists()
    assert (root / "liquidity_absence_window.yaml").exists()
    assert (root / "liquidity_vacuum.yaml").exists()
