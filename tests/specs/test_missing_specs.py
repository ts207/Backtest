import os
from pathlib import Path

def test_volatility_specs_exist():
    root = Path("spec/events")
    assert (root / "vol_shock_relaxation.yaml").exists()
    assert (root / "vol_aftershock_window.yaml").exists()
    assert (root / "range_compression_breakout_window.yaml").exists()
