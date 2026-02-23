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

    # Funding & OI
    assert (root / "funding_extreme_reversal_window.yaml").exists()
    assert (root / "funding_extreme_onset.yaml").exists()
    assert (root / "funding_persistence_window.yaml").exists()
    assert (root / "funding_normalization.yaml").exists()
    assert (root / "oi_spike_positive.yaml").exists()
    assert (root / "oi_spike_negative.yaml").exists()
    assert (root / "oi_flush.yaml").exists()

    # Remaining
    assert (root / "directional_exhaustion_after_forced_flow.yaml").exists()
    assert (root / "cross_venue_desync.yaml").exists()
    assert (root / "LIQUIDATION_CASCADE.yaml").exists()
