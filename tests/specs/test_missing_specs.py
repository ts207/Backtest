import os
from pathlib import Path

def test_event_specs_exist():
    root = Path("spec/events")
    # Canonical event specs
    assert (root / "VOL_SHOCK.yaml").exists()
    assert (root / "LIQUIDITY_VACUUM.yaml").exists()
    assert (root / "FORCED_FLOW_EXHAUSTION.yaml").exists()
    assert (root / "CROSS_VENUE_DESYNC.yaml").exists()
    assert (root / "FUNDING_EXTREME_ONSET.yaml").exists()
    assert (root / "FUNDING_PERSISTENCE_TRIGGER.yaml").exists()
    assert (root / "FUNDING_NORMALIZATION_TRIGGER.yaml").exists()
    assert (root / "OI_SPIKE_POSITIVE.yaml").exists()
    assert (root / "OI_SPIKE_NEGATIVE.yaml").exists()
    assert (root / "OI_FLUSH.yaml").exists()
    assert (root / "LIQUIDATION_CASCADE.yaml").exists()
