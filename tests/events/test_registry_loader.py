from events.registry import _load_event_specs, EVENT_REGISTRY_SPECS

def test_dynamic_loading():
    # Ensure loaded specs match the hardcoded baseline (sanity check)
    loaded = _load_event_specs()
    assert "vol_shock_relaxation" in loaded
    assert loaded["vol_shock_relaxation"].signal_column == "vol_shock_relaxation_event"
    assert len(loaded) >= 16  # We should have loaded at least 16 specs
