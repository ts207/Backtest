from events.registry import _load_event_specs, EVENT_REGISTRY_SPECS

def test_dynamic_loading():
    # Ensure loaded specs match the hardcoded baseline (sanity check)
    loaded = _load_event_specs()
    assert "VOL_SHOCK" in loaded
    assert loaded["VOL_SHOCK"].signal_column == "vol_shock_relaxation_event"
    assert len(loaded) >= 11
