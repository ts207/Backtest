from events.registry import _load_event_specs, EVENT_REGISTRY_SPECS

def test_dynamic_loading():
    # Ensure loaded specs match the hardcoded baseline (sanity check)
    loaded = _load_event_specs()
    assert "VOL_SHOCK" in loaded
    assert loaded["VOL_SHOCK"].signal_column == "vol_shock_relaxation_event"
    assert len(loaded) >= 11

def test_load_event_specs_fail_fast_on_missing_fields(tmp_path, monkeypatch):
    import inspect
    import pytest
    import yaml
    
    spec_dir = tmp_path / "spec" / "events"
    spec_dir.mkdir(parents=True)
    
    valid_spec = spec_dir / "valid.yaml"
    valid_spec.write_text(yaml.dump({
        "event_type": "VALID_EVENT",
        "reports_dir": "valid_reports",
        "events_file": "events.csv",
        "signal_column": "valid_event"
    }))
    
    malformed_spec = spec_dir / "malformed.yaml"
    malformed_spec.write_text(yaml.dump({
        "event_type": "MALFORMED_EVENT",
        "reports_dir": "malformed_reports",
        "events_file": "events.csv"
    }))
    
    # project_root parent becomes tmp_path
    monkeypatch.setattr("events.registry.PROJECT_ROOT", tmp_path / "project")
    
    with pytest.raises(ValueError, match="Malformed spec malformed.yaml — missing registry fields"):
        _load_event_specs()
