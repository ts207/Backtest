from pipelines.research.generate_candidate_templates import _get_templates_for_event


def test_taxonomy_mapping_canonical_events():
    templates = _get_templates_for_event("LIQUIDITY_VACUUM")
    assert "mean_reversion" in templates
    assert "continuation" in templates
    assert "liquidity_reversion_v2" in templates

    templates = _get_templates_for_event("VOL_SHOCK")
    assert "continuation" in templates
    assert "trend_continuation" in templates


def test_taxonomy_mapping_unknown_legacy_label_uses_defaults():
    templates = _get_templates_for_event("legacy_state_window")
    assert templates == _get_templates_for_event("unknown_event")


def test_taxonomy_mapping_unknown_event_uses_defaults():
    templates = _get_templates_for_event("unknown_event")
    assert "mean_reversion" in templates
    assert "continuation" in templates
