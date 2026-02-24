from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research import phase2_candidate_discovery as p2


def test_operator_registry_has_all_family_templates():
    families = p2._load_family_spec().get("families", {})
    verb_lexicon = p2._load_template_verb_lexicon()
    operators = p2._operator_registry(verb_lexicon)
    assert operators

    for event_type, cfg in families.items():
        canonical_family = str(cfg.get("canonical_family", event_type)).strip().upper()
        templates = cfg.get("templates", [])
        for verb in templates:
            op = p2._validate_operator_for_event(
                template_verb=str(verb),
                canonical_family=canonical_family,
                operator_registry=operators,
            )
            assert op


def test_operator_registry_rejects_incompatible_family():
    operators = {
        "desync_repair": {
            "operator_id": "op.desync_repair",
            "operator_version": "v1",
            "compatible_families": ["INFORMATION_DESYNC"],
        }
    }
    with pytest.raises(ValueError, match="incompatible with family"):
        p2._validate_operator_for_event(
            template_verb="desync_repair",
            canonical_family="LIQUIDITY_DISLOCATION",
            operator_registry=operators,
        )

