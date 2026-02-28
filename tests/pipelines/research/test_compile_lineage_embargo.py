from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research.compile_strategy_blueprints import _build_blueprint, _load_operator_registry


def test_build_blueprint_records_bridge_embargo_in_lineage():
    row = {
        "candidate_id": "cand_1",
        "source_path": "data/reports/phase2/r1/VOL_SHOCK/phase2_candidates.csv",
        "promotion_track": "standard",
        "bridge_embargo_days_used": 3,
        "n_events": 150,
        "condition": "all",
        "action": "long",
        "event": "VOL_SHOCK",
    }

    bp, _ = _build_blueprint(
        run_id="r1",
        run_symbols=["BTCUSDT"],
        event_type="VOL_SHOCK",
        row=row,
        phase2_lookup={},
        stats={},
        fees_bps=2.0,
        slippage_bps=4.0,
        min_events=100,
        cost_config_digest="digest",
    )

    assert bp.lineage.bridge_embargo_days_used == 3


def test_blueprint_contains_ontology_contract():
    row = {
        "candidate_id": "cand_2",
        "source_path": "data/reports/phase2/r1/CROSS_VENUE_DESYNC/phase2_candidates.csv",
        "promotion_track": "standard",
        "bridge_embargo_days_used": 3,
        "n_events": 150,
        "condition": "all",
        "action": "long",
        "event": "CROSS_VENUE_DESYNC",
        "canonical_event_type": "CROSS_VENUE_DESYNC",
        "canonical_family": "INFORMATION_DESYNC",
        "template_verb": "desync_repair",
        "state_id": "DESYNC_PERSISTENCE_STATE",
    }

    bp, _ = _build_blueprint(
        run_id="r1",
        run_symbols=["BTCUSDT"],
        event_type="CROSS_VENUE_DESYNC",
        row=row,
        phase2_lookup={},
        stats={},
        fees_bps=2.0,
        slippage_bps=4.0,
        min_events=100,
        cost_config_digest="digest",
        ontology_spec_hash_value="sha256:test",
        operator_registry=_load_operator_registry(),
    )

    assert bp.lineage.ontology_spec_hash == "sha256:test"
    assert bp.lineage.canonical_event_type == "CROSS_VENUE_DESYNC"
    assert bp.lineage.canonical_family == "INFORMATION_DESYNC"
    assert bp.lineage.template_verb == "desync_repair"
    assert bp.lineage.operator_version
