from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from research.edge_identity import edge_id_from_row, structural_edge_components


def test_edge_id_ignores_horizon_and_conditioning_variation():
    base = {
        "canonical_event_type": "VOL_SHOCK",
        "template_id": "mean_reversion",
        "direction_rule": "contrarian",
        "signal_polarity_logic": "shock_up_short_shock_down_long",
    }
    row_a = dict(base, horizon_bars=6, condition_signature="severity_bucket=high")
    row_b = dict(base, horizon_bars=12, condition_signature="severity_bucket=extreme")
    assert edge_id_from_row(row_a) == edge_id_from_row(row_b)


def test_structural_edge_components_fall_back_to_runtime_fields():
    row = {
        "event_type": "LIQUIDITY_VACUUM",
        "template_verb": "mean_reversion",
        "action": "short",
    }
    c = structural_edge_components(row)
    assert c.event_type == "LIQUIDITY_VACUUM"
    assert c.template_family == "MEAN_REVERSION"
    assert c.direction_rule == "SHORT"
