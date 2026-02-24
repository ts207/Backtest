from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping


def _norm_token(value: Any, *, default: str) -> str:
    token = str(value or "").strip().upper()
    return token if token else default


def _first_present(row: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = str(row.get(key, "")).strip()
        if value:
            return value
    return ""


@dataclass(frozen=True)
class StructuralEdgeComponents:
    event_type: str
    template_family: str
    direction_rule: str
    signal_polarity_logic: str


def structural_edge_components(row: Mapping[str, Any]) -> StructuralEdgeComponents:
    event_type = _norm_token(
        _first_present(row, "canonical_event_type", "event_type", "event"),
        default="UNKNOWN_EVENT",
    )
    template_family = _norm_token(
        _first_present(row, "template_family", "template_id", "template_verb", "rule_template"),
        default="UNKNOWN_TEMPLATE",
    )
    direction_rule = _norm_token(
        _first_present(row, "direction_rule", "direction", "trade_direction", "action"),
        default="UNKNOWN_DIRECTION",
    )
    signal_polarity_logic = _norm_token(
        _first_present(row, "signal_polarity_logic", "side_policy", "polarity_logic", "signal_polarity"),
        default="UNKNOWN_POLARITY",
    )
    return StructuralEdgeComponents(
        event_type=event_type,
        template_family=template_family,
        direction_rule=direction_rule,
        signal_polarity_logic=signal_polarity_logic,
    )


def edge_id_from_components(components: StructuralEdgeComponents) -> str:
    payload = {
        "event_type": components.event_type,
        "template_family": components.template_family,
        "direction_rule": components.direction_rule,
        "signal_polarity_logic": components.signal_polarity_logic,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "edge_" + hashlib.sha256(encoded).hexdigest()[:20]


def edge_id_from_row(row: Mapping[str, Any]) -> str:
    return edge_id_from_components(structural_edge_components(row))

