from __future__ import annotations

from typing import Optional


def canonical_bh_group_key(
    *,
    canonical_family: str,
    canonical_event_type: str,
    template_verb: str,
    horizon: str,
    state_id: Optional[str] = None,
    symbol: Optional[str] = None,
    include_symbol: bool = True,
    direction_bucket: Optional[str] = None,
) -> str:
    """Canonical BH-FDR grouping key.

    Primary ontology dimensions:
      (canonical_family, canonical_event_type, template_verb, horizon)
    Optional dimensions:
      state_id (when statistically stable), direction_bucket, symbol.
    """
    family = str(canonical_family or "").strip().upper()
    event_type = str(canonical_event_type or "").strip().upper()
    verb = str(template_verb or "").strip()
    h = str(horizon or "").strip()
    state = str(state_id or "").strip().upper()
    sym = str(symbol or "").strip().upper()
    direction = str(direction_bucket or "").strip().lower()

    parts = [family or "UNKNOWN_FAMILY", event_type or "UNKNOWN_EVENT", verb or "unknown_verb", h or "unknown_horizon"]
    if state:
        parts.append(f"STATE={state}")
    if direction:
        parts.append(f"DIR={direction}")
    if include_symbol and sym:
        parts.insert(0, sym)
    return "|".join(parts)

