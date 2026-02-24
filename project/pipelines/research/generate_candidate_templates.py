from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.spec_loader import load_global_defaults
from pipelines._lib.ontology_contract import (
    ONTOLOGY_SPEC_RELATIVE_PATHS,
    ontology_component_hashes,
    ontology_spec_hash,
    ontology_spec_paths,
    validate_state_registry_source_events,
    validate_candidate_templates_schema,
)
from events.registry import EVENT_REGISTRY_SPECS

def _load_global_defaults() -> Dict[str, Any]:
    return load_global_defaults(project_root=PROJECT_ROOT)

GLOBAL_DEFAULTS = _load_global_defaults()

# Default configuration for expansion
DEFAULT_HORIZONS = GLOBAL_DEFAULTS.get("horizons", ["5m", "15m", "60m"])
DEFAULT_RULE_TEMPLATES = GLOBAL_DEFAULTS.get("rule_templates", ["mean_reversion", "continuation", "trend_continuation", "pullback_entry"])
DEFAULT_CONDITIONING = GLOBAL_DEFAULTS.get("conditioning", {
    "vol_regime": ["high", "low"],
    "carry_state": ["pos", "neg", "neutral"],
    "severity_bucket": ["top_10pct", "extreme_5pct"],
    "funding_bps": ["extreme_pos", "extreme_neg"],
    "vpin": ["high_toxic"],
    "regime_vol_liquidity": ["high_vol_low_liq", "low_vol_high_liq"]
})

_ONTOLOGY_PATHS = ontology_spec_paths(PROJECT_ROOT.parent)
TAXONOMY_PATH = _ONTOLOGY_PATHS["taxonomy"]
CANONICAL_EVENT_REGISTRY_PATH = _ONTOLOGY_PATHS["canonical_event_registry"]
STATE_REGISTRY_PATH = _ONTOLOGY_PATHS["state_registry"]
VERB_LEXICON_PATH = _ONTOLOGY_PATHS["template_verb_lexicon"]


def _to_str_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    out: List[str] = []
    for item in value:
        token = str(item).strip()
        if token:
            out.append(token)
    return out


def _load_yaml(path: Path, *, strict: bool = False, required: bool = False, name: str = "spec") -> Dict[str, Any]:
    if not path.exists():
        if strict and required:
            raise FileNotFoundError(f"Missing required ontology {name}: {path}")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if isinstance(data, dict):
        return data
    if strict and required:
        raise ValueError(f"Invalid ontology {name}: expected mapping at {path}")
    return {}


def _load_taxonomy(*, strict: bool = False) -> Dict[str, Any]:
    return _load_yaml(TAXONOMY_PATH, strict=strict, required=True, name="taxonomy")


def _load_canonical_event_registry(*, strict: bool = False) -> Dict[str, Any]:
    return _load_yaml(
        CANONICAL_EVENT_REGISTRY_PATH,
        strict=strict,
        required=True,
        name="canonical_event_registry",
    )


def _load_state_registry(*, strict: bool = False) -> Dict[str, Any]:
    return _load_yaml(STATE_REGISTRY_PATH, strict=strict, required=True, name="state_registry")


def _load_template_verb_lexicon(*, strict: bool = False) -> Dict[str, Any]:
    return _load_yaml(VERB_LEXICON_PATH, strict=strict, required=True, name="template_verb_lexicon")


def _norm_label(value: Any) -> str:
    return str(value).strip().upper()


def _family_templates(family_cfg: Dict[str, Any]) -> List[str]:
    runtime = family_cfg.get("runtime_templates", [])
    if isinstance(runtime, list) and runtime:
        return [str(x) for x in runtime if str(x).strip()]
    templates = family_cfg.get("templates", DEFAULT_RULE_TEMPLATES)
    if isinstance(templates, list) and templates:
        return [str(x) for x in templates if str(x).strip()]
    return list(DEFAULT_RULE_TEMPLATES)


def _resolve_canonical_event_type(event_type: str, alias_maps: List[Dict[str, Any]]) -> str:
    event_type_norm = _norm_label(event_type)
    for aliases in alias_maps:
        if not isinstance(aliases, dict):
            continue
        mapped = aliases.get(str(event_type).strip())
        if mapped is not None:
            return _norm_label(mapped)
        for alias_key, canonical in aliases.items():
            if _norm_label(alias_key) == event_type_norm:
                return _norm_label(canonical)
    return event_type_norm


def _source_states_by_event(state_registry: Dict[str, Any]) -> Dict[str, List[str]]:
    by_event: Dict[str, List[str]] = {}
    states = state_registry.get("states", [])
    if not isinstance(states, list):
        return by_event
    for row in states:
        if not isinstance(row, dict):
            continue
        event = _norm_label(row.get("source_event_type", ""))
        state_id = _norm_label(row.get("state_id", ""))
        if not event or not state_id:
            continue
        by_event.setdefault(event, [])
        if state_id not in by_event[event]:
            by_event[event].append(state_id)
    return by_event


def _build_event_ontology_index(taxonomy: Dict[str, Any], state_registry: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    families = taxonomy.get("families", {})
    if not isinstance(families, dict):
        return {}

    source_states = _source_states_by_event(state_registry)
    index: Dict[str, Dict[str, Any]] = {}
    for family_name, family_cfg in families.items():
        if not isinstance(family_cfg, dict):
            continue
        family = _norm_label(family_name)
        family_states = [_norm_label(x) for x in family_cfg.get("states", []) if str(x).strip()]
        events = [_norm_label(x) for x in family_cfg.get("events", []) if str(x).strip()]
        for event in events:
            index[event] = {
                "canonical_family": family,
                "family_states": family_states,
                "source_states": list(source_states.get(event, [])),
            }
    return index


def _build_canonical_event_info(canonical_registry: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    families = canonical_registry.get("families", {})
    if not isinstance(families, dict):
        return out
    for family_name, family_cfg in families.items():
        if not isinstance(family_cfg, dict):
            continue
        family = _norm_label(family_name)
        family_tags = _to_str_list(family_cfg.get("tags", []))
        family_mechanism = str(family_cfg.get("mechanism", "")).strip() or None
        family_allowed_templates = _to_str_list(family_cfg.get("allowed_templates", []))
        family_default_horizons = _to_str_list(family_cfg.get("default_horizons", []))
        family_side_policy = str(family_cfg.get("side_policy", "")).strip() or None

        events_raw = family_cfg.get("events", [])
        if isinstance(events_raw, dict):
            events_iter = [{"event_type": key, **(value if isinstance(value, dict) else {})} for key, value in events_raw.items()]
        elif isinstance(events_raw, list):
            events_iter = events_raw
        else:
            events_iter = []

        for raw in events_iter:
            event_meta: Dict[str, Any]
            if isinstance(raw, str):
                event_id = _norm_label(raw)
                event_meta = {}
            elif isinstance(raw, dict):
                event_id = _norm_label(raw.get("event_type") or raw.get("event_id") or raw.get("id") or "")
                event_meta = raw
            else:
                continue
            if not event_id:
                continue
            event_tags = family_tags + _to_str_list(event_meta.get("tags", []))
            # Preserve order while de-duplicating.
            tags_unique = list(dict.fromkeys([t for t in event_tags if t]))
            out[event_id] = {
                "canonical_family": family,
                "mechanism": str(event_meta.get("mechanism", family_mechanism or "")).strip() or None,
                "tags": tags_unique,
                "allowed_templates": _to_str_list(event_meta.get("allowed_templates", family_allowed_templates)),
                "default_horizons": _to_str_list(event_meta.get("default_horizons", family_default_horizons)),
                "side_policy": str(event_meta.get("side_policy", family_side_policy or "")).strip() or None,
            }
    return out


def _verb_set_from_lexicon(verb_lexicon: Dict[str, Any]) -> set[str]:
    verbs = verb_lexicon.get("verbs", {})
    if not isinstance(verbs, dict):
        return set()
    out: set[str] = set()
    for group in verbs.values():
        if not isinstance(group, list):
            continue
        for v in group:
            token = str(v).strip()
            if token:
                out.add(token)
    return out


def _validate_and_filter_templates(
    templates: List[str],
    *,
    verb_set: set[str],
    strict: bool,
) -> tuple[List[str], List[str]]:
    unknown = [t for t in templates if verb_set and t not in verb_set]
    if unknown and strict:
        raise ValueError(f"Unknown template verbs found: {sorted(set(unknown))}")
    filtered = [t for t in templates if t not in unknown]
    return filtered, unknown


def _event_ontology_context(
    event_type: str,
    *,
    taxonomy: Dict[str, Any],
    canonical_registry: Dict[str, Any],
    event_index: Dict[str, Dict[str, Any]],
    canonical_event_info: Dict[str, Dict[str, Any]],
    verb_lexicon: Dict[str, Any],
    strict: bool,
) -> Dict[str, Any]:
    alias_maps: List[Dict[str, Any]] = []
    taxonomy_aliases = taxonomy.get("runtime_event_aliases", {})
    if isinstance(taxonomy_aliases, dict):
        alias_maps.append(taxonomy_aliases)
    canonical_aliases = canonical_registry.get("runtime_event_aliases", {})
    if isinstance(canonical_aliases, dict):
        alias_maps.append(canonical_aliases)

    canonical_event = _resolve_canonical_event_type(event_type, alias_maps)
    taxonomy_info = event_index.get(canonical_event, {})
    canonical_info = canonical_event_info.get(canonical_event, {})

    taxonomy_family = str(taxonomy_info.get("canonical_family", "")).strip()
    canonical_family = str(canonical_info.get("canonical_family", "")).strip()
    family_mismatch = bool(taxonomy_family and canonical_family and taxonomy_family != canonical_family)
    if family_mismatch and strict:
        raise ValueError(
            f"Canonical/taxonomy family mismatch for {canonical_event}: "
            f"canonical={canonical_family}, taxonomy={taxonomy_family}"
        )
    family = canonical_family or taxonomy_family

    family_states = [str(x) for x in taxonomy_info.get("family_states", [])]
    source_states = [str(x) for x in taxonomy_info.get("source_states", [])]
    all_states = sorted({*family_states, *source_states})

    templates = _get_templates_for_event(canonical_event, taxonomy)
    canonical_allowed_templates = _to_str_list(canonical_info.get("allowed_templates", []))
    if canonical_allowed_templates:
        templates = [t for t in templates if t in canonical_allowed_templates]

    verb_set = _verb_set_from_lexicon(verb_lexicon)
    filtered_templates, unknown_templates = _validate_and_filter_templates(
        templates,
        verb_set=verb_set,
        strict=strict,
    )

    horizons = _to_str_list(canonical_info.get("default_horizons", [])) or list(DEFAULT_HORIZONS)
    in_taxonomy = bool(taxonomy_family)
    in_canonical_registry = bool(canonical_family)
    if strict and not in_canonical_registry:
        raise ValueError(f"Event missing in canonical registry: {canonical_event}")
    fully_known = in_taxonomy and in_canonical_registry
    return {
        "canonical_event_type": canonical_event,
        "canonical_family": family,
        "in_taxonomy": in_taxonomy,
        "in_canonical_registry": in_canonical_registry,
        "fully_known": fully_known,
        "family_mismatch": family_mismatch,
        "mechanism": canonical_info.get("mechanism"),
        "tags": _to_str_list(canonical_info.get("tags", [])),
        "side_policy": canonical_info.get("side_policy"),
        "default_horizons": horizons,
        "canonical_allowed_templates": canonical_allowed_templates,
        "family_states": family_states,
        "source_states": source_states,
        "all_states": all_states,
        "rule_templates": filtered_templates,
        "unknown_templates": unknown_templates,
    }


def _flatten_list_column(values: Any) -> List[str]:
    out: List[str] = []
    if not isinstance(values, pd.Series):
        return out
    for item in values:
        if not isinstance(item, list):
            continue
        for token in item:
            s = str(token).strip()
            if s:
                out.append(s)
    return out


def _state_registry_rows(state_registry: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = state_registry.get("states", [])
    if isinstance(rows, list):
        return [r for r in rows if isinstance(r, dict)]
    return []


def _iter_family_events(families: Dict[str, Any]) -> List[tuple[str, Dict[str, Any]]]:
    out: List[tuple[str, Dict[str, Any]]] = []
    if not isinstance(families, dict):
        return out
    for _, family_cfg in families.items():
        if not isinstance(family_cfg, dict):
            continue
        events_raw = family_cfg.get("events", [])
        if isinstance(events_raw, dict):
            for ev_key, ev_meta in events_raw.items():
                ev = _norm_label(ev_key)
                if not ev:
                    continue
                out.append((ev, ev_meta if isinstance(ev_meta, dict) else {}))
        elif isinstance(events_raw, list):
            for item in events_raw:
                if isinstance(item, str):
                    ev = _norm_label(item)
                    if ev:
                        out.append((ev, {}))
                elif isinstance(item, dict):
                    ev = _norm_label(item.get("event_type") or item.get("event_id") or item.get("id") or "")
                    if ev:
                        out.append((ev, item))
    return out


def _collect_planned_events(
    taxonomy: Dict[str, Any],
    canonical_registry: Dict[str, Any],
) -> set[str]:
    planned: set[str] = set()
    for key in ("planned_events", "planned_event_types"):
        for source in (taxonomy, canonical_registry):
            value = source.get(key, []) if isinstance(source, dict) else []
            if isinstance(value, list):
                planned.update({_norm_label(x) for x in value if str(x).strip()})

    impl_by_event = taxonomy.get("implementation_status_by_event", {}) if isinstance(taxonomy, dict) else {}
    if isinstance(impl_by_event, dict):
        for ev, status in impl_by_event.items():
            if str(status).strip().lower() in {"planned", "roadmap", "future"}:
                planned.add(_norm_label(ev))

    for ev, meta in _iter_family_events(taxonomy.get("families", {})):
        status = str(meta.get("implementation_status", meta.get("status", ""))).strip().lower()
        if status in {"planned", "roadmap", "future"}:
            planned.add(ev)
    for ev, meta in _iter_family_events(canonical_registry.get("families", {})):
        status = str(meta.get("implementation_status", meta.get("status", ""))).strip().lower()
        if status in {"planned", "roadmap", "future"}:
            planned.add(ev)

    return {x for x in planned if x}


def _collect_declared_implemented_events(
    taxonomy: Dict[str, Any],
    canonical_registry: Dict[str, Any],
) -> set[str]:
    declared: set[str] = set()
    for key in ("implemented_events", "implemented_event_types"):
        for source in (taxonomy, canonical_registry):
            value = source.get(key, []) if isinstance(source, dict) else []
            if isinstance(value, list):
                declared.update({_norm_label(x) for x in value if str(x).strip()})

    impl_by_event = taxonomy.get("implementation_status_by_event", {}) if isinstance(taxonomy, dict) else {}
    if isinstance(impl_by_event, dict):
        for ev, status in impl_by_event.items():
            if str(status).strip().lower() in {"implemented", "active"}:
                declared.add(_norm_label(ev))
    impl_by_event = canonical_registry.get("implementation_status_by_event", {}) if isinstance(canonical_registry, dict) else {}
    if isinstance(impl_by_event, dict):
        for ev, status in impl_by_event.items():
            if str(status).strip().lower() in {"implemented", "active"}:
                declared.add(_norm_label(ev))

    for ev, meta in _iter_family_events(taxonomy.get("families", {})):
        status = str(meta.get("implementation_status", meta.get("status", ""))).strip().lower()
        implemented = meta.get("implemented")
        if status in {"implemented", "active"} or bool(implemented):
            declared.add(ev)
    for ev, meta in _iter_family_events(canonical_registry.get("families", {})):
        status = str(meta.get("implementation_status", meta.get("status", ""))).strip().lower()
        implemented = meta.get("implemented")
        if status in {"implemented", "active"} or bool(implemented):
            declared.add(ev)
    return {x for x in declared if x}


def _collect_family_event_types(doc: Dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for ev, _ in _iter_family_events(doc.get("families", {}) if isinstance(doc, dict) else {}):
        if ev:
            out.add(ev)
    return out


def _implemented_registry_event_types() -> set[str]:
    return {_norm_label(ev) for ev in EVENT_REGISTRY_SPECS.keys() if str(ev).strip()}


def _validate_implemented_event_contract(
    *,
    taxonomy: Dict[str, Any],
    canonical_registry: Dict[str, Any],
    allow_planned: bool,
) -> Dict[str, List[str]]:
    implemented = _implemented_registry_event_types()
    taxonomy_events = _collect_family_event_types(taxonomy)
    canonical_events = _collect_family_event_types(canonical_registry)
    ontology_events = taxonomy_events | canonical_events
    planned_events = _collect_planned_events(taxonomy, canonical_registry)
    declared_implemented_events = _collect_declared_implemented_events(taxonomy, canonical_registry)
    unlisted_default_planned = any(
        str(source.get("unlisted_event_status", source.get("default_unlisted_event_status", ""))).strip().lower() == "planned"
        for source in (taxonomy, canonical_registry)
        if isinstance(source, dict)
    )
    if unlisted_default_planned:
        planned_events.update({ev for ev in ontology_events if ev not in declared_implemented_events})
    planned_events = {ev for ev in planned_events if ev}

    taxonomy_unimplemented = sorted(
        ev for ev in taxonomy_events
        if ev not in implemented and (not allow_planned or ev not in planned_events)
    )
    canonical_unimplemented = sorted(
        ev for ev in canonical_events
        if ev not in implemented and (not allow_planned or ev not in planned_events)
    )
    planned_unimplemented = sorted(
        ev for ev in ontology_events
        if ev not in implemented and ev in planned_events
    )
    declared_implemented_missing = sorted(
        ev for ev in declared_implemented_events
        if ev not in implemented
    )
    return {
        "implemented_event_types": sorted(implemented),
        "taxonomy_events": sorted(taxonomy_events),
        "canonical_registry_events": sorted(canonical_events),
        "planned_events": sorted(planned_events),
        "declared_implemented_events": sorted(declared_implemented_events),
        "declared_implemented_missing_in_registry": declared_implemented_missing,
        "taxonomy_unimplemented_nonplanned": taxonomy_unimplemented,
        "canonical_unimplemented_nonplanned": canonical_unimplemented,
        "planned_unimplemented_events": planned_unimplemented,
    }


def _get_templates_for_event(event_type: str, taxonomy: Optional[Dict[str, Any]] = None) -> List[str]:
    """Determine rule templates for a given event type based on taxonomy."""
    if taxonomy is None:
        taxonomy = _load_taxonomy()

    event_type_norm = _norm_label(event_type)
    families = taxonomy.get("families", {})
    if not isinstance(families, dict):
        return list(DEFAULT_RULE_TEMPLATES)

    # New ontology path: runtime event aliases map runtime labels to canonical events.
    runtime_aliases = taxonomy.get("runtime_event_aliases", {})
    if isinstance(runtime_aliases, dict):
        mapped = runtime_aliases.get(str(event_type).strip())
        if mapped is not None:
            event_type_norm = _norm_label(mapped)
        else:
            for alias_key, canonical in runtime_aliases.items():
                if _norm_label(alias_key) == event_type_norm:
                    event_type_norm = _norm_label(canonical)
                    break

    # Legacy state windows resolve to state-specific templates or source-event family templates.
    legacy_states = taxonomy.get("legacy_state_aliases", {})
    if isinstance(legacy_states, dict):
        for alias_key, state_cfg in legacy_states.items():
            if _norm_label(alias_key) != event_type_norm:
                continue
            if isinstance(state_cfg, dict):
                allowed_templates = state_cfg.get("allowed_templates", [])
                if isinstance(allowed_templates, list) and allowed_templates:
                    return [str(x) for x in allowed_templates if str(x).strip()]
                source_event = state_cfg.get("source_event_type")
                if source_event:
                    event_type_norm = _norm_label(source_event)
            break

    for family_cfg in families.values():
        if not isinstance(family_cfg, dict):
            continue
        events = family_cfg.get("events", [])
        event_labels = {_norm_label(e) for e in events} if isinstance(events, list) else set()
        if event_type_norm in event_labels:
            return _family_templates(family_cfg)

    # Backward-compat for older taxonomy layouts that only listed lowercase event ids.
    legacy_norm = str(event_type).strip().lower()
    for family_cfg in families.values():
        if not isinstance(family_cfg, dict):
            continue
        events = [str(e).strip().lower() for e in family_cfg.get("events", [])] if isinstance(family_cfg.get("events", []), list) else []
        if legacy_norm in events:
            return _family_templates(family_cfg)

    return list(DEFAULT_RULE_TEMPLATES)

def _extract_event_type(statement: str) -> Optional[str]:
    """Heuristically extract event type from statement."""
    match = re.search(r'""event_type"": ""([A-Z0-9_]+)""', statement)
    if match:
        return match.group(1)
    matches = re.findall(r'\b[A-Z][A-Z0-9_]{5,}\b', statement)
    if matches:
        return matches[0]
    return None

def main() -> int:
    parser = argparse.ArgumentParser(description="Knowledge Atlas: Global Candidate Template Generator")
    parser.add_argument("--backlog", default="research_backlog.csv")
    parser.add_argument("--atlas_dir", default="atlas")
    parser.add_argument("--attribution_report", default=None, help="Path to regime performance attribution report (Parquet)")
    parser.add_argument(
        "--ontology_strict",
        type=int,
        default=(1 if str(os.getenv("CI", "")).strip() else 0),
        help="Fail closed on ontology spec/template mismatches (default: 1 in CI, else 0).",
    )
    parser.add_argument(
        "--implemented_only_events",
        type=int,
        default=1,
        help="If 1, only generate event templates for active registry-backed events.",
    )
    parser.add_argument(
        "--allow_planned_unimplemented",
        type=int,
        default=1,
        help="If 1, planned (roadmap) events are allowed in taxonomy/registry validation output without strict failure.",
    )
    args = parser.parse_args()

    atlas_dir = PROJECT_ROOT.parent / args.atlas_dir
    ensure_dir(atlas_dir)

    # Use a dummy run_id for manifest, as this is a global task
    manifest = start_manifest("generate_candidate_templates", "global_atlas", vars(args), [], [])

    try:
        backlog_path = PROJECT_ROOT.parent / args.backlog
        if not backlog_path.exists():
            raise FileNotFoundError(f"Backlog not found: {backlog_path}")
        
        df = pd.read_csv(backlog_path)
        strict_ontology = bool(int(args.ontology_strict))
        implemented_only_events = bool(int(args.implemented_only_events))
        allow_planned_unimplemented = bool(int(args.allow_planned_unimplemented))
        taxonomy = _load_taxonomy(strict=strict_ontology)
        canonical_registry = _load_canonical_event_registry(strict=strict_ontology)
        state_registry = _load_state_registry(strict=strict_ontology)
        verb_lexicon = _load_template_verb_lexicon(strict=strict_ontology)
        ontology_hash = ontology_spec_hash(PROJECT_ROOT.parent)
        component_hashes = ontology_component_hashes(PROJECT_ROOT.parent)
        event_ontology_index = _build_event_ontology_index(taxonomy, state_registry)
        canonical_event_info = _build_canonical_event_info(canonical_registry)
        canonical_events = set(canonical_event_info.keys())
        implemented_contract = _validate_implemented_event_contract(
            taxonomy=taxonomy,
            canonical_registry=canonical_registry,
            allow_planned=allow_planned_unimplemented,
        )
        implemented_events = set(implemented_contract["implemented_event_types"])
        nonplanned_unimplemented = sorted(
            set(implemented_contract["taxonomy_unimplemented_nonplanned"])
            | set(implemented_contract["canonical_unimplemented_nonplanned"])
        )
        declared_implemented_missing = list(implemented_contract["declared_implemented_missing_in_registry"])
        if strict_ontology and nonplanned_unimplemented:
            raise ValueError(
                "Unimplemented non-planned events in ontology specs: "
                + ", ".join(nonplanned_unimplemented)
            )
        if strict_ontology and declared_implemented_missing:
            raise ValueError(
                "Events declared implemented in ontology but missing active registry specs: "
                + ", ".join(declared_implemented_missing)
            )
        state_registry_issues = validate_state_registry_source_events(
            state_registry=state_registry,
            canonical_event_types=canonical_events,
        )
        if state_registry_issues and strict_ontology:
            raise ValueError(
                "state_registry source_event_type validation failed: "
                + "; ".join(state_registry_issues[:10])
            )
        
        # 1. Load Performance Attribution (if provided)
        attribution_map = {}
        if args.attribution_report:
            attr_path = Path(args.attribution_report)
            if attr_path.exists():
                attr_df = pd.read_parquet(attr_path)
                # Map concept_id -> score (e.g. sharpe_ratio)
                if "concept_id" in attr_df.columns and "sharpe_ratio" in attr_df.columns:
                    # If multiple rows per concept (different regimes), take the mean or max
                    attribution_map = attr_df.groupby("concept_id")["sharpe_ratio"].mean().to_dict()
        
        def _get_attribution_bonus(row):
            concept_id = str(row.get("concept_id"))
            return attribution_map.get(concept_id, 0.0)

        df["regime_attribution_score"] = df.apply(_get_attribution_bonus, axis=1)
        # Higher attribution bonus reduces priority_score (making it higher priority)
        # We clip bonus to avoid negative priority_scores
        df["adjusted_priority_score"] = (df["priority_score"] - df["regime_attribution_score"]).clip(lower=0.0)

        # 2. Deterministic Ordering
        # priority_score (lower is higher priority), tie-break with claim_id
        df = df.sort_values(by=["adjusted_priority_score", "claim_id"]).reset_index(drop=True)
        
        # 2. Filter for operationalizable and unverified
        active_claims = df[(df['operationalizable'] == 'Y') & (df['status'] != 'verified')].copy()
        
        candidate_templates = []
        spec_tasks = []
        skipped_event_claims: List[Dict[str, str]] = []
        planned_candidates: List[Dict[str, Any]] = []
        
        print("Iterating over active claims...")
        for i, row in active_claims.iterrows():
            try:
                claim_id = str(row['claim_id'])
                c_type = str(row['candidate_type']).lower() # event or feature
                statement = str(row['statement_summary'])
                target_pattern = str(row['next_artifact'])
                # print(f"Processing claim: {claim_id}, type: {c_type}")
                
                if c_type == 'event':
                    raw_event_type = _extract_event_type(statement)
                    if not raw_event_type:
                        continue
                    ontology_ctx = _event_ontology_context(
                        raw_event_type,
                        taxonomy=taxonomy,
                        canonical_registry=canonical_registry,
                        event_index=event_ontology_index,
                        canonical_event_info=canonical_event_info,
                        verb_lexicon=verb_lexicon,
                        strict=strict_ontology,
                    )
                    event_type = str(ontology_ctx["canonical_event_type"])
                    if implemented_only_events and event_type not in implemented_events:
                        reason = (
                            "planned_unimplemented_event"
                            if event_type in set(implemented_contract["planned_events"])
                            else "unimplemented_event"
                        )
                        if strict_ontology and reason == "unimplemented_event":
                            raise ValueError(
                                f"Claim {claim_id} resolved to non-implemented event {event_type} "
                                f"outside active registry contract."
                            )
                        skipped_event_claims.append(
                            {
                                "claim_id": claim_id,
                                "event_type": event_type,
                                "reason": reason,
                            }
                        )
                        if reason == "planned_unimplemented_event":
                            planned_candidates.append(
                                {
                                    "claim_id": claim_id,
                                    "concept_id": row.get("concept_id"),
                                    "event_type": event_type,
                                    "runtime_event_type": raw_event_type,
                                    "canonical_family": str(ontology_ctx.get("canonical_family", "")),
                                    "statement": statement,
                                    "priority_score": row.get("priority_score"),
                                    "assets_filter": row.get("assets"),
                                }
                            )
                        continue
                    
                    target_path = target_pattern.replace("{event_type}", event_type)
                    spec_exists = (PROJECT_ROOT.parent / target_path).exists()
                    
                    if not spec_exists:
                        spec_tasks.append({
                            "claim_id": claim_id,
                            "concept_id": row['concept_id'],
                            "object_type": "event",
                            "target_path": target_path,
                            "priority_score": row['priority_score'],
                            "statement": statement,
                            "assets_filter": row['assets']
                        })
                    
                    # Add template regardless of spec existence (Stage 2 will filter)
                    candidate_templates.append({
                        "template_id": f"{claim_id}@{event_type}",
                        "source_claim_id": claim_id,
                        "concept_id": row['concept_id'],
                        "object_type": "event",
                        "runtime_event_type": raw_event_type,
                        "event_type": event_type,
                        "canonical_event_type": event_type,
                        "canonical_family": str(ontology_ctx["canonical_family"]),
                        "ontology_in_taxonomy": bool(ontology_ctx["in_taxonomy"]),
                        "ontology_in_canonical_registry": bool(ontology_ctx["in_canonical_registry"]),
                        "ontology_fully_known": bool(ontology_ctx["fully_known"]),
                        "ontology_event_known": bool(ontology_ctx["fully_known"]),
                        "ontology_family_mismatch": bool(ontology_ctx["family_mismatch"]),
                        "ontology_event_mechanism": ontology_ctx.get("mechanism"),
                        "ontology_event_tags": list(ontology_ctx.get("tags", [])),
                        "ontology_side_policy": ontology_ctx.get("side_policy"),
                        "ontology_source_states": list(ontology_ctx["source_states"]),
                        "ontology_family_states": list(ontology_ctx["family_states"]),
                        "ontology_all_states": list(ontology_ctx["all_states"]),
                        "ontology_unknown_templates": list(ontology_ctx["unknown_templates"]),
                        "ontology_allowed_templates": list(ontology_ctx.get("canonical_allowed_templates", [])),
                        "ontology_spec_hash": ontology_hash,
                        "target_spec_path": target_path,
                        "rule_templates": list(ontology_ctx["rule_templates"]),
                        "horizons": list(ontology_ctx["default_horizons"]),
                        "conditioning": DEFAULT_CONDITIONING,
                        "assets_filter": row['assets'],
                        "min_events": 50,
                        "label_type": "returns",
                        "regime_attribution_score": row['regime_attribution_score']
                    })

                elif c_type == 'feature':
                    # Features like ROLL/VPIN
                    features_val = str(row.get('features', ''))
                    if not features_val or features_val == 'nan':
                        continue
                    features_list = features_val.split('|')
                    for feat in features_list:
                        target_path = target_pattern.replace("{feature_name}", feat)
                        spec_exists = (PROJECT_ROOT.parent / target_path).exists()
                        
                        if not spec_exists:
                            spec_tasks.append({
                                "claim_id": claim_id,
                                "concept_id": row['concept_id'],
                                "object_type": "feature",
                                "target_path": target_path,
                                "priority_score": row['priority_score'],
                                "statement": statement,
                                "assets_filter": row['assets']
                            })
                        
                        candidate_templates.append({
                            "template_id": f"{claim_id}@{feat}",
                            "source_claim_id": claim_id,
                            "concept_id": row['concept_id'],
                            "object_type": "feature",
                            "runtime_event_type": None,
                            "event_type": None,
                            "feature_name": feat,
                            "canonical_event_type": None,
                            "canonical_family": None,
                            "ontology_in_taxonomy": False,
                            "ontology_fully_known": False,
                            "ontology_event_known": False,
                            "ontology_in_canonical_registry": False,
                            "ontology_family_mismatch": False,
                            "ontology_event_mechanism": None,
                            "ontology_event_tags": [],
                            "ontology_side_policy": None,
                            "ontology_source_states": [],
                            "ontology_family_states": [],
                            "ontology_all_states": [],
                            "ontology_unknown_templates": [],
                            "ontology_allowed_templates": [],
                            "ontology_spec_hash": ontology_hash,
                            "target_spec_path": target_path,
                            "rule_templates": ["feature_conditioned_prediction"],
                            "horizons": DEFAULT_HORIZONS,
                            "conditioning": {"vol_regime": ["high"]},
                            "assets_filter": row['assets'],
                            "min_events": 50,
                            "label_type": "RV" if "vol" in statement.lower() or "rv" in statement.lower() else "returns",
                            "regime_attribution_score": row['regime_attribution_score']
                        })
            except Exception as e:
                if strict_ontology:
                    raise
                print(f"Error processing claim {row.get('claim_id')}: {e}")
                continue

        # Save Global Artifacts
        templates_df = pd.DataFrame(candidate_templates)
        if not templates_df.empty:
            validate_candidate_templates_schema(templates_df)
        templates_df.to_parquet(atlas_dir / "candidate_templates.parquet", index=False)
        
        tasks_df = pd.DataFrame(spec_tasks)
        tasks_df.to_parquet(atlas_dir / "spec_tasks.parquet", index=False)
        planned_df = pd.DataFrame(planned_candidates)
        planned_df.to_parquet(atlas_dir / "planned_candidates.parquet", index=False)

        event_rows = templates_df[templates_df.get("object_type") == "event"] if not templates_df.empty else pd.DataFrame()
        event_types_generated = sorted(
            {
                str(x).strip()
                for x in event_rows.get("canonical_event_type", pd.Series(dtype=object)).tolist()
                if str(x).strip()
            }
        )
        events_in_taxonomy = sorted(
            {
                str(ev).strip()
                for _, ev in event_rows.loc[
                    event_rows.get("ontology_in_taxonomy", False) == True, "canonical_event_type"
                ].items()
                if str(ev).strip()
            }
        ) if not event_rows.empty and "ontology_in_taxonomy" in event_rows.columns else []
        events_in_canonical_registry = sorted(
            {
                str(ev).strip()
                for _, ev in event_rows.loc[
                    event_rows.get("ontology_in_canonical_registry", False) == True, "canonical_event_type"
                ].items()
                if str(ev).strip()
            }
        ) if not event_rows.empty and "ontology_in_canonical_registry" in event_rows.columns else []
        events_missing_in_canonical_registry = sorted(
            {
                str(ev).strip()
                for _, ev in event_rows.loc[
                    event_rows.get("ontology_in_canonical_registry", False) == False, "canonical_event_type"
                ].items()
                if str(ev).strip()
            }
        ) if not event_rows.empty and "ontology_in_canonical_registry" in event_rows.columns else []

        unknown_templates_list = sorted(
            set(_flatten_list_column(event_rows.get("ontology_unknown_templates", pd.Series(dtype=object))))
        )

        state_rows = _state_registry_rows(state_registry)
        states_total = len(state_rows)
        states_linked_total = 0
        states_with_missing_source_event: List[str] = []
        for state in state_rows:
            state_id = _norm_label(state.get("state_id", ""))
            source_event = _norm_label(state.get("source_event_type", ""))
            if source_event and source_event in canonical_events:
                states_linked_total += 1
            elif state_id and source_event:
                states_with_missing_source_event.append(state_id)
        states_with_missing_source_event = sorted(set(states_with_missing_source_event))

        taxonomy_version = taxonomy.get("schema_version") or taxonomy.get("version")
        canonical_version = canonical_registry.get("version")
        state_registry_version = state_registry.get("version")
        verb_lexicon_version = verb_lexicon.get("version")

        ontology_linkage = {
            "ontology_spec_hash": ontology_hash,
            "component_hashes": component_hashes,
            "spec_paths": {
                "taxonomy": ONTOLOGY_SPEC_RELATIVE_PATHS["taxonomy"],
                "canonical_event_registry": ONTOLOGY_SPEC_RELATIVE_PATHS["canonical_event_registry"],
                "state_registry": ONTOLOGY_SPEC_RELATIVE_PATHS["state_registry"],
                "template_verb_lexicon": ONTOLOGY_SPEC_RELATIVE_PATHS["template_verb_lexicon"],
            },
            "spec_versions": {
                "taxonomy": taxonomy_version,
                "canonical_event_registry": canonical_version,
                "state_registry": state_registry_version,
                "template_verb_lexicon": verb_lexicon_version,
            },
            "counts": {
                "events_total": int(len(event_types_generated)),
                "events_in_taxonomy": int(len(events_in_taxonomy)),
                "events_in_canonical_registry": int(len(events_in_canonical_registry)),
                "events_in_implemented_contract": int(len(sorted(set(event_types_generated) & set(implemented_contract["implemented_event_types"])))),
                "implemented_event_types_total": int(len(implemented_contract["implemented_event_types"])),
                "templates_total": int(len(templates_df)),
                "event_templates_total": int((templates_df.get("object_type") == "event").sum()) if not templates_df.empty else 0,
                "feature_templates_total": int((templates_df.get("object_type") == "feature").sum()) if not templates_df.empty else 0,
                "unknown_templates_total": int(len(_flatten_list_column(event_rows.get("ontology_unknown_templates", pd.Series(dtype=object))))),
                "states_total": int(states_total),
                "states_linked_total": int(states_linked_total),
                "event_templates_unknown_ontology": int(
                    templates_df[
                        (templates_df.get("object_type") == "event")
                        & (~templates_df.get("ontology_fully_known", False))
                    ].shape[0]
                ) if not templates_df.empty and "ontology_fully_known" in templates_df.columns else 0,
                "event_claims_skipped_unimplemented": int(len(skipped_event_claims)),
                "planned_candidates_total": int(len(planned_candidates)),
            },
            "unresolved": {
                "events_missing_in_canonical_registry": events_missing_in_canonical_registry,
                "templates_missing_in_verb_lexicon": unknown_templates_list,
                "states_with_missing_source_event": states_with_missing_source_event,
                "state_registry_validation_issues": state_registry_issues,
                "taxonomy_unimplemented_nonplanned": implemented_contract["taxonomy_unimplemented_nonplanned"],
                "canonical_unimplemented_nonplanned": implemented_contract["canonical_unimplemented_nonplanned"],
                "declared_implemented_missing_in_registry": implemented_contract["declared_implemented_missing_in_registry"],
                "planned_unimplemented_events": implemented_contract["planned_unimplemented_events"],
                "skipped_event_claims": skipped_event_claims,
            },
            "implementation_contract": {
                "implemented_only_events": implemented_only_events,
                "allow_planned_unimplemented": allow_planned_unimplemented,
                "implemented_event_types": implemented_contract["implemented_event_types"],
                "declared_implemented_events": implemented_contract["declared_implemented_events"],
                "planned_events": implemented_contract["planned_events"],
            },
        }
        with (atlas_dir / "ontology_linkage.json").open("w", encoding="utf-8") as f:
            json.dump(ontology_linkage, f, indent=2, sort_keys=True)
        
        # Human-readable index
        with (atlas_dir / "template_index.md").open("w", encoding="utf-8") as f:
            f.write("# Knowledge Atlas: Candidate Templates\n\n")
            f.write(f"Generated from `{args.backlog}`\n\n")
            if not templates_df.empty:
                table_cols = [
                    "template_id",
                    "object_type",
                    "event_type",
                    "canonical_family",
                    "regime_attribution_score",
                    "assets_filter",
                ]
                f.write(templates_df.reindex(columns=table_cols).to_markdown(index=False))
            else:
                f.write("No active templates found.\n")

        finalize_manifest(manifest, "success", stats={
            "templates_count": len(candidate_templates),
            "spec_tasks_count": len(spec_tasks),
            "planned_candidates_count": len(planned_candidates),
            "ontology_strict": int(strict_ontology),
            "implemented_only_events": int(implemented_only_events),
            "ontology_spec_hash": ontology_hash,
            "ontology_event_count": int(len(event_ontology_index)),
            "ontology_canonical_registry_event_count": int(len(canonical_events)),
            "implemented_event_types_total": int(len(implemented_contract["implemented_event_types"])),
            "event_claims_skipped_unimplemented": int(len(skipped_event_claims)),
        })
        return 0

    except Exception as exc:
        import traceback
        traceback.print_exc()
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1

if __name__ == "__main__":
    sys.exit(main())
