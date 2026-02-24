from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PROJECT_ROOT.parent
sys.path.insert(0, str(PROJECT_ROOT))

from events.registry import EVENT_REGISTRY_SPECS
from pipelines.run_all import PHASE2_EVENT_CHAIN
from pipelines._lib.ontology_contract import (
    materialized_state_ids,
    normalize_state_registry_records,
    ontology_spec_paths,
    state_id_to_context_column,
)


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _norm(value: Any) -> str:
    return str(value or "").strip().upper()


def _iter_family_events(doc: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    families = doc.get("families", {})
    if not isinstance(families, dict):
        return out
    for family_cfg in families.values():
        if not isinstance(family_cfg, dict):
            continue
        events_raw = family_cfg.get("events", [])
        if isinstance(events_raw, list):
            for row in events_raw:
                if isinstance(row, str):
                    ev = _norm(row)
                    if ev:
                        out.append(ev)
                elif isinstance(row, dict):
                    ev = _norm(row.get("event_type") or row.get("event_id") or row.get("id"))
                    if ev:
                        out.append(ev)
        elif isinstance(events_raw, dict):
            for key in events_raw.keys():
                ev = _norm(key)
                if ev:
                    out.append(ev)
    return out


def _collect_events_from_list(doc: Dict[str, Any], key: str) -> set[str]:
    out: set[str] = set()
    values = doc.get(key, [])
    if isinstance(values, list):
        out = {_norm(x) for x in values if _norm(x)}
    return out


def _collect_declared_implemented(doc: Dict[str, Any]) -> set[str]:
    out = _collect_events_from_list(doc, "implemented_events") | _collect_events_from_list(doc, "implemented_event_types")
    by_event = doc.get("implementation_status_by_event", {})
    if isinstance(by_event, dict):
        for ev, status in by_event.items():
            if str(status).strip().lower() in {"implemented", "active"}:
                out.add(_norm(ev))

    families = doc.get("families", {})
    if isinstance(families, dict):
        for family_cfg in families.values():
            if not isinstance(family_cfg, dict):
                continue
            events_raw = family_cfg.get("events", [])
            if not isinstance(events_raw, list):
                continue
            for row in events_raw:
                if not isinstance(row, dict):
                    continue
                ev = _norm(row.get("event_type") or row.get("event_id") or row.get("id"))
                if not ev:
                    continue
                status = str(row.get("implementation_status", row.get("status", ""))).strip().lower()
                if status in {"implemented", "active"} or bool(row.get("implemented")):
                    out.add(ev)
    return {ev for ev in out if ev}


def _collect_planned(doc: Dict[str, Any]) -> set[str]:
    out = _collect_events_from_list(doc, "planned_events") | _collect_events_from_list(doc, "planned_event_types")
    by_event = doc.get("implementation_status_by_event", {})
    if isinstance(by_event, dict):
        for ev, status in by_event.items():
            if str(status).strip().lower() in {"planned", "roadmap", "future"}:
                out.add(_norm(ev))

    families = doc.get("families", {})
    if isinstance(families, dict):
        for family_cfg in families.values():
            if not isinstance(family_cfg, dict):
                continue
            events_raw = family_cfg.get("events", [])
            if not isinstance(events_raw, list):
                continue
            for row in events_raw:
                if not isinstance(row, dict):
                    continue
                ev = _norm(row.get("event_type") or row.get("event_id") or row.get("id"))
                if not ev:
                    continue
                status = str(row.get("implementation_status", row.get("status", ""))).strip().lower()
                if status in {"planned", "roadmap", "future"}:
                    out.add(ev)
    return {ev for ev in out if ev}


def _active_event_yaml_specs(spec_dir: Path) -> Dict[str, Dict[str, Any]]:
    specs: Dict[str, Dict[str, Any]] = {}
    for path in sorted(spec_dir.glob("*.yaml")):
        if path.name == "canonical_event_registry.yaml":
            continue
        doc = _load_yaml(path)
        if not doc:
            continue
        if bool(doc.get("deprecated", False)) or not bool(doc.get("active", True)):
            continue
        ev = _norm(doc.get("event_type"))
        if ev:
            specs[ev] = doc
    return specs


def _script_declares_event_type(path: Path) -> bool:
    if not path.exists():
        return False
    text = path.read_text(encoding="utf-8")
    patterns = [
        r"\[\s*['\"]event_type['\"]\s*\]\s*=",
        r"['\"]event_type['\"]\s*:",
        r"\.assign\(\s*event_type\s*=",
    ]
    return any(re.search(p, text) for p in patterns)


def run_audit(repo_root: Path) -> Dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    project_root = repo_root / "project"
    spec_paths = ontology_spec_paths(repo_root)
    spec_events_dir = repo_root / "spec" / "events"
    research_root = project_root / "pipelines" / "research"

    taxonomy = _load_yaml(spec_paths["taxonomy"])
    canonical = _load_yaml(spec_paths["canonical_event_registry"])
    state_registry = _load_yaml(spec_paths["state_registry"])

    registry_backed = sorted({_norm(ev) for ev in EVENT_REGISTRY_SPECS.keys() if _norm(ev)})
    active_spec_events = sorted(_active_event_yaml_specs(spec_events_dir).keys())
    active_specs_without_registry = sorted(set(active_spec_events) - set(registry_backed))

    chain_events = sorted({_norm(ev) for ev, _, _ in PHASE2_EVENT_CHAIN if _norm(ev)})
    chain_map: Dict[str, str] = {}
    for ev, script, _ in PHASE2_EVENT_CHAIN:
        event = _norm(ev)
        if event:
            chain_map[event] = str(script).strip()

    missing_phase2_chain_entries = sorted(set(registry_backed) - set(chain_events))
    chain_entries_with_missing_specs = sorted(set(chain_events) - set(registry_backed))

    missing_analyzer_per_event: Dict[str, str] = {}
    for ev in registry_backed:
        script_name = chain_map.get(ev, "")
        if not script_name:
            missing_analyzer_per_event[ev] = "no_phase2_chain_entry"
            continue
        script_path = research_root / script_name
        if not script_path.exists():
            missing_analyzer_per_event[ev] = f"missing_script:{script_name}"

    scripts_to_events: Dict[str, List[str]] = {}
    for ev, script_name in chain_map.items():
        scripts_to_events.setdefault(script_name, []).append(ev)
    multi_type_analyzers_missing_event_type: Dict[str, List[str]] = {}
    for script_name, events in sorted(scripts_to_events.items()):
        if len(events) <= 1:
            continue
        script_path = research_root / script_name
        if not _script_declares_event_type(script_path):
            multi_type_analyzers_missing_event_type[script_name] = sorted(events)

    taxonomy_events = sorted(set(_iter_family_events(taxonomy)))
    canonical_events = sorted(set(_iter_family_events(canonical)))
    ontology_events = sorted(set(taxonomy_events) | set(canonical_events))

    taxonomy_declared_implemented = _collect_declared_implemented(taxonomy)
    canonical_declared_implemented = _collect_declared_implemented(canonical)
    declared_implemented = sorted(taxonomy_declared_implemented | canonical_declared_implemented)
    declared_implemented_missing_in_registry = sorted(set(declared_implemented) - set(registry_backed))

    planned_events = _collect_planned(taxonomy) | _collect_planned(canonical)
    if (
        str(taxonomy.get("unlisted_event_status", "")).strip().lower() == "planned"
        or str(canonical.get("unlisted_event_status", "")).strip().lower() == "planned"
    ):
        planned_events = planned_events | (set(ontology_events) - set(declared_implemented))
    planned_events_sorted = sorted(planned_events)

    taxonomy_not_implemented = sorted(set(taxonomy_events) - set(registry_backed))
    canonical_not_implemented = sorted(set(canonical_events) - set(registry_backed))
    planned_backlog = sorted(
        ev for ev in set(taxonomy_not_implemented) | set(canonical_not_implemented)
        if ev in planned_events
    )

    states = normalize_state_registry_records(state_registry)
    materialized_ids = set(materialized_state_ids())
    state_registry_not_materialized = sorted(
        state["state_id"] for state in states
        if state["state_id"] not in materialized_ids
    )
    states_with_missing_source_event = sorted(
        state["state_id"] for state in states
        if state["source_event_type"] not in set(canonical_events)
    )
    materialized_state_columns = {
        state_id: state_id_to_context_column(state_id)
        for state_id in sorted(materialized_ids)
    }

    failures: List[str] = []
    if missing_phase2_chain_entries:
        failures.append(f"missing_phase2_chain_entries={','.join(missing_phase2_chain_entries)}")
    if chain_entries_with_missing_specs:
        failures.append(f"chain_entries_with_missing_specs={','.join(chain_entries_with_missing_specs)}")
    if missing_analyzer_per_event:
        failures.append("missing_analyzer_per_event=" + ",".join(sorted(missing_analyzer_per_event.keys())))
    if multi_type_analyzers_missing_event_type:
        failures.append(
            "multi_type_analyzers_missing_event_type="
            + ",".join(sorted(multi_type_analyzers_missing_event_type.keys()))
        )
    if active_specs_without_registry:
        failures.append("active_specs_without_registry=" + ",".join(active_specs_without_registry))
    if declared_implemented_missing_in_registry:
        failures.append(
            "declared_implemented_missing_in_registry=" + ",".join(declared_implemented_missing_in_registry)
        )

    return {
        "counts": {
            "implemented_events_total": len(registry_backed),
            "active_event_specs_total": len(active_spec_events),
            "phase2_chain_events_total": len(chain_events),
            "taxonomy_events_total": len(taxonomy_events),
            "canonical_events_total": len(canonical_events),
            "planned_backlog_total": len(planned_backlog),
            "state_registry_total": len(states),
            "state_registry_materialized_total": len(materialized_ids),
            "state_registry_not_materialized_total": len(state_registry_not_materialized),
        },
        "implemented_contract": {
            "implemented_events": registry_backed,
            "active_event_specs": active_spec_events,
            "active_specs_without_registry": active_specs_without_registry,
            "missing_phase2_chain_entries": missing_phase2_chain_entries,
            "missing_analyzer_per_event": missing_analyzer_per_event,
            "chain_entries_with_missing_specs": chain_entries_with_missing_specs,
            "multi_type_analyzers_missing_event_type": multi_type_analyzers_missing_event_type,
        },
        "ontology_backlog": {
            "declared_implemented_events": declared_implemented,
            "declared_implemented_missing_in_registry": declared_implemented_missing_in_registry,
            "taxonomy_not_implemented": taxonomy_not_implemented,
            "canonical_not_implemented": canonical_not_implemented,
            "planned_events": planned_events_sorted,
            "planned_backlog": planned_backlog,
        },
        "states": {
            "materialized_state_ids": sorted(materialized_ids),
            "materialized_state_columns": materialized_state_columns,
            "state_registry_not_materialized": state_registry_not_materialized,
            "states_with_missing_source_event": states_with_missing_source_event,
        },
        "failures": failures,
    }


def _print_text(report: Dict[str, Any]) -> None:
    counts = report.get("counts", {})
    implemented = report.get("implemented_contract", {})
    backlog = report.get("ontology_backlog", {})
    states = report.get("states", {})
    failures = report.get("failures", [])

    print("Ontology Consistency Audit")
    print("==========================")
    print(
        "Implemented events: {implemented}/{active} active specs, chain={chain}".format(
            implemented=counts.get("implemented_events_total", 0),
            active=counts.get("active_event_specs_total", 0),
            chain=counts.get("phase2_chain_events_total", 0),
        )
    )
    print(f"Planned backlog events: {counts.get('planned_backlog_total', 0)}")
    print(
        "State registry: total={total} materialized={mat} not_materialized={missing}".format(
            total=counts.get("state_registry_total", 0),
            mat=counts.get("state_registry_materialized_total", 0),
            missing=counts.get("state_registry_not_materialized_total", 0),
        )
    )
    print("")
    print("Missing phase2 chain entries:", implemented.get("missing_phase2_chain_entries", []))
    print("Chain entries with missing specs:", implemented.get("chain_entries_with_missing_specs", []))
    print("Missing analyzer per event:", implemented.get("missing_analyzer_per_event", {}))
    print(
        "Multi-type analyzers missing event_type:",
        implemented.get("multi_type_analyzers_missing_event_type", {}),
    )
    print(
        "Declared implemented missing in registry:",
        backlog.get("declared_implemented_missing_in_registry", []),
    )
    print("State registry not materialized:", states.get("state_registry_not_materialized", []))
    if failures:
        print("")
        print("FAILURES:")
        for item in failures:
            print(f"- {item}")
    else:
        print("")
        print("No fail-closed contract issues detected.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit ontology consistency against implemented event contract.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--output", default="")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    parser.add_argument("--fail-on-missing", action="store_true")
    args = parser.parse_args()

    report = run_audit(Path(args.repo_root))
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

    if args.format == "json":
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        _print_text(report)

    if args.fail_on_missing and report.get("failures"):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
