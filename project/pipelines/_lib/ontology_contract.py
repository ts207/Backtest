from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd


ONTOLOGY_SPEC_RELATIVE_PATHS: Dict[str, str] = {
    "taxonomy": "spec/multiplicity/taxonomy.yaml",
    "canonical_event_registry": "spec/events/canonical_event_registry.yaml",
    "state_registry": "spec/states/state_registry.yaml",
    "template_verb_lexicon": "spec/hypotheses/template_verb_lexicon.yaml",
}


def ontology_spec_paths(repo_root: Path) -> Dict[str, Path]:
    return {
        key: repo_root / rel_path
        for key, rel_path in ONTOLOGY_SPEC_RELATIVE_PATHS.items()
    }


def _sha256_bytes(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def ontology_component_hashes(repo_root: Path) -> Dict[str, Optional[str]]:
    out: Dict[str, Optional[str]] = {}
    for key, path in ontology_spec_paths(repo_root).items():
        if not path.exists():
            out[key] = None
            continue
        out[key] = _sha256_bytes(path.read_bytes())
    return out


def ontology_spec_hash(repo_root: Path) -> str:
    hasher = hashlib.sha256()
    paths = ontology_spec_paths(repo_root)
    for key in sorted(paths):
        rel_path = ONTOLOGY_SPEC_RELATIVE_PATHS[key]
        path = paths[key]
        hasher.update(key.encode("utf-8"))
        hasher.update(rel_path.encode("utf-8"))
        if path.exists():
            hasher.update(path.read_bytes())
        else:
            hasher.update(b"")
    return "sha256:" + hasher.hexdigest()


def load_ontology_linkage_hash(atlas_dir: Path) -> Optional[str]:
    path = atlas_dir / "ontology_linkage.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    value = str(payload.get("ontology_spec_hash", "")).strip()
    return value or None


def load_run_manifest_hashes(data_root: Path, run_id: str) -> Dict[str, Optional[str]]:
    path = data_root / "runs" / run_id / "run_manifest.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return {
        "ontology_spec_hash": str(payload.get("ontology_spec_hash", "")).strip() or None,
        "taxonomy_hash": str(payload.get("taxonomy_hash", "")).strip() or None,
        "canonical_event_registry_hash": str(payload.get("canonical_event_registry_hash", "")).strip() or None,
        "state_registry_hash": str(payload.get("state_registry_hash", "")).strip() or None,
        "verb_lexicon_hash": str(payload.get("verb_lexicon_hash", "")).strip() or None,
    }


def _is_list_like(value: Any) -> bool:
    return isinstance(value, (list, tuple, np.ndarray))


def _as_str_list(value: Any) -> List[str]:
    if value is None:
        return []
    if _is_list_like(value):
        return [str(v).strip() for v in value if str(v).strip()]
    token = str(value).strip()
    if not token:
        return []
    # Some parquet writers may serialize object lists as JSON strings.
    if token.startswith("[") and token.endswith("]"):
        try:
            parsed = json.loads(token)
        except Exception:
            return [token]
        if _is_list_like(parsed):
            return [str(v).strip() for v in parsed if str(v).strip()]
    return [token]


def validate_candidate_templates_schema(df: pd.DataFrame) -> None:
    required_common = [
        "template_id",
        "object_type",
        "rule_templates",
        "horizons",
        "conditioning",
        "ontology_spec_hash",
    ]
    required_event = [
        "template_id",
        "object_type",
        "runtime_event_type",
        "canonical_event_type",
        "canonical_family",
        "rule_templates",
        "horizons",
        "conditioning",
        "ontology_in_taxonomy",
        "ontology_in_canonical_registry",
        "ontology_unknown_templates",
        "ontology_spec_hash",
        "ontology_source_states",
        "ontology_family_states",
        "ontology_all_states",
    ]
    required_feature = [
        "template_id",
        "object_type",
        "feature_name",
        "rule_templates",
        "horizons",
        "conditioning",
        "ontology_spec_hash",
    ]

    missing_common = [col for col in required_common if col not in df.columns]
    if missing_common:
        raise ValueError(f"candidate_templates schema missing common columns: {missing_common}")

    event_mask = df.get("object_type", pd.Series(dtype=object)).astype(str).str.lower() == "event"
    feature_mask = df.get("object_type", pd.Series(dtype=object)).astype(str).str.lower() == "feature"
    event_rows = df[event_mask].copy()
    feature_rows = df[feature_mask].copy()

    if not event_rows.empty:
        missing_event = [col for col in required_event if col not in event_rows.columns]
        if missing_event:
            raise ValueError(f"candidate_templates event rows missing required columns: {missing_event}")
    if not feature_rows.empty:
        missing_feature = [col for col in required_feature if col not in feature_rows.columns]
        if missing_feature:
            raise ValueError(f"candidate_templates feature rows missing required columns: {missing_feature}")

    list_columns = [
        "rule_templates",
        "horizons",
        "ontology_unknown_templates",
        "ontology_source_states",
        "ontology_family_states",
        "ontology_all_states",
    ]
    for col in list_columns:
        if col not in df.columns:
            continue
        for idx, value in df[col].items():
            if value is None:
                raise ValueError(f"candidate_templates.{col} has null at row {idx}")
            if isinstance(value, float) and pd.isna(value):
                raise ValueError(f"candidate_templates.{col} has null at row {idx}")
            if _is_list_like(value):
                continue
            # Accept JSON-encoded list strings if they decode.
            parsed = _as_str_list(value)
            if not (str(value).strip().startswith("[") and parsed):
                raise ValueError(f"candidate_templates.{col} must be list-like at row {idx}; got {type(value).__name__}")

    if "conditioning" in df.columns:
        for idx, value in df["conditioning"].items():
            if not isinstance(value, dict):
                raise ValueError(f"candidate_templates.conditioning must be dict at row {idx}; got {type(value).__name__}")

    if "ontology_spec_hash" in df.columns:
        hashes = sorted(
            {
                str(v).strip()
                for v in df["ontology_spec_hash"].tolist()
                if str(v).strip()
            }
        )
        if not hashes:
            raise ValueError("candidate_templates.ontology_spec_hash is empty")
        if len(hashes) > 1:
            raise ValueError(f"candidate_templates has multiple ontology_spec_hash values: {hashes}")


def parse_list_field(value: Any) -> List[str]:
    return _as_str_list(value)


def bool_field(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if value is None:
        return False
    token = str(value).strip().lower()
    return token in {"1", "true", "t", "yes", "y"}


def choose_template_ontology_hash(df: pd.DataFrame) -> Optional[str]:
    if "ontology_spec_hash" not in df.columns:
        return None
    hashes = sorted({str(v).strip() for v in df["ontology_spec_hash"].tolist() if str(v).strip()})
    if len(hashes) == 1:
        return hashes[0]
    return None


def ontology_component_hash_fields(component_hashes: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    return {
        "taxonomy_hash": component_hashes.get("taxonomy"),
        "canonical_event_registry_hash": component_hashes.get("canonical_event_registry"),
        "state_registry_hash": component_hashes.get("state_registry"),
        "verb_lexicon_hash": component_hashes.get("template_verb_lexicon"),
    }


def compare_hash_fields(
    expected: str,
    candidates: Iterable[Tuple[str, Optional[str]]],
) -> List[str]:
    mismatches: List[str] = []
    for label, value in candidates:
        v = str(value or "").strip()
        if not v:
            continue
        if v != expected:
            mismatches.append(f"{label}={v} (expected {expected})")
    return mismatches


def normalize_state_registry_records(state_registry: Dict[str, Any]) -> List[Dict[str, Any]]:
    defaults = state_registry.get("defaults", {}) if isinstance(state_registry, dict) else {}
    if not isinstance(defaults, dict):
        defaults = {}
    default_scope = str(defaults.get("state_scope", "source_only")).strip() or "source_only"
    default_min_events = int(defaults.get("min_events", 200) or 200)

    out: List[Dict[str, Any]] = []
    rows = state_registry.get("states", []) if isinstance(state_registry, dict) else []
    if not isinstance(rows, list):
        return out
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        state_id = str(raw.get("state_id", "")).strip().upper()
        source_event_type = str(raw.get("source_event_type", "")).strip().upper()
        family = str(raw.get("family", "")).strip().upper()
        if not state_id or not source_event_type:
            continue
        state_scope = str(raw.get("state_scope", default_scope)).strip().lower() or "source_only"
        if state_scope not in {"source_only", "family_safe", "global"}:
            state_scope = "source_only"
        min_events = int(raw.get("min_events", default_min_events) or default_min_events)
        out.append(
            {
                "state_id": state_id,
                "family": family,
                "source_event_type": source_event_type,
                "state_scope": state_scope,
                "min_events": max(0, min_events),
                "activation_rule": str(raw.get("activation_rule", "")).strip(),
                "decay_rule": str(raw.get("decay_rule", "")).strip(),
                "max_duration": raw.get("max_duration"),
            }
        )
    return out


def validate_state_registry_source_events(
    *,
    state_registry: Dict[str, Any],
    canonical_event_types: Iterable[str],
) -> List[str]:
    known = {str(x).strip().upper() for x in canonical_event_types if str(x).strip()}
    issues: List[str] = []
    for row in normalize_state_registry_records(state_registry):
        source_event = row["source_event_type"]
        if source_event not in known:
            issues.append(
                f"state_id={row['state_id']} has source_event_type={source_event} not present in canonical registry"
            )
    return sorted(set(issues))
