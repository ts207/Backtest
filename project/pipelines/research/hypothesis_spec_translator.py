from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import yaml

from pipelines._lib.timeframe_constants import HORIZON_BARS_BY_TIMEFRAME


DEFAULT_OUTPUT_SCHEMA = [
    "lift_bps",
    "p_value",
    "q_value",
    "n",
    "effect_ci",
    "stability_score",
    "net_after_cost",
]

_CONDITION_KEY_ALIASES = {
    "funding_bps": "funding_rate_bps",
}


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _norm_upper(value: Any) -> str:
    return _norm(value).upper()


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def load_active_hypothesis_specs(repo_root: Path) -> List[Dict[str, Any]]:
    repo_root = Path(repo_root)
    spec_dir = repo_root / "spec" / "hypotheses"
    if not spec_dir.exists():
        return []

    out: List[Dict[str, Any]] = []
    for path in sorted(spec_dir.glob("*.yaml")):
        if path.name == "template_verb_lexicon.yaml":
            continue
        doc = _load_yaml(path)
        if not doc:
            continue
        status = _norm(doc.get("status", "active")).lower()
        if status != "active":
            continue
        hypothesis_id = _norm(doc.get("hypothesis_id")) or path.stem.upper()
        version = int(doc.get("version", 1) or 1)
        scope = doc.get("scope", {})
        if not isinstance(scope, dict):
            scope = {}
        conditioning_features = scope.get("conditioning_features", [])
        if not isinstance(conditioning_features, list):
            conditioning_features = []
        conditioning_features = [
            _norm(feature)
            for feature in conditioning_features
            if _norm(feature)
        ]

        metric = "lift_bps"
        claim = doc.get("claim", {})
        if isinstance(claim, dict):
            quantitative_target = claim.get("quantitative_target", {})
            if isinstance(quantitative_target, dict):
                metric = _norm(quantitative_target.get("metric")) or metric

        out.append(
            {
                "hypothesis_id": hypothesis_id,
                "version": version,
                "spec_path": str(path.relative_to(repo_root)),
                "conditioning_features": conditioning_features,
                "metric": metric,
                "output_schema": list(DEFAULT_OUTPUT_SCHEMA),
            }
        )
    return out


def load_template_side_policy(repo_root: Path) -> Dict[str, str]:
    lexicon_path = Path(repo_root) / "spec" / "hypotheses" / "template_verb_lexicon.yaml"
    doc = _load_yaml(lexicon_path)
    operators = doc.get("operators", {})
    if not isinstance(operators, dict):
        return {}
    out: Dict[str, str] = {}
    for template_verb, op in operators.items():
        if not isinstance(op, dict):
            continue
        side_policy = _norm(op.get("side_policy", "both")).lower()
        out[_norm(template_verb)] = side_policy or "both"
    return out


def _normalize_available_condition_keys(keys: Iterable[str]) -> set[str]:
    out: set[str] = set()
    for key in keys:
        token = _norm(key)
        if not token:
            continue
        out.add(token)
        out.add(token.lower())
        out.add(_CONDITION_KEY_ALIASES.get(token, token))
    return out


def _horizon_bars(horizon: str) -> int:
    key = _norm(horizon).lower()
    return int(HORIZON_BARS_BY_TIMEFRAME.get(key, 12))


def _condition_signature(conditioning: Dict[str, Any]) -> str:
    if not conditioning:
        return "all"
    parts: List[str] = []
    for key in sorted(conditioning.keys()):
        value = conditioning.get(key)
        parts.append(f"{_norm(key)}={_norm(value)}")
    return "&".join(parts) if parts else "all"


def _condition_dsl(conditioning: Dict[str, Any]) -> str:
    if not conditioning:
        return "all"
    clauses: List[str] = []
    for key in sorted(conditioning.keys()):
        value = _norm(conditioning.get(key))
        clauses.append(f'{_norm(key)} == "{value}"')
    return " AND ".join(clauses) if clauses else "all"


def _candidate_hash(payload: Dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return "cand_" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:24]


def translate_candidate_hypotheses(
    *,
    base_candidate: Dict[str, Any],
    hypothesis_specs: List[Dict[str, Any]],
    available_condition_keys: Iterable[str],
    template_side_policy: Dict[str, str],
    strict: bool,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    audit: List[Dict[str, Any]] = []
    available = _normalize_available_condition_keys(available_condition_keys)
    conditioning = dict(base_candidate.get("conditioning", {}) or {})

    for spec in hypothesis_specs:
        hypothesis_id = _norm(spec.get("hypothesis_id"))
        required_features = [_norm(x) for x in spec.get("conditioning_features", []) if _norm(x)]

        missing_required = [
            key for key in required_features
            if key not in available and key.lower() not in available and _CONDITION_KEY_ALIASES.get(key, key) not in available
        ]
        if missing_required:
            detail = {
                "hypothesis_id": hypothesis_id,
                "status": "skipped_missing_spec_condition_keys",
                "missing_keys": sorted(set(missing_required)),
            }
            audit.append(detail)
            if strict:
                raise ValueError(
                    f"Hypothesis {hypothesis_id} missing required conditioning keys in joined frame: {sorted(set(missing_required))}"
                )
            continue

        condition_keys = [_norm(k) for k in conditioning.keys() if _norm(k)]
        missing_condition_keys = [
            key for key in condition_keys
            if key not in available and key.lower() not in available and _CONDITION_KEY_ALIASES.get(key, key) not in available
        ]
        if missing_condition_keys:
            detail = {
                "hypothesis_id": hypothesis_id,
                "status": "skipped_missing_condition_keys",
                "missing_keys": sorted(set(missing_condition_keys)),
            }
            audit.append(detail)
            if strict:
                raise ValueError(
                    f"Hypothesis {hypothesis_id} missing condition keys in joined frame: {sorted(set(missing_condition_keys))}"
                )
            continue

        template_verb = _norm(base_candidate.get("rule_template"))
        direction_rule = _norm(template_side_policy.get(template_verb, "both")).lower() or "both"
        condition_signature = _condition_signature(conditioning)
        condition_dsl = _condition_dsl(conditioning)
        horizon = _norm(base_candidate.get("horizon"))
        row = dict(base_candidate)
        row.update(
            {
                "hypothesis_id": hypothesis_id,
                "hypothesis_version": int(spec.get("version", 1) or 1),
                "hypothesis_spec_path": _norm(spec.get("spec_path")),
                "template_id": template_verb,
                "horizon_bars": _horizon_bars(horizon),
                "entry_lag_bars": int(base_candidate.get("entry_lag_bars", 0) or 0),
                "direction_rule": direction_rule,
                "condition_signature": condition_signature,
                "condition": condition_dsl,
                "hypothesis_metric": _norm(spec.get("metric", "lift_bps")) or "lift_bps",
                "hypothesis_output_schema": list(spec.get("output_schema", DEFAULT_OUTPUT_SCHEMA)),
            }
        )
        row["candidate_id"] = _candidate_hash(
            {
                "hypothesis_id": row["hypothesis_id"],
                "event_type": _norm(row.get("event_type")),
                "symbol": _norm(row.get("symbol")),
                "template_id": row["template_id"],
                "horizon_bars": int(row["horizon_bars"]),
                "entry_lag_bars": int(row["entry_lag_bars"]),
                "direction_rule": row["direction_rule"],
                "condition_signature": row["condition_signature"],
                "state_id": _norm_upper(row.get("state_id")),
            }
        )
        rows.append(row)
        audit.append(
            {
                "hypothesis_id": hypothesis_id,
                "status": "executed",
                "missing_keys": [],
                "candidate_id": row["candidate_id"],
            }
        )

    return rows, audit
