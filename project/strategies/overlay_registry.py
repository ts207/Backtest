from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EDGES_DIR = PROJECT_ROOT / "edges"
PINNED_SPEC_VERSION = "v1"
REQUIRED_FIELDS = {
    "spec_version",
    "name",
    "event",
    "condition",
    "action",
    "status",
    "tuning_allowed",
    "run_ids_evidence",
    "cost_bps_validated",
    "notes",
    "objective",
    "constraints",
    "stability",
}
ALLOWED_STATUSES = {"DRAFT", "APPROVED", "REJECTED", "ARCHIVED"}
ALLOWED_OBJECTIVE_METRICS = {
    "net_total_return",
    "net_cagr",
    "avg_period_return",
    "geometric_mean_return",
}
REQUIRED_EVIDENCE_FIELDS = {"run_id", "split", "date_range", "universe", "config_hash"}
DEFAULT_OVERLAY_SPECS: Dict[str, Dict[str, Any]] = {
    "funding_extreme_filter": {
        "spec_version": PINNED_SPEC_VERSION,
        "name": "funding_extreme_filter",
        "event": "FUNDING_EXTREME_ONSET",
        "condition": "funding_percentile_high",
        "action": "risk_throttle_0.75",
        "status": "APPROVED",
        "tuning_allowed": False,
        "run_ids_evidence": [
            {
                "run_id": "baseline",
                "split": "validation",
                "date_range": "2020-01-01/2025-12-31",
                "universe": "BTCUSDT,ETHUSDT",
                "config_hash": "builtin",
            }
        ],
        "cost_bps_validated": 6.0,
        "notes": "Built-in overlay for conservative entry throttling during extreme funding dislocations.",
        "objective": {"target_metric": "net_total_return"},
        "constraints": {
            "max_drawdown_pct": 0.25,
            "tail_loss": 0.15,
            "exposure_limits": "default",
            "turnover_budget": "default",
        },
        "stability": {
            "sign_consistency_min": 0.5,
            "effect_ci_excludes_0": True,
            "max_regime_flip_count": 5,
        },
    },
    "mev_aware_risk_filter": {
        "spec_version": PINNED_SPEC_VERSION,
        "name": "mev_aware_risk_filter",
        "event": "microstructure_execution_quality",
        "condition": "mev_risk_bps_high",
        "action": "entry_gate_skip",
        "status": "APPROVED",
        "tuning_allowed": False,
        "run_ids_evidence": [
            {
                "run_id": "baseline",
                "split": "validation",
                "date_range": "2020-01-01/2025-12-31",
                "universe": "BTCUSDT,ETHUSDT",
                "config_hash": "builtin",
            }
        ],
        "cost_bps_validated": 8.0,
        "notes": "Built-in overlay to block/limit entries under elevated MEV-like execution risk.",
        "objective": {"target_metric": "net_total_return"},
        "constraints": {
            "max_drawdown_pct": 0.25,
            "tail_loss": 0.15,
            "exposure_limits": "default",
            "turnover_budget": "default",
        },
        "stability": {
            "sign_consistency_min": 0.5,
            "effect_ci_excludes_0": True,
            "max_regime_flip_count": 5,
        },
        "runtime": {
            "type": "mev_aware_risk_filter",
            "throttle_start_bps": 12.0,
            "block_threshold_bps": 25.0,
        },
    },
}


def _validate_evidence_entries(spec: Dict[str, Any], source_path: Path) -> None:
    entries = spec.get("run_ids_evidence")
    if not isinstance(entries, list) or not entries:
        raise ValueError(f"APPROVED overlay requires non-empty run_ids_evidence: {source_path}")

    for idx, item in enumerate(entries):
        if not isinstance(item, dict):
            raise ValueError(f"APPROVED overlay evidence entry {idx} must be an object: {source_path}")
        missing = sorted(REQUIRED_EVIDENCE_FIELDS.difference(item.keys()))
        if missing:
            raise ValueError(f"APPROVED overlay evidence entry {idx} missing fields {missing}: {source_path}")
        for key in REQUIRED_EVIDENCE_FIELDS:
            if not str(item.get(key, "")).strip():
                raise ValueError(f"APPROVED overlay evidence entry {idx} has empty {key}: {source_path}")


def _validate_approved_overlay_requirements(spec: Dict[str, Any], source_path: Path) -> None:
    _validate_evidence_entries(spec, source_path)

    cost_bps = spec.get("cost_bps_validated")
    if not isinstance(cost_bps, (int, float)) or float(cost_bps) < 0:
        raise ValueError(f"APPROVED overlay requires non-negative numeric cost_bps_validated: {source_path}")

    objective = spec.get("objective")
    target_metric = objective.get("target_metric") if isinstance(objective, dict) else None
    if target_metric not in ALLOWED_OBJECTIVE_METRICS:
        raise ValueError(
            "APPROVED overlay requires objective.target_metric in "
            f"{sorted(ALLOWED_OBJECTIVE_METRICS)}: {source_path}"
        )

    constraints = spec.get("constraints")
    if not isinstance(constraints, dict):
        raise ValueError(f"APPROVED overlay requires constraints object: {source_path}")
    for key in ("max_drawdown_pct", "tail_loss", "exposure_limits", "turnover_budget"):
        if key not in constraints:
            raise ValueError(f"APPROVED overlay missing constraints.{key}: {source_path}")

    stability = spec.get("stability")
    if not isinstance(stability, dict):
        raise ValueError(f"APPROVED overlay requires stability object: {source_path}")
    required_stability_thresholds = ("sign_consistency_min", "effect_ci_excludes_0", "max_regime_flip_count")
    for key in required_stability_thresholds:
        if key not in stability:
            raise ValueError(f"APPROVED overlay missing stability.{key}: {source_path}")

    sign_consistency = stability.get("sign_consistency_min")
    if not isinstance(sign_consistency, (int, float)) or not (0 <= float(sign_consistency) <= 1):
        raise ValueError(f"APPROVED overlay stability.sign_consistency_min must be in [0,1]: {source_path}")

    if not isinstance(stability.get("effect_ci_excludes_0"), bool):
        raise ValueError(f"APPROVED overlay stability.effect_ci_excludes_0 must be boolean: {source_path}")

    max_flips = stability.get("max_regime_flip_count")
    if not isinstance(max_flips, int) or max_flips < 0:
        raise ValueError(f"APPROVED overlay stability.max_regime_flip_count must be a non-negative integer: {source_path}")


def _validate_spec(spec: Dict[str, Any], source_path: Path) -> None:
    missing = sorted(REQUIRED_FIELDS.difference(spec.keys()))
    if missing:
        raise ValueError(f"Overlay spec missing required fields {missing}: {source_path}")
    if spec.get("spec_version") != PINNED_SPEC_VERSION:
        raise ValueError(
            f"Overlay spec version mismatch for {source_path.name}: "
            f"expected {PINNED_SPEC_VERSION}, found {spec.get('spec_version')}"
        )
    status = str(spec.get("status", "")).strip().upper()
    if status not in ALLOWED_STATUSES:
        raise ValueError(f"Overlay spec has unsupported status '{status}': {source_path}")
    if status == "APPROVED":
        _validate_approved_overlay_requirements(spec, source_path)


def _load_overlay_specs(edges_dir: Path = EDGES_DIR) -> Dict[str, Dict[str, Any]]:
    registry: Dict[str, Dict[str, Any]] = deepcopy(DEFAULT_OVERLAY_SPECS)
    spec_paths = sorted(edges_dir.glob("*.json"))
    for spec_path in spec_paths:
        with spec_path.open("r", encoding="utf-8") as f:
            spec = json.load(f)
        _validate_spec(spec, spec_path)
        name = str(spec["name"]).strip()
        if not name:
            raise ValueError(f"Overlay spec has empty name: {spec_path}")
        if name in registry:
            raise ValueError(f"Duplicate overlay spec name '{name}': {spec_path}")
        registry[name] = spec
    return registry


def list_overlays() -> list[str]:
    return sorted(_load_overlay_specs().keys())


def list_applicable_overlays() -> list[str]:
    return sorted([name for name, spec in _load_overlay_specs().items() if spec.get("status") == "APPROVED"])


def get_overlay(name: str) -> Dict[str, Any]:
    key = name.strip()
    registry = _load_overlay_specs()
    if key not in registry:
        available = ", ".join(sorted(registry.keys()))
        raise ValueError(f"Unknown overlay '{name}'. Available overlays: {available}")
    return deepcopy(registry[key])


def apply_overlay(name: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Return params augmented with a named overlay configuration."""
    out = deepcopy(params) if params is not None else {}
    overlay = get_overlay(name)
    status = str(overlay.get("status", "")).strip()
    if status != "APPROVED":
        raise ValueError(f"Overlay {name.strip()} is not applicable (status={status}).")

    overlays = list(out.get("overlays", []))
    overlays.append(name.strip())
    out["overlays"] = overlays

    overlay_specs = deepcopy(out.get("overlay_specs", {}))
    overlay_specs[name.strip()] = overlay
    out["overlay_specs"] = overlay_specs

    runtime_cfg = overlay.get("runtime")
    if isinstance(runtime_cfg, dict):
        overlay_runtime = deepcopy(out.get("overlay_runtime", {}))
        overlay_runtime[name.strip()] = runtime_cfg
        out["overlay_runtime"] = overlay_runtime
    return out
