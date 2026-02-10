from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

REQUIRED_EDGE_FIELDS = {
    "edge_id",
    "strategy",
    "status",
    "signal_fields",
    "cost_assumptions",
    "validity_window",
    "required_evidence",
    "params",
}
ALLOWED_EDGE_STATUS = {"DRAFT", "APPROVED", "REJECTED", "ARCHIVED"}
REQUIRED_EVIDENCE_FIELDS = {"run_id", "split", "date_range", "universe", "config_hash"}
REQUIRED_APPROVED_COMPARATIVE_FIELDS = {"family_id", "relative_rank_within_family", "dominated_by"}


@dataclass(frozen=True)
class EdgeContractValidationError(Exception):
    message: str

    def __str__(self) -> str:
        return self.message


def _validate_required_fields(edge: Dict[str, Any]) -> None:
    missing = sorted(REQUIRED_EDGE_FIELDS.difference(edge.keys()))
    if missing:
        raise EdgeContractValidationError(f"edge contract missing required fields: {missing}")


def _validate_status(edge: Dict[str, Any]) -> None:
    status = str(edge.get("status", "")).strip().upper()
    if status not in ALLOWED_EDGE_STATUS:
        raise EdgeContractValidationError(f"unsupported edge status: {status}")


def _validate_signal_fields(edge: Dict[str, Any]) -> None:
    signal_fields = edge.get("signal_fields")
    if not isinstance(signal_fields, list) or not signal_fields:
        raise EdgeContractValidationError("signal_fields must be a non-empty list")
    if any(not str(field).strip() for field in signal_fields):
        raise EdgeContractValidationError("signal_fields cannot contain empty names")


def _validate_cost_assumptions(edge: Dict[str, Any]) -> None:
    cost = edge.get("cost_assumptions")
    if not isinstance(cost, dict):
        raise EdgeContractValidationError("cost_assumptions must be an object")
    for key in ("fee_bps_per_side", "slippage_bps_per_fill"):
        if key not in cost:
            raise EdgeContractValidationError(f"cost_assumptions missing {key}")
        if not isinstance(cost[key], (int, float)) or float(cost[key]) < 0:
            raise EdgeContractValidationError(f"cost_assumptions.{key} must be a non-negative number")


def _validate_validity_window(edge: Dict[str, Any]) -> None:
    validity = edge.get("validity_window")
    if not isinstance(validity, dict):
        raise EdgeContractValidationError("validity_window must be an object")
    for key in ("start", "end"):
        if key not in validity:
            raise EdgeContractValidationError(f"validity_window missing {key}")


def _validate_params(edge: Dict[str, Any]) -> None:
    params = edge.get("params")
    if not isinstance(params, dict):
        raise EdgeContractValidationError("params must be an object")


def _validate_required_evidence(edge: Dict[str, Any], require_approved: bool) -> None:
    entries = edge.get("required_evidence")
    status = str(edge.get("status", "")).strip().upper()

    if not isinstance(entries, list):
        raise EdgeContractValidationError("required_evidence must be a list")

    if status == "APPROVED" or require_approved:
        if not entries:
            raise EdgeContractValidationError("APPROVED edge must include non-empty required_evidence")
        for idx, item in enumerate(entries):
            if not isinstance(item, dict):
                raise EdgeContractValidationError(f"required_evidence[{idx}] must be an object")
            missing = sorted(REQUIRED_EVIDENCE_FIELDS.difference(item.keys()))
            if missing:
                raise EdgeContractValidationError(f"required_evidence[{idx}] missing fields {missing}")
            for field in REQUIRED_EVIDENCE_FIELDS:
                if not str(item.get(field, "")).strip():
                    raise EdgeContractValidationError(f"required_evidence[{idx}].{field} cannot be empty")


def _validate_comparative_fields(edge: Dict[str, Any], require_approved: bool) -> None:
    status = str(edge.get("status", "")).strip().upper()
    if status != "APPROVED" and not require_approved:
        return

    missing = sorted(REQUIRED_APPROVED_COMPARATIVE_FIELDS.difference(edge.keys()))
    if missing:
        raise EdgeContractValidationError(f"APPROVED edge missing comparative fields: {missing}")

    family_id = str(edge.get("family_id", "")).strip()
    if not family_id:
        raise EdgeContractValidationError("APPROVED edge requires non-empty family_id")

    rank = edge.get("relative_rank_within_family")
    if not isinstance(rank, int) or rank < 1:
        raise EdgeContractValidationError("relative_rank_within_family must be an integer >= 1")

    dominated_by = edge.get("dominated_by")
    if not isinstance(dominated_by, list):
        raise EdgeContractValidationError("dominated_by must be a list")
    if any(not str(item).strip() for item in dominated_by):
        raise EdgeContractValidationError("dominated_by cannot contain empty edge ids")


def validate_edge_contract(edge: Dict[str, Any], *, require_approved: bool = False) -> Dict[str, Any]:
    """Validate edge contract structure and return a defensive copy."""
    if not isinstance(edge, dict):
        raise EdgeContractValidationError("edge contract must be an object")

    _validate_required_fields(edge)
    _validate_status(edge)
    _validate_signal_fields(edge)
    _validate_cost_assumptions(edge)
    _validate_validity_window(edge)
    _validate_params(edge)
    _validate_required_evidence(edge, require_approved=require_approved)
    _validate_comparative_fields(edge, require_approved=require_approved)
    return deepcopy(edge)


def load_approved_edge_contracts(edges: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    approved: List[Dict[str, Any]] = []
    for edge in edges:
        parsed = validate_edge_contract(edge)
        if str(parsed.get("status", "")).strip().upper() == "APPROVED":
            # Approved edges must include a complete evidence block.
            parsed = validate_edge_contract(parsed, require_approved=True)
            approved.append(parsed)
    if not approved:
        raise EdgeContractValidationError("no APPROVED edge contracts found")
    return approved
