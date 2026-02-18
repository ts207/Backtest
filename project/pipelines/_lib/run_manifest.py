from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _manifest_path(run_id: str, stage: str) -> Path:
    project_root = Path(__file__).resolve().parents[2]
    data_root = Path(os.getenv("BACKTEST_DATA_ROOT", project_root.parent / "data"))
    out_dir = data_root / "runs" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{stage}.json"


REQUIRED_INPUT_PROVENANCE_KEYS = (
    "vendor",
    "exchange",
    "schema_version",
    "schema_hash",
    "extraction_start",
    "extraction_end",
)


def schema_hash_from_columns(columns: List[str]) -> str:
    normalized = [str(col) for col in columns]
    payload = "|".join(normalized)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def validate_input_provenance(inputs: List[Dict[str, Any]]) -> None:
    for idx, item in enumerate(inputs):
        provenance = item.get("provenance")
        if not isinstance(provenance, dict):
            raise ValueError(f"Input index {idx} missing provenance block")
        missing = [k for k in REQUIRED_INPUT_PROVENANCE_KEYS if not provenance.get(k)]
        if missing:
            path = item.get("path", f"input[{idx}]")
            raise ValueError(f"Input {path} missing required provenance keys: {missing}")


def feature_schema_registry_path() -> Path:
    project_root = Path(__file__).resolve().parents[2]
    return project_root / "schemas" / "feature_schema_v1.json"


def load_feature_schema_registry() -> Dict[str, Any]:
    schema_path = feature_schema_registry_path()
    if not schema_path.exists():
        raise ValueError(f"Feature schema registry missing: {schema_path}")
    try:
        payload = json.loads(schema_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Feature schema registry is invalid JSON: {schema_path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Feature schema registry must be a JSON object: {schema_path}")
    return payload


def feature_schema_identity() -> Tuple[str, str]:
    schema_path = feature_schema_registry_path()
    payload = load_feature_schema_registry()
    version = str(payload.get("version", "feature_schema_v1"))
    schema_hash = hashlib.sha256(schema_path.read_bytes()).hexdigest()
    return version, schema_hash


def validate_feature_schema_columns(*, dataset_key: str, columns: List[str]) -> Tuple[str, str]:
    registry = load_feature_schema_registry()
    datasets = registry.get("datasets", {})
    if not isinstance(datasets, dict):
        raise ValueError("Feature schema registry missing `datasets` object")
    contract = datasets.get(dataset_key, {})
    if not isinstance(contract, dict):
        raise ValueError(f"Feature schema registry missing dataset contract: {dataset_key}")
    required_columns = contract.get("required_columns", [])
    if not isinstance(required_columns, list):
        raise ValueError(f"Feature schema required_columns must be a list for dataset: {dataset_key}")
    missing = [col for col in required_columns if col not in columns]
    if missing:
        raise ValueError(f"Feature schema contract violated for {dataset_key}; missing columns: {missing}")
    return feature_schema_identity()


def start_manifest(
    stage_name: str,
    run_id: str,
    params: Dict[str, Any],
    inputs: List[Dict[str, Any]],
    outputs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "stage": stage_name,
        "started_at": _utc_now_iso(),
        "finished_at": None,
        "status": "running",
        "parameters": params,
        "inputs": inputs,
        "outputs": outputs,
        "error": None,
        "stats": None,
    }


def finalize_manifest(
    manifest: Dict[str, Any],
    status: str,
    error: Optional[str] = None,
    stats: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    manifest["finished_at"] = _utc_now_iso()
    manifest["status"] = status
    manifest["error"] = error
    manifest["stats"] = stats

    out_path = _manifest_path(manifest["run_id"], manifest["stage"])
    temp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    temp_path.replace(out_path)
    return manifest
