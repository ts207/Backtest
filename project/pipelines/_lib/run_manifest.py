from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pipelines._lib.spec_utils import get_spec_hashes


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _manifest_path(run_id: str, stage: str) -> Path:
    project_root = Path(__file__).resolve().parents[2]
    data_root = Path(os.getenv("BACKTEST_DATA_ROOT", project_root.parent / "data"))
    out_dir = data_root / "runs" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{stage}.json"


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _git_commit(project_root: Path) -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(project_root), "rev-parse", "HEAD"],
            text=True,
        ).strip()
    except Exception:
        return "unknown"


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _input_parquet_hashes(inputs: List[Dict[str, Any]], *, max_files: int = 32) -> Dict[str, Any]:
    files: List[Path] = []
    seen: set[str] = set()
    for item in inputs:
        raw_path = item.get("path")
        if not raw_path:
            continue
        path = Path(str(raw_path))
        if path.is_file() and path.suffix.lower() == ".parquet":
            key = str(path.resolve())
            if key not in seen:
                files.append(path)
                seen.add(key)
            continue
        if path.is_dir():
            for child in sorted(path.rglob("*.parquet")):
                key = str(child.resolve())
                if key in seen:
                    continue
                files.append(child)
                seen.add(key)
                if len(files) >= max_files:
                    break
        if len(files) >= max_files:
            break

    hashes: Dict[str, str] = {}
    for path in files:
        if not path.exists() or not path.is_file():
            continue
        try:
            hashes[str(path)] = _sha256_file(path)
        except OSError:
            continue

    return {
        "files": hashes,
        "truncated": len(files) >= max_files,
        "max_files": int(max_files),
    }


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
    project_root = _project_root()
    return {
        "run_id": run_id,
        "stage": stage_name,
        "started_at": _utc_now_iso(),
        "finished_at": None,
        "status": "running",
        "git_commit": _git_commit(project_root),
        "spec_hashes": get_spec_hashes(project_root.parent),
        "parameters": params,
        "inputs": inputs,
        "outputs": outputs,
        "input_parquet_hashes": {"files": {}, "truncated": False, "max_files": 32},
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
    manifest["input_parquet_hashes"] = _input_parquet_hashes(manifest.get("inputs", []))

    out_path = _manifest_path(manifest["run_id"], manifest["stage"])
    temp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    temp_path.replace(out_path)
    return manifest


def enrich_manifest_with_env(manifest: dict):
    import platform, sys, os
    manifest["python_version"] = sys.version
    manifest["platform"] = platform.platform()
    manifest["env_snapshot"] = {k: os.environ.get(k) for k in ["BACKTEST_DATA_ROOT"]}
    return manifest
