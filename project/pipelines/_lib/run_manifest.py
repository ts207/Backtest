from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


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
