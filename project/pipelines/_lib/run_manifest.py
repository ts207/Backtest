from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def start_manifest(run_id: str, stage: str, config_paths: List[str]) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "stage": stage,
        "started_at": _utc_now_iso(),
        "finished_at": None,
        "status": "failed",
        "config_paths": config_paths,
        "inputs": [],
        "outputs": [],
        "error": None,
    }


def finalize_manifest(
    manifest: Dict[str, Any],
    inputs: List[Dict[str, Any]],
    outputs: List[Dict[str, Any]],
    status: str,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    manifest["finished_at"] = _utc_now_iso()
    manifest["status"] = status
    manifest["inputs"] = inputs
    manifest["outputs"] = outputs
    manifest["error"] = error

    run_id = manifest["run_id"]
    stage = manifest["stage"]
    out_dir = Path("project") / "runs" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{stage}.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    return manifest
