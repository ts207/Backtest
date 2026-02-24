from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib import run_manifest


def test_start_manifest_includes_git_and_spec_hashes():
    manifest = run_manifest.start_manifest(
        stage_name="unit_stage",
        run_id="r_manifest",
        params={},
        inputs=[],
        outputs=[],
    )
    assert "git_commit" in manifest
    assert "spec_hashes" in manifest
    assert isinstance(manifest["spec_hashes"], dict)
    assert str(manifest.get("ontology_spec_hash", "")).startswith("sha256:")
    assert "taxonomy_hash" in manifest
    assert "canonical_event_registry_hash" in manifest
    assert "state_registry_hash" in manifest
    assert "verb_lexicon_hash" in manifest
    assert "python_version" in manifest
    assert "platform" in manifest
    assert "env_snapshot" in manifest


def test_finalize_manifest_hashes_input_parquets(monkeypatch, tmp_path):
    parquet_path = tmp_path / "input.parquet"
    payload = b"fake parquet payload"
    parquet_path.write_bytes(payload)
    expected_hash = hashlib.sha256(payload).hexdigest()

    out_manifest = tmp_path / "unit_stage.json"
    monkeypatch.setattr(run_manifest, "_manifest_path", lambda run_id, stage: out_manifest)

    manifest = run_manifest.start_manifest(
        stage_name="unit_stage",
        run_id="r_manifest",
        params={},
        inputs=[{"path": str(parquet_path)}],
        outputs=[],
    )
    finalized = run_manifest.finalize_manifest(manifest, status="success", stats={})

    files = finalized["input_parquet_hashes"]["files"]
    assert str(parquet_path) in files
    assert files[str(parquet_path)] == expected_hash

    disk_payload = json.loads(out_manifest.read_text(encoding="utf-8"))
    assert disk_payload["input_parquet_hashes"]["files"][str(parquet_path)] == expected_hash
