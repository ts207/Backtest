from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from pathlib import Path

from scripts.build_run_registry import upsert_run, create_schema, query_top_promoted


def test_upsert_and_query(tmp_path):
    db_path = tmp_path / "runs.sqlite"
    conn = sqlite3.connect(db_path)
    create_schema(conn)
    upsert_run(conn, {
        "run_id": "run_001",
        "stage": "promote_blueprints",
        "status": "success",
        "survivors_count": 3,
        "tested_count": 10,
        "timestamp": "2026-01-01T00:00:00",
    })
    conn.commit()
    rows = query_top_promoted(conn, limit=5)
    assert len(rows) == 1
    assert rows[0]["run_id"] == "run_001"


def test_schema_idempotent(tmp_path):
    db_path = tmp_path / "runs.sqlite"
    conn = sqlite3.connect(db_path)
    create_schema(conn)
    create_schema(conn)  # must not raise
    conn.close()


def test_main_indexes_manifests(tmp_path):
    # Create a fake run manifest
    run_dir = tmp_path / "runs" / "run_abc"
    run_dir.mkdir(parents=True)
    manifest = {
        "run_id": "run_abc",
        "stage": "promote_blueprints",
        "status": "success",
        "started_at": "2026-01-01T00:00:00",
        "stats": {"survivors_count": 5, "tested_count": 20},
    }
    (run_dir / "run_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    db_path = tmp_path / "meta" / "runs.sqlite"
    old_argv = sys.argv
    sys.argv = ["build_run_registry", "--data_root", str(tmp_path), "--db", str(db_path)]
    try:
        from scripts.build_run_registry import main
        main()
    finally:
        sys.argv = old_argv

    conn = sqlite3.connect(db_path)
    rows = query_top_promoted(conn, limit=10)
    conn.close()
    assert len(rows) == 1
    assert rows[0]["run_id"] == "run_abc"
    assert rows[0]["survivors_count"] == 5
