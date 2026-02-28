from __future__ import annotations

import json
import sqlite3
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
