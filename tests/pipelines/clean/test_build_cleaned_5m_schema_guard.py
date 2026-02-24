from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.clean.build_cleaned_5m as build_cleaned_5m


def test_build_cleaned_fails_fast_when_raw_ohlcv_schema_missing(monkeypatch):
    manifest_calls = []

    def fake_start_manifest(stage_name, run_id, params, inputs, outputs):
        return {"stage": stage_name, "run_id": run_id, "params": params}

    def fake_finalize_manifest(manifest, status, error=None, stats=None):
        manifest_calls.append(
            {
                "status": status,
                "error": error,
                "stats": stats,
            }
        )
        return manifest

    def fake_list_parquet_files(_path):
        return [Path("/tmp/fake.parquet")]

    def fake_read_parquet(_files):
        return pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    ["2026-01-01 00:00:00", "2026-01-01 00:05:00"], utc=True
                ),
                "open": [100.0, 101.0],
                "high": [101.0, 102.0],
                "low": [99.0, 100.0],
                "close": [100.5, 101.5],
                # volume intentionally missing
            }
        )

    monkeypatch.setattr(build_cleaned_5m, "start_manifest", fake_start_manifest)
    monkeypatch.setattr(build_cleaned_5m, "finalize_manifest", fake_finalize_manifest)
    monkeypatch.setattr(build_cleaned_5m, "list_parquet_files", fake_list_parquet_files)
    monkeypatch.setattr(build_cleaned_5m, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_cleaned_5m.py",
            "--run_id",
            "r_schema_guard",
            "--symbols",
            "BTCUSDT",
            "--market",
            "spot",
        ],
    )

    rc = build_cleaned_5m.main()
    assert rc == 1
    assert manifest_calls
    assert manifest_calls[-1]["status"] == "failed"
    assert "Missing columns" in str(manifest_calls[-1]["error"])
    assert "volume" in str(manifest_calls[-1]["error"])
