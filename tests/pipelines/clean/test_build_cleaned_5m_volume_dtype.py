from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.clean.build_cleaned_5m as build_cleaned_5m


def test_build_cleaned_coerces_integer_volume_to_float_before_schema(monkeypatch, tmp_path):
    captured_frames: list[pd.DataFrame] = []

    def fake_list_parquet_files(_path):
        return [Path("dummy.parquet")]

    def fake_read_parquet(_files):
        return pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"], utc=True),
                "open": [100.0, 101.0],
                "high": [101.0, 102.0],
                "low": [99.0, 100.0],
                "close": [100.5, 101.5],
                # integer dtype from upstream parquet chunks is valid raw input
                "volume": [10, 11],
            }
        )

    def fake_start_manifest(stage_name, run_id, params, inputs, outputs):
        return {"stage": stage_name, "run_id": run_id, "params": params, "inputs": inputs, "outputs": outputs}

    def fake_finalize_manifest(manifest, status, error=None, stats=None):
        return manifest

    def fake_write_parquet(df, path):
        captured_frames.append(df.copy())
        return Path(path), "parquet"

    monkeypatch.setattr(build_cleaned_5m, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(build_cleaned_5m, "list_parquet_files", fake_list_parquet_files)
    monkeypatch.setattr(build_cleaned_5m, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(build_cleaned_5m, "start_manifest", fake_start_manifest)
    monkeypatch.setattr(build_cleaned_5m, "finalize_manifest", fake_finalize_manifest)
    monkeypatch.setattr(build_cleaned_5m, "write_parquet", fake_write_parquet)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_cleaned_5m.py",
            "--run_id",
            "r_volume_dtype",
            "--symbols",
            "BTCUSDT",
            "--market",
            "spot",
        ],
    )

    rc = build_cleaned_5m.main()

    assert rc == 0
    assert captured_frames
    assert pd.api.types.is_float_dtype(captured_frames[0]["volume"])
