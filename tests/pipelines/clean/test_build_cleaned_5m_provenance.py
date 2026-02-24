from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.clean.build_cleaned_5m as build_cleaned_5m


def _raw_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"], utc=True),
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "volume": [10.0, 11.0],
        }
    )


def _funding_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T08:00:00Z"], utc=True),
            "funding_rate": [1.0, 1.1],
            "source": ["fundingRate", "fundingRate"],
        }
    )


def test_build_cleaned_validates_input_provenance(monkeypatch, tmp_path):
    read_calls = {"i": 0}
    captured = {}

    def fake_list_parquet_files(_path):
        return [Path("dummy.parquet")]

    def fake_read_parquet(_files):
        read_calls["i"] += 1
        return _raw_frame() if read_calls["i"] == 1 else _funding_frame()

    def fake_start_manifest(stage_name, run_id, params, inputs, outputs):
        return {"stage": stage_name, "run_id": run_id, "params": params, "inputs": inputs, "outputs": outputs}

    def fake_finalize_manifest(manifest, status, error=None, stats=None):
        captured["status"] = status
        captured["error"] = error
        return manifest

    def fake_write_parquet(df, path):
        return Path(path), "parquet"

    def fake_validate_input_provenance(inputs):
        captured["inputs"] = inputs

    monkeypatch.setattr(build_cleaned_5m, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(build_cleaned_5m, "list_parquet_files", fake_list_parquet_files)
    monkeypatch.setattr(build_cleaned_5m, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(build_cleaned_5m, "start_manifest", fake_start_manifest)
    monkeypatch.setattr(build_cleaned_5m, "finalize_manifest", fake_finalize_manifest)
    monkeypatch.setattr(build_cleaned_5m, "write_parquet", fake_write_parquet)
    monkeypatch.setattr(build_cleaned_5m, "validate_input_provenance", fake_validate_input_provenance)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_cleaned_5m.py",
            "--run_id",
            "r_prov",
            "--symbols",
            "BTCUSDT",
            "--market",
            "perp",
            "--funding_scale",
            "bps",
        ],
    )

    rc = build_cleaned_5m.main()

    assert rc == 0
    assert captured.get("status") == "success"
    assert "inputs" in captured
    assert len(captured["inputs"]) >= 2
    for item in captured["inputs"]:
        provenance = item.get("provenance", {})
        assert provenance.get("vendor")
        assert provenance.get("exchange")
        assert provenance.get("schema_version")
        assert provenance.get("schema_hash")
        assert provenance.get("extraction_start")
        assert provenance.get("extraction_end")
