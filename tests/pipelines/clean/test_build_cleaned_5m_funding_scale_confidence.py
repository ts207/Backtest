from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.clean.build_cleaned_5m as build_cleaned_5m
from pipelines._lib import sanity


def test_infer_funding_scale_reports_low_confidence_for_ambiguous_data():
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"], utc=True),
            "funding_rate": [0.0001, 0.0002],
            "source": ["unknown", "unknown"],
        }
    )
    _, scale_used, confidence = sanity.infer_and_apply_funding_scale(frame, "funding_rate")
    assert scale_used == 1.0
    assert confidence < 0.99


def test_infer_funding_scale_explicit_override_is_high_confidence():
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"], utc=True),
            "funding_rate": [0.0001, 0.0002],
            "source": ["unknown", "unknown"],
        }
    )
    _, scale_used, confidence = sanity.infer_and_apply_funding_scale(
        frame,
        "funding_rate",
        explicit_scale=sanity.FUNDING_SCALE_NAME_TO_MULTIPLIER["bps"],
    )
    assert scale_used == sanity.FUNDING_SCALE_NAME_TO_MULTIPLIER["bps"]
    assert confidence == 1.0


def _raw_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"], utc=True),
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "volume": [10.0, 12.0],
        }
    )


def _ambiguous_funding_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"], utc=True),
            "funding_rate": [0.0001, 0.0002],
            "source": ["unknown", "unknown"],
        }
    )


def _monkeypatch_io(monkeypatch, *, raw: pd.DataFrame, funding: pd.DataFrame, tmp_path: Path):
    calls = {"i": 0}

    def fake_list_parquet_files(_path):
        return [Path("dummy.parquet")]

    def fake_read_parquet(_files):
        calls["i"] += 1
        return raw.copy() if calls["i"] == 1 else funding.copy()

    manifest_calls = []

    def fake_start_manifest(stage_name, run_id, params, inputs, outputs):
        return {"stage": stage_name, "run_id": run_id, "params": params, "inputs": inputs, "outputs": outputs}

    def fake_finalize_manifest(manifest, status, error=None, stats=None):
        manifest_calls.append({"status": status, "error": error, "stats": stats})
        return manifest

    def fake_write_parquet(df, path):
        return Path(path), "parquet"

    monkeypatch.setattr(build_cleaned_5m, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(build_cleaned_5m, "list_parquet_files", fake_list_parquet_files)
    monkeypatch.setattr(build_cleaned_5m, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(build_cleaned_5m, "start_manifest", fake_start_manifest)
    monkeypatch.setattr(build_cleaned_5m, "finalize_manifest", fake_finalize_manifest)
    monkeypatch.setattr(build_cleaned_5m, "write_parquet", fake_write_parquet)
    return manifest_calls


def test_build_cleaned_fails_on_low_confidence_auto_scale(monkeypatch, tmp_path):
    manifest_calls = _monkeypatch_io(monkeypatch, raw=_raw_frame(), funding=_ambiguous_funding_frame(), tmp_path=tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_cleaned_5m.py",
            "--run_id",
            "r_low_conf",
            "--symbols",
            "BTCUSDT",
            "--market",
            "perp",
            "--funding_scale",
            "auto",
        ],
    )

    rc = build_cleaned_5m.main()
    assert rc == 1
    assert manifest_calls
    assert manifest_calls[-1]["status"] == "failed"
    assert "Low confidence funding scale inference" in str(manifest_calls[-1]["error"])


def test_build_cleaned_allows_explicit_funding_scale(monkeypatch, tmp_path):
    manifest_calls = _monkeypatch_io(monkeypatch, raw=_raw_frame(), funding=_ambiguous_funding_frame(), tmp_path=tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_cleaned_5m.py",
            "--run_id",
            "r_explicit_scale",
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
    assert manifest_calls
    assert manifest_calls[-1]["status"] == "success"
