import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

REQUIRED_KEYS = {
    "vendor",
    "exchange",
    "schema_version",
    "schema_hash",
    "extraction_start",
    "extraction_end",
}


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _seed_raw_inputs(data_root: Path, symbol: str) -> None:
    ts = pd.date_range("2024-01-01", periods=96, freq="15min", tz="UTC")
    raw = pd.DataFrame(
        {
            "timestamp": ts,
            "open": [100.0 + i * 0.1 for i in range(len(ts))],
            "high": [100.2 + i * 0.1 for i in range(len(ts))],
            "low": [99.8 + i * 0.1 for i in range(len(ts))],
            "close": [100.1 + i * 0.1 for i in range(len(ts))],
            "volume": [10.0] * len(ts),
        }
    )
    funding_ts = pd.date_range("2024-01-01", periods=4, freq="8h", tz="UTC")
    funding = pd.DataFrame({"timestamp": funding_ts, "funding_rate": [0.0001, -0.0001, 0.0002, -0.0002]})

    raw_path = data_root / "lake" / "raw" / "binance" / "perp" / symbol / "ohlcv_15m" / "raw.csv"
    funding_path = data_root / "lake" / "raw" / "binance" / "perp" / symbol / "funding" / "funding.csv"
    _write_csv(raw, raw_path)
    _write_csv(funding, funding_path)


def test_clean_and_features_manifests_include_input_provenance(tmp_path: Path) -> None:
    run_id = "prov_test"
    symbol = "BTCUSDT"
    _seed_raw_inputs(tmp_path, symbol)

    env = os.environ.copy()
    env["BACKTEST_DATA_ROOT"] = str(tmp_path)

    clean_cmd = [
        sys.executable,
        str(ROOT / "project" / "pipelines" / "clean" / "build_cleaned_15m.py"),
        "--run_id",
        run_id,
        "--symbols",
        symbol,
        "--market",
        "perp",
        "--start",
        "2024-01-01",
        "--end",
        "2024-01-01",
    ]
    subprocess.run(clean_cmd, check=True, env=env)

    features_cmd = [
        sys.executable,
        str(ROOT / "project" / "pipelines" / "features" / "build_features_v1.py"),
        "--run_id",
        run_id,
        "--symbols",
        symbol,
        "--market",
        "perp",
    ]
    subprocess.run(features_cmd, check=True, env=env)

    clean_manifest = json.loads((tmp_path / "runs" / run_id / "build_cleaned_15m.json").read_text(encoding="utf-8"))
    features_manifest = json.loads((tmp_path / "runs" / run_id / "build_features_v1.json").read_text(encoding="utf-8"))
    context_cmd = [
        sys.executable,
        str(ROOT / "project" / "pipelines" / "features" / "build_context_features.py"),
        "--run_id",
        run_id,
        "--symbols",
        symbol,
        "--timeframe",
        "15m",
        "--start",
        "2024-01-01",
        "--end",
        "2024-01-01",
        "--force",
        "1",
    ]
    subprocess.run(context_cmd, check=True, env=env)
    context_manifest = json.loads((tmp_path / "runs" / run_id / "build_context_features.json").read_text(encoding="utf-8"))

    for manifest in [clean_manifest, features_manifest, context_manifest]:
        assert manifest["status"] == "success"
        assert manifest["inputs"]
        for row in manifest["inputs"]:
            assert "provenance" in row
            assert REQUIRED_KEYS.issubset(set(row["provenance"].keys()))
            assert all(row["provenance"][k] for k in REQUIRED_KEYS)


def test_validate_input_provenance_raises_on_missing_block() -> None:
    sys.path.insert(0, str(ROOT / "project"))
    from pipelines._lib.run_manifest import validate_input_provenance

    try:
        validate_input_provenance([{"path": "x.csv"}])
    except ValueError as exc:
        assert "missing provenance block" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing provenance")


def test_run_all_writes_run_level_manifest_with_provenance(monkeypatch, tmp_path: Path) -> None:
    sys.path.insert(0, str(ROOT / "project"))
    from pipelines import run_all

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(run_all, "_run_stage", lambda stage, script_path, base_args, run_id: True)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            "manifest_run",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
        ],
    )

    assert run_all.main() == 0
    payload = json.loads((tmp_path / "runs" / "manifest_run" / "run_manifest.json").read_text(encoding="utf-8"))
    for key in [
        "git_commit",
        "data_hash",
        "feature_schema_version",
        "feature_schema_hash",
        "config_digest",
    ]:
        assert key in payload
        assert str(payload[key]).strip() != ""
    assert payload["status"] == "success"
