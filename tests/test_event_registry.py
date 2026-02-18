from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from events.registry import EVENT_REGISTRY_SPECS, load_registry_events


def _write_phase1_event_fixture(tmp_path: Path, run_id: str, event_type: str, rows: int = 12) -> None:
    spec = EVENT_REGISTRY_SPECS[event_type]
    out_dir = tmp_path / "reports" / spec.reports_dir / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = pd.date_range("2024-01-01", periods=rows, freq="15min", tz="UTC")
    events = pd.DataFrame(
        {
            "event_id": [f"e{i}" for i in range(rows)],
            "symbol": ["BTCUSDT" if i % 2 == 0 else "ETHUSDT" for i in range(rows)],
            "enter_ts": ts,
        }
    )
    events.to_csv(out_dir / spec.events_file, index=False)


def _run_build_registry(tmp_path: Path, run_id: str, event_type: str = "all") -> None:
    env = os.environ.copy()
    env["BACKTEST_DATA_ROOT"] = str(tmp_path)
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "project" / "pipelines" / "research" / "build_event_registry.py"),
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--event_type",
            event_type,
        ],
        check=True,
        env=env,
    )


def test_event_registry_phase1_count_parity(tmp_path: Path) -> None:
    run_id = "registry_count_parity"
    _write_phase1_event_fixture(tmp_path=tmp_path, run_id=run_id, event_type="vol_shock_relaxation", rows=14)

    _run_build_registry(tmp_path=tmp_path, run_id=run_id, event_type="vol_shock_relaxation")
    registry_events = load_registry_events(
        data_root=tmp_path,
        run_id=run_id,
        event_type="vol_shock_relaxation",
        symbols=["BTCUSDT", "ETHUSDT"],
    )
    assert len(registry_events) == 14


def test_event_registry_rebuild_is_deterministic(tmp_path: Path) -> None:
    run_id = "registry_deterministic"
    _write_phase1_event_fixture(tmp_path=tmp_path, run_id=run_id, event_type="vol_shock_relaxation", rows=10)
    _write_phase1_event_fixture(tmp_path=tmp_path, run_id=run_id, event_type="liquidity_absence_window", rows=8)

    _run_build_registry(tmp_path=tmp_path, run_id=run_id, event_type="all")
    events_path = tmp_path / "events" / run_id / "events.parquet"
    if not events_path.exists():
        events_path = tmp_path / "events" / run_id / "events.csv"
    first = pd.read_parquet(events_path) if events_path.suffix == ".parquet" else pd.read_csv(events_path)

    _run_build_registry(tmp_path=tmp_path, run_id=run_id, event_type="all")
    second = pd.read_parquet(events_path) if events_path.suffix == ".parquet" else pd.read_csv(events_path)

    first_sorted = first.sort_values(["timestamp", "symbol", "event_type", "event_id"]).reset_index(drop=True)
    second_sorted = second.sort_values(["timestamp", "symbol", "event_type", "event_id"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(first_sorted, second_sorted)

    manifest = tmp_path / "events" / run_id / "registry_manifest.json"
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert payload["event_rows"] == len(first_sorted)
