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
from events.registry import load_registry_flags


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


def test_registry_incremental_build_preserves_previous_families(tmp_path: Path) -> None:
    run_id = "registry_incremental_preserve"
    event_a = "vol_shock_relaxation"
    event_b = "liquidity_absence_window"
    _write_phase1_event_fixture(tmp_path=tmp_path, run_id=run_id, event_type=event_a, rows=7)
    _write_phase1_event_fixture(tmp_path=tmp_path, run_id=run_id, event_type=event_b, rows=5)

    _run_build_registry(tmp_path=tmp_path, run_id=run_id, event_type=event_a)
    _run_build_registry(tmp_path=tmp_path, run_id=run_id, event_type=event_b)

    registry_events = load_registry_events(data_root=tmp_path, run_id=run_id)
    per_family = registry_events.groupby("event_type").size().to_dict()
    assert int(per_family.get(event_a, 0)) == 7
    assert int(per_family.get(event_b, 0)) == 5


def test_registry_flags_include_multiple_families_after_sequential_builds(tmp_path: Path) -> None:
    run_id = "registry_flags_multifamily"
    event_a = "vol_shock_relaxation"
    event_b = "liquidity_absence_window"
    _write_phase1_event_fixture(tmp_path=tmp_path, run_id=run_id, event_type=event_a, rows=8)
    _write_phase1_event_fixture(tmp_path=tmp_path, run_id=run_id, event_type=event_b, rows=6)

    _run_build_registry(tmp_path=tmp_path, run_id=run_id, event_type=event_a)
    _run_build_registry(tmp_path=tmp_path, run_id=run_id, event_type=event_b)

    flags = load_registry_flags(data_root=tmp_path, run_id=run_id)
    signal_a = EVENT_REGISTRY_SPECS[event_a].signal_column
    signal_b = EVENT_REGISTRY_SPECS[event_b].signal_column
    assert signal_a in flags.columns
    assert signal_b in flags.columns
    assert bool(flags[signal_a].any())
    assert bool(flags[signal_b].any())


def test_registry_merge_replaces_selected_family_only(tmp_path: Path) -> None:
    run_id = "registry_replace_selected_family"
    event_a = "vol_shock_relaxation"
    event_b = "liquidity_absence_window"
    _write_phase1_event_fixture(tmp_path=tmp_path, run_id=run_id, event_type=event_a, rows=9)
    _write_phase1_event_fixture(tmp_path=tmp_path, run_id=run_id, event_type=event_b, rows=4)

    _run_build_registry(tmp_path=tmp_path, run_id=run_id, event_type=event_a)
    _run_build_registry(tmp_path=tmp_path, run_id=run_id, event_type=event_b)

    _write_phase1_event_fixture(tmp_path=tmp_path, run_id=run_id, event_type=event_a, rows=3)
    _run_build_registry(tmp_path=tmp_path, run_id=run_id, event_type=event_a)

    registry_events = load_registry_events(data_root=tmp_path, run_id=run_id)
    per_family = registry_events.groupby("event_type").size().to_dict()
    assert int(per_family.get(event_a, 0)) == 3
    assert int(per_family.get(event_b, 0)) == 4
