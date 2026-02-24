from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research.feasibility_guard import FeasibilityGuard


def test_unknown_dataset_id_fails_closed(tmp_path):
    spec_path = tmp_path / "spec" / "events" / "test_event.yaml"
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec_path.write_text(
        "event_type: test_event\n"
        "inputs:\n"
        "  - dataset: mystery_dataset_v1\n",
        encoding="utf-8",
    )

    guard = FeasibilityGuard(project_root=tmp_path, data_root=tmp_path / "data", run_id="r1")
    ok, reason = guard.check_feasibility("spec/events/test_event.yaml", "BTCUSDT")

    assert ok is False
    assert "mystery_dataset_v1" in reason


def test_known_ohlcv_dataset_id_passes_when_data_exists(tmp_path):
    spec_path = tmp_path / "spec" / "events" / "test_event.yaml"
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec_path.write_text(
        "event_type: test_event\n"
        "inputs:\n"
        "  - dataset: ohlcv_perp_5m\n",
        encoding="utf-8",
    )

    (tmp_path / "data" / "lake" / "cleaned" / "perp" / "BTCUSDT" / "bars_5m").mkdir(parents=True, exist_ok=True)
    guard = FeasibilityGuard(project_root=tmp_path, data_root=tmp_path / "data", run_id="r1")
    ok, reason = guard.check_feasibility("spec/events/test_event.yaml", "BTCUSDT")

    assert ok is True
    assert reason == "ready"
