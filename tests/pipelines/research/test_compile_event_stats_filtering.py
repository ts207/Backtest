from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.research.compile_strategy_blueprints as compiler
from events.registry import EVENT_REGISTRY_SPECS


def _write_events_csv(root: Path, run_id: str, spec_event_type: str, rows: list[dict]) -> Path:
    spec = EVENT_REGISTRY_SPECS[spec_event_type]
    path = root / "reports" / spec.reports_dir / run_id / spec.events_file
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def test_event_stats_subtype_filters_shared_funding_file(monkeypatch, tmp_path):
    run_id = "r_stats_subtype"
    _write_events_csv(
        tmp_path,
        run_id,
        "FUNDING_EXTREME_ONSET",
        rows=[
            {
                "event_type": "FUNDING_EXTREME_ONSET",
                "time_to_secondary_shock": 10.0,
                "adverse_proxy_excess": 0.2,
                "forward_abs_return_h": 0.3,
            },
            {
                "event_type": "FUNDING_PERSISTENCE_TRIGGER",
                "time_to_secondary_shock": 20.0,
                "adverse_proxy_excess": 0.4,
                "forward_abs_return_h": 0.5,
            },
        ],
    )
    monkeypatch.setattr(compiler, "DATA_ROOT", tmp_path)
    stats = compiler._event_stats(run_id=run_id, event_type="FUNDING_EXTREME_ONSET")
    assert stats["half_life"].tolist() == [10.0]
    assert stats["adverse"].tolist() == [0.2]
    assert stats["favorable"].tolist() == [0.3]


def test_event_stats_canonical_funding_persistence_filters_shared_file(monkeypatch, tmp_path):
    run_id = "r_stats_funding_persistence"
    _write_events_csv(
        tmp_path,
        run_id,
        "FUNDING_PERSISTENCE_TRIGGER",
        rows=[
            {"event_type": "FUNDING_EXTREME_ONSET", "time_to_secondary_shock": 1.0, "adverse_proxy_excess": 0.1, "forward_abs_return_h": 0.1},
            {"event_type": "FUNDING_PERSISTENCE_TRIGGER", "time_to_secondary_shock": 3.0, "adverse_proxy_excess": 0.3, "forward_abs_return_h": 0.3},
            {"event_type": "FUNDING_NORMALIZATION_TRIGGER", "time_to_secondary_shock": 4.0, "adverse_proxy_excess": 0.4, "forward_abs_return_h": 0.4},
        ],
    )
    monkeypatch.setattr(compiler, "DATA_ROOT", tmp_path)
    stats = compiler._event_stats(run_id=run_id, event_type="FUNDING_PERSISTENCE_TRIGGER")
    assert stats["half_life"].tolist() == [3.0]
    assert stats["adverse"].tolist() == [0.3]
    assert stats["favorable"].tolist() == [0.3]
