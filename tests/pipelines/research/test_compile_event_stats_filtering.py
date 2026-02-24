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
        "funding_extreme_onset",
        rows=[
            {
                "event_type": "funding_extreme_onset",
                "time_to_secondary_shock": 10.0,
                "adverse_proxy_excess": 0.2,
                "forward_abs_return_h": 0.3,
            },
            {
                "event_type": "funding_persistence_window",
                "time_to_secondary_shock": 20.0,
                "adverse_proxy_excess": 0.4,
                "forward_abs_return_h": 0.5,
            },
        ],
    )
    monkeypatch.setattr(compiler, "DATA_ROOT", tmp_path)
    stats = compiler._event_stats(run_id=run_id, event_type="funding_extreme_onset")
    assert stats["half_life"].tolist() == [10.0]
    assert stats["adverse"].tolist() == [0.2]
    assert stats["favorable"].tolist() == [0.3]


def test_event_stats_aggregate_funding_union_includes_all_children(monkeypatch, tmp_path):
    run_id = "r_stats_funding_union"
    _write_events_csv(
        tmp_path,
        run_id,
        "funding_episodes",
        rows=[
            {"event_type": "funding_extreme_onset", "time_to_secondary_shock": 1.0, "adverse_proxy_excess": 0.1, "forward_abs_return_h": 0.1},
            {"event_type": "funding_acceleration", "time_to_secondary_shock": 2.0, "adverse_proxy_excess": 0.2, "forward_abs_return_h": 0.2},
            {"event_type": "funding_persistence_window", "time_to_secondary_shock": 3.0, "adverse_proxy_excess": 0.3, "forward_abs_return_h": 0.3},
            {"event_type": "funding_normalization", "time_to_secondary_shock": 4.0, "adverse_proxy_excess": 0.4, "forward_abs_return_h": 0.4},
            {"event_type": "oi_spike_positive", "time_to_secondary_shock": 99.0, "adverse_proxy_excess": 9.9, "forward_abs_return_h": 9.9},
        ],
    )
    monkeypatch.setattr(compiler, "DATA_ROOT", tmp_path)
    stats = compiler._event_stats(run_id=run_id, event_type="funding_episodes")
    assert sorted(stats["half_life"].tolist()) == [1.0, 2.0, 3.0, 4.0]
    assert sorted(stats["adverse"].tolist()) == [0.1, 0.2, 0.3, 0.4]
    assert sorted(stats["favorable"].tolist()) == [0.1, 0.2, 0.3, 0.4]
