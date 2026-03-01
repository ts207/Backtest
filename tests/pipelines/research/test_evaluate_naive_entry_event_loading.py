from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.research.evaluate_naive_entry as evaluate_naive_entry


def test_load_phase1_events_uses_registry_spec_paths_and_subtype_filter(monkeypatch, tmp_path):
    monkeypatch.setattr(evaluate_naive_entry, "DATA_ROOT", tmp_path)

    run_id = "r_eval"
    events_path = (
        tmp_path
        / "reports"
        / "funding_events"
        / run_id
        / "funding_episode_events.parquet"
    )
    events_path.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {"event_type": "FUNDING_EXTREME_ONSET", "symbol": "BTCUSDT", "enter_ts": "2026-01-01T00:00:00Z"},
            {"event_type": "FUNDING_PERSISTENCE_TRIGGER", "symbol": "BTCUSDT", "enter_ts": "2026-01-01T00:05:00Z"},
        ]
    ).to_parquet(events_path, index=False)

    out = evaluate_naive_entry._load_phase1_events(run_id=run_id, event_type="FUNDING_PERSISTENCE_TRIGGER")
    assert not out.empty
    assert set(out["event_type"].astype(str).unique()) == {"FUNDING_PERSISTENCE_TRIGGER"}
