from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))


def test_state_expansion_bounded(tmp_path):
    run_id = "r_state_bound"
    atlas_dir = tmp_path / "atlas"
    atlas_dir.mkdir()

    pd.DataFrame(
        [
            {
                "template_id": "CL_001@CROSS_VENUE_DESYNC",
                "source_claim_id": "CL_001",
                "concept_id": "C_1",
                "object_type": "event",
                "runtime_event_type": "cross_venue_desync",
                "event_type": "CROSS_VENUE_DESYNC",
                "canonical_event_type": "CROSS_VENUE_DESYNC",
                "canonical_family": "INFORMATION_DESYNC",
                "ontology_in_taxonomy": True,
                "ontology_in_canonical_registry": True,
                "ontology_unknown_templates": [],
                "ontology_source_states": [],
                "ontology_family_states": [],
                "ontology_all_states": [],
                "ontology_spec_hash": "sha256:test",
                "target_spec_path": "spec/events/CROSS_VENUE_DESYNC.yaml",
                "rule_templates": ["desync_repair"],
                "horizons": ["5m"],
                "conditioning": {"vol_regime": []},
                "assets_filter": "*",
                "min_events": 50,
            }
        ]
    ).to_parquet(atlas_dir / "candidate_templates.parquet", index=False)

    spec_dir = tmp_path / "spec" / "events"
    spec_dir.mkdir(parents=True)
    (spec_dir / "CROSS_VENUE_DESYNC.yaml").write_text(
        "event_type: CROSS_VENUE_DESYNC\n"
        "reports_dir: cross_venue_desync\n"
        "events_file: cross_venue_desync_events.csv\n"
        "signal_column: cross_venue_desync_event\n",
        encoding="utf-8",
    )

    lake_dir = tmp_path / "data" / "lake" / "cleaned" / "perp" / "BTCUSDT" / "bars_5m"
    lake_dir.mkdir(parents=True)
    pd.DataFrame({"timestamp": [pd.Timestamp.now()]}).to_parquet(lake_dir / "bars.parquet")

    ctx_dir = tmp_path / "data" / "lake" / "context" / "market_state" / "BTCUSDT"
    ctx_dir.mkdir(parents=True)
    pd.DataFrame({"timestamp": [pd.Timestamp.now()]}).to_parquet(ctx_dir / "5m.parquet")

    custom_state_registry = {
        "defaults": {"state_scope": "source_only", "min_events": 100},
        "states": [
            {"state_id": "S1", "family": "INFORMATION_DESYNC", "source_event_type": "CROSS_VENUE_DESYNC", "state_scope": "source_only", "min_events": 100, "activation_rule": "a"},
            {"state_id": "S2", "family": "INFORMATION_DESYNC", "source_event_type": "CROSS_VENUE_DESYNC", "state_scope": "source_only", "min_events": 100, "activation_rule": "a"},
            {"state_id": "S3", "family": "INFORMATION_DESYNC", "source_event_type": "CROSS_VENUE_DESYNC", "state_scope": "source_only", "min_events": 100, "activation_rule": "a"},
            {"state_id": "S4", "family": "INFORMATION_DESYNC", "source_event_type": "CROSS_VENUE_DESYNC", "state_scope": "source_only", "min_events": 100, "activation_rule": "a"},
            {"state_id": "F1", "family": "INFORMATION_DESYNC", "source_event_type": "LEAD_LAG_BREAK", "state_scope": "family_safe", "min_events": 120, "activation_rule": "f"},
            {"state_id": "F2", "family": "INFORMATION_DESYNC", "source_event_type": "LEAD_LAG_BREAK", "state_scope": "family_safe", "min_events": 120, "activation_rule": "f"},
            {"state_id": "F3", "family": "INFORMATION_DESYNC", "source_event_type": "LEAD_LAG_BREAK", "state_scope": "family_safe", "min_events": 120, "activation_rule": "f"},
        ],
    }

    out_dir = tmp_path / "reports"
    test_args = [
        "generate_candidate_plan.py",
        "--run_id",
        run_id,
        "--symbols",
        "BTCUSDT",
        "--atlas_dir",
        str(atlas_dir),
        "--out_dir",
        str(out_dir),
        "--max_source_states_per_event",
        "3",
        "--max_family_safe_states_per_event",
        "2",
    ]

    import pipelines.research.generate_candidate_plan as gcp

    with patch.dict(os.environ, {"BACKTEST_DATA_ROOT": str(tmp_path / "data")}):
        with patch.object(sys, "argv", test_args):
            mock_project_root = tmp_path / "project"
            with patch.object(gcp, "PROJECT_ROOT", mock_project_root):
                with patch.object(gcp, "DATA_ROOT", tmp_path / "data"):
                    with patch.object(gcp, "ontology_spec_hash", return_value="sha256:test"):
                        with patch.object(gcp, "_load_state_registry", return_value=custom_state_registry):
                            rc = gcp.main()
    assert rc == 0

    rows = [
        json.loads(x)
        for x in (out_dir / "candidate_plan.jsonl").read_text(encoding="utf-8").splitlines()
        if x.strip()
    ]
    assert rows
    # base + 3 source + 2 family_safe = 6 variants
    assert len(rows) == 6
    state_rows = [row for row in rows if row.get("state_id")]
    assert len(state_rows) == 5
    assert sorted({row["state_provenance"] for row in state_rows}) == ["family", "source"]
    assert sorted({row["state_id"] for row in state_rows}) == ["F1", "F2", "S1", "S2", "S3"]
