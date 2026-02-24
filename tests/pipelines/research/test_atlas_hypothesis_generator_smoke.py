from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.research.atlas_hypothesis_generator as atlas_gen


def test_atlas_hypothesis_generator_runs_smoke(tmp_path):
    backlog = pd.DataFrame(
        [
            {
                "claim_id": "CL_001",
                "candidate_type": "event",
                "statement_summary": 'payload {""event_type"": ""LIQUIDITY_VACUUM""}',
                "features": "",
                "next_artifact": "spec/events/{event_type}.yaml",
                "priority_score": 1.0,
                "assets": "BTC",
                "concept_id": "C1",
                "operationalizable": "Y",
                "status": "draft",
            }
        ]
    )
    backlog_path = tmp_path / "research_backlog.csv"
    backlog.to_csv(backlog_path, index=False)

    # Create matching spec so one candidate_plan row can be emitted.
    spec_dir = tmp_path / "spec" / "events"
    spec_dir.mkdir(parents=True)
    (spec_dir / "LIQUIDITY_VACUUM.yaml").write_text("event_type: LIQUIDITY_VACUUM\n", encoding="utf-8")

    out_dir = tmp_path / "out"
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True)

    test_args = [
        "atlas_hypothesis_generator.py",
        "--run_id",
        "r_atlas_smoke",
        "--symbols",
        "BTCUSDT",
        "--backlog",
        str(backlog_path.relative_to(tmp_path)),
        "--out_dir",
        str(out_dir),
    ]

    with patch.dict(os.environ, {"BACKTEST_DATA_ROOT": str(tmp_path / "data")}):
        with patch.object(sys, "argv", test_args):
            with patch.object(atlas_gen, "PROJECT_ROOT", project_root):
                with patch.object(atlas_gen, "DATA_ROOT", tmp_path / "data"):
                    rc = atlas_gen.main()

    assert rc == 0
    assert (out_dir / "candidate_plan.jsonl").exists()
    assert (out_dir / "spec_tasks.parquet").exists() or (out_dir / "spec_tasks.csv").exists()
