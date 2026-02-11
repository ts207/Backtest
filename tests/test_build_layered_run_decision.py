from __future__ import annotations

import json
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research.build_layered_run_decision import build_layered_decision


def test_layered_decision_marks_global_not_assessed_for_single_strategy(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))

    run_id = "r1"
    checklist_dir = tmp_path / "runs" / run_id / "research_checklist"
    checklist_dir.mkdir(parents=True, exist_ok=True)
    (checklist_dir / "checklist.json").write_text(
        json.dumps({"decision": "KEEP_RESEARCH", "failure_reasons": ["x"]}),
        encoding="utf-8",
    )

    # provide one promoted research family so research layer is non-empty
    phase2_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    phase2_dir.mkdir(parents=True, exist_ok=True)
    (phase2_dir / "promoted_candidates.json").write_text(
        json.dumps([
            {"event": "vol_shock_relaxation", "candidate_id": "vol_shock_relaxation_0"},
            {"event": "vol_shock_relaxation", "candidate_id": "vol_shock_relaxation_1"},
        ]),
        encoding="utf-8",
    )

    payload = build_layered_decision(run_id=run_id, out_dir=tmp_path / "reports" / "run_decisions" / run_id)

    assert payload["research_layer"]["decision"] == "RESEARCH_PROMOTIONS_PRESENT"
    assert payload["strategy_layer"]["strategy_count"] == 1
    assert payload["global_decision"] == "GLOBAL_DEPLOYABILITY_NOT_ASSESSED_SINGLE_STRATEGY"


def test_layered_decision_research_inconclusive_when_no_phase2(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))

    run_id = "r2"
    payload = build_layered_decision(run_id=run_id, out_dir=tmp_path / "reports" / "run_decisions" / run_id)

    assert payload["research_layer"]["decision"] == "RESEARCH_INCONCLUSIVE"
    assert payload["research_layer"]["promoted_candidate_count"] == 0
