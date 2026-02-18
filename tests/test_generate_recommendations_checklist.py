import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import generate_recommendations_checklist


def test_checklist_promote_when_all_gates_pass(monkeypatch, tmp_path: Path) -> None:
    run_id = "run_pass"
    reports_root = tmp_path / "reports"
    edge_path = reports_root / "edge_candidates" / run_id / "edge_candidates_normalized.csv"
    edge_path.parent.mkdir(parents=True, exist_ok=True)
    edge_path.write_text(
        "\n".join(
            [
                "run_id,event,candidate_id,status,edge_score,expected_return_proxy,stability_proxy,n_events,source_path",
                f"{run_id},vol_shock_relaxation,c1,PROMOTED,1.2,0.1,0.8,210,x",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    expectancy_path = reports_root / "expectancy" / run_id / "conditional_expectancy.json"
    expectancy_path.parent.mkdir(parents=True, exist_ok=True)
    expectancy_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "expectancy_exists": True,
                "expectancy_evidence": [{"condition": "compression", "horizon_bars": 16}],
            }
        ),
        encoding="utf-8",
    )

    robustness_path = reports_root / "expectancy" / run_id / "conditional_expectancy_robustness.json"
    robustness_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "survivors": [{"condition": "compression", "horizon": 16}],
                "stability_diagnostics": {"pass": True},
                "capacity_diagnostics": {"pass": True},
            }
        ),
        encoding="utf-8",
    )

    out_dir = tmp_path / "out"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "generate_recommendations_checklist.py",
            "--run_id",
            run_id,
            "--reports_root",
            str(reports_root),
            "--runs_root",
            str(tmp_path / "runs"),
            "--out_dir",
            str(out_dir),
        ],
    )

    assert generate_recommendations_checklist.main() == 0

    payload = json.loads((out_dir / "checklist.json").read_text(encoding="utf-8"))
    assert payload["decision"] == "PROMOTE"
    assert all(g["passed"] for g in payload["gates"])


def test_checklist_keep_research_when_gates_fail(monkeypatch, tmp_path: Path) -> None:
    run_id = "run_fail"
    reports_root = tmp_path / "reports"
    edge_path = reports_root / "edge_candidates" / run_id / "edge_candidates_normalized.csv"
    edge_path.parent.mkdir(parents=True, exist_ok=True)
    edge_path.write_text(
        "run_id,event,candidate_id,status,edge_score,expected_return_proxy,stability_proxy,n_events,source_path\n",
        encoding="utf-8",
    )

    expectancy_path = reports_root / "expectancy" / run_id / "conditional_expectancy.json"
    expectancy_path.parent.mkdir(parents=True, exist_ok=True)
    expectancy_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "expectancy_exists": False,
                "expectancy_evidence": [],
            }
        ),
        encoding="utf-8",
    )

    robustness_path = reports_root / "expectancy" / run_id / "conditional_expectancy_robustness.json"
    robustness_path.write_text(json.dumps({"run_id": run_id, "survivors": [], "stability_diagnostics": {"pass": False}, "capacity_diagnostics": {"pass": False}}), encoding="utf-8")

    out_dir = tmp_path / "out"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "generate_recommendations_checklist.py",
            "--run_id",
            run_id,
            "--reports_root",
            str(reports_root),
            "--runs_root",
            str(tmp_path / "runs"),
            "--out_dir",
            str(out_dir),
        ],
    )

    assert generate_recommendations_checklist.main() == 1

    payload = json.loads((out_dir / "checklist.json").read_text(encoding="utf-8"))
    assert payload["decision"] == "KEEP_RESEARCH"
    assert any("edge candidates below threshold" in reason for reason in payload["failure_reasons"])
    assert any("expectancy_exists is false" in reason for reason in payload["failure_reasons"])


def test_checklist_requires_promoted_edges(monkeypatch, tmp_path: Path) -> None:
    run_id = "run_no_promoted"
    reports_root = tmp_path / "reports"
    edge_path = reports_root / "edge_candidates" / run_id / "edge_candidates_normalized.csv"
    edge_path.parent.mkdir(parents=True, exist_ok=True)
    edge_path.write_text(
        "\n".join(
            [
                "run_id,event,candidate_id,status,edge_score,expected_return_proxy,stability_proxy,n_events,source_path",
                f"{run_id},vol_shock_relaxation,c1,DRAFT,1.2,0.1,0.8,210,x",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    expectancy_path = reports_root / "expectancy" / run_id / "conditional_expectancy.json"
    expectancy_path.parent.mkdir(parents=True, exist_ok=True)
    expectancy_path.write_text(
        json.dumps({"run_id": run_id, "expectancy_exists": True, "expectancy_evidence": [{"condition": "x"}]}),
        encoding="utf-8",
    )
    robustness_path = reports_root / "expectancy" / run_id / "conditional_expectancy_robustness.json"
    robustness_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "survivors": [{"condition": "x"}],
                "stability_diagnostics": {"pass": True},
                "capacity_diagnostics": {"pass": True},
            }
        ),
        encoding="utf-8",
    )

    out_dir = tmp_path / "out"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "generate_recommendations_checklist.py",
            "--run_id",
            run_id,
            "--reports_root",
            str(reports_root),
            "--runs_root",
            str(tmp_path / "runs"),
            "--out_dir",
            str(out_dir),
        ],
    )
    assert generate_recommendations_checklist.main() == 1
    payload = json.loads((out_dir / "checklist.json").read_text(encoding="utf-8"))
    assert payload["decision"] == "KEEP_RESEARCH"
    assert any("promoted edge candidates below threshold" in reason for reason in payload["failure_reasons"])
