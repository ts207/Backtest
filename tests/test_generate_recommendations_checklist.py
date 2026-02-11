import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import generate_recommendations_checklist


def test_checklist_promote_when_all_gates_pass(monkeypatch, tmp_path: Path) -> None:
    run_id = "run_pass"
    reports_root = tmp_path / "reports"
    summary_path = reports_root / "vol_compression_expansion_v1" / run_id / "summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "total_trades": 25,
                "max_drawdown": -0.05,
                "fee_sensitivity": [{"fee_bps_per_side": 6.0, "net_return": 0.12}],
                "data_quality": {
                    "symbols": {
                        "BTCUSDT": {
                            "pct_missing_ohlcv": {
                                "2024-01": {"pct_missing_ohlcv": 0.0001},
                            }
                        }
                    }
                },
                "stability_checks": {"sign_consistency": 0.8},
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
    summary_path = reports_root / "vol_compression_expansion_v1" / run_id / "summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "total_trades": 3,
                "max_drawdown": -0.30,
                "fee_sensitivity": [{"fee_bps_per_side": 6.0, "net_return": -0.02}],
                "data_quality": {},
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
    assert any("trade count below threshold" in reason for reason in payload["failure_reasons"])
    assert any("stability checks are missing" in reason for reason in payload["failure_reasons"])
