import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.report import organize_reports


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_organize_reports_builds_run_centric_index(monkeypatch, tmp_path: Path) -> None:
    reports_root = tmp_path / "reports"

    _write(reports_root / "vol_compression_expansion_v1" / "run_a" / "summary.md", "backtest")
    _write(reports_root / "vol_shock_relaxation" / "run_a" / "vol_shock_relaxation_summary.md", "phase1")
    _write(reports_root / "phase2" / "run_a" / "vol_shock_relaxation" / "phase2_summary.md", "phase2")
    _write(reports_root / "vol_compression_expansion_v1" / "run_b" / "summary.md", "backtest_b")

    out_root = reports_root / "by_run"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "organize_reports.py",
            "--reports_root",
            str(reports_root),
            "--out_root",
            str(out_root),
            "--mode",
            "copy",
            "--force",
            "1",
        ],
    )

    assert organize_reports.main() == 0

    assert (out_root / "run_a" / "backtest" / "vol_compression_expansion_v1" / "summary.md").exists()
    assert (out_root / "run_a" / "research" / "vol_shock_relaxation" / "vol_shock_relaxation_summary.md").exists()
    assert (out_root / "run_a" / "research" / "phase2" / "vol_shock_relaxation" / "phase2_summary.md").exists()
    assert (out_root / "run_b" / "backtest" / "vol_compression_expansion_v1" / "summary.md").exists()

    root_readme = (out_root / "README.md").read_text(encoding="utf-8")
    assert "`run_a`" in root_readme
    assert "`run_b`" in root_readme

    manifest = json.loads((out_root / "index_manifest.json").read_text(encoding="utf-8"))
    assert manifest["runs"]["run_a"] == 3
    assert manifest["runs"]["run_b"] == 1
