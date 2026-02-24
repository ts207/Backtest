from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_debug_scripts_not_in_repo_root():
    for name in [
        "debug_candidates.py",
        "debug_features.py",
        "debug_parquet.py",
        "show_candidates.py",
        "check_diagnostics.py",
        "run_condition_certification.py",
    ]:
        assert not (REPO_ROOT / name).exists(), f"Unexpected root debug script: {name}"


def test_debug_scripts_exist_under_project_scripts_debug():
    debug_dir = REPO_ROOT / "project" / "scripts" / "debug"
    expected = {
        "debug_candidates.py",
        "debug_features.py",
        "debug_parquet.py",
        "show_candidates.py",
        "check_diagnostics.py",
        "run_condition_certification.py",
    }
    found = {p.name for p in debug_dir.glob("*.py")}
    assert expected.issubset(found)
