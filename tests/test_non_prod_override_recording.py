import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))


def _run_all_with_flags(monkeypatch, tmp_path, extra_argv):
    """Run run_all.main() with all stages mocked out, returning the written manifest."""
    from pipelines import run_all

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(run_all, "_run_stage", lambda stage, script_path, base_args, run_id: True)

    base_argv = [
        "run_all.py",
        "--run_id", "override_test",
        "--symbols", "BTCUSDT",
        "--start", "2024-01-01",
        "--end", "2024-01-02",
    ]
    monkeypatch.setattr(sys, "argv", base_argv + extra_argv)

    rc = run_all.main()
    assert rc == 0
    manifest_path = tmp_path / "runs" / "override_test" / "run_manifest.json"
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def test_no_overrides_when_all_defaults(monkeypatch, tmp_path):
    """With default flags, non_production_overrides must be empty."""
    payload = _run_all_with_flags(monkeypatch, tmp_path, [])
    assert payload["non_production_overrides"] == []


def test_allow_fallback_blueprints_recorded(monkeypatch, tmp_path):
    """--strategy_blueprint_allow_fallback 1 must appear in non_production_overrides."""
    payload = _run_all_with_flags(
        monkeypatch, tmp_path,
        ["--strategy_blueprint_allow_fallback", "1"],
    )
    overrides = payload["non_production_overrides"]
    assert any("allow_fallback_blueprints" in o for o in overrides), (
        f"Expected allow_fallback_blueprints in overrides: {overrides}"
    )


def test_allow_unexpected_strategy_files_recorded(monkeypatch, tmp_path):
    """--walkforward_allow_unexpected_strategy_files 1 must appear in non_production_overrides."""
    payload = _run_all_with_flags(
        monkeypatch, tmp_path,
        ["--walkforward_allow_unexpected_strategy_files", "1"],
    )
    overrides = payload["non_production_overrides"]
    assert any("allow_unexpected_strategy_files" in o for o in overrides), (
        f"Expected allow_unexpected_strategy_files in overrides: {overrides}"
    )


def test_promotion_allow_fallback_evidence_recorded(monkeypatch, tmp_path):
    """--promotion_allow_fallback_evidence 1 must appear in non_production_overrides."""
    payload = _run_all_with_flags(
        monkeypatch, tmp_path,
        ["--promotion_allow_fallback_evidence", "1"],
    )
    overrides = payload["non_production_overrides"]
    assert any("allow_fallback_evidence" in o for o in overrides), (
        f"Expected allow_fallback_evidence in overrides: {overrides}"
    )


def test_allow_naive_entry_fail_recorded(monkeypatch, tmp_path):
    """--strategy_blueprint_allow_naive_entry_fail 1 must appear in non_production_overrides."""
    payload = _run_all_with_flags(
        monkeypatch, tmp_path,
        ["--strategy_blueprint_allow_naive_entry_fail", "1"],
    )
    overrides = payload["non_production_overrides"]
    assert any("allow_naive_entry_fail" in o for o in overrides), (
        f"Expected allow_naive_entry_fail in overrides: {overrides}"
    )
