from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.run_all as run_all


def _arg_value(args: list[str], flag: str) -> str:
    idx = args.index(flag)
    return str(args[idx + 1])


def test_run_all_ohlcv_ingest_respects_force_flag(monkeypatch, tmp_path):
    captured: list[tuple[str, list[str]]] = []

    def fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, list(base_args)))
        return False

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(run_all, "_git_commit", lambda _project_root: "test-sha")
    monkeypatch.setattr(run_all, "_run_stage", fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--force",
            "0",
        ],
    )

    rc = run_all.main()
    assert rc == 1
    assert captured
    stage, args = captured[0]
    assert stage == "ingest_binance_um_ohlcv_5m"
    assert _arg_value(args, "--force") == "0"


def test_run_all_bridge_stage_default_embargo_is_nonzero(monkeypatch, tmp_path):
    captured: list[tuple[str, list[str]]] = []

    def fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, list(base_args)))
        return True

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(run_all, "_git_commit", lambda _project_root: "test-sha")
    monkeypatch.setattr(run_all, "_run_stage", fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--skip_ingest_ohlcv",
            "1",
            "--skip_ingest_funding",
            "1",
            "--skip_ingest_spot_ohlcv",
            "1",
            "--run_hypothesis_generator",
            "0",
            "--run_phase2_conditional",
            "1",
            "--run_bridge_eval_phase2",
            "1",
            "--run_strategy_blueprint_compiler",
            "0",
            "--run_strategy_builder",
            "0",
            "--run_recommendations_checklist",
            "0",
        ],
    )

    rc = run_all.main()
    assert rc == 0
    bridge = [row for row in captured if row[0].startswith("bridge_evaluate_phase2")]
    assert bridge
    _, bridge_args = bridge[0]
    assert _arg_value(bridge_args, "--embargo_days") == "1"


def test_run_all_progress_logs_elapsed_and_eta(monkeypatch, tmp_path, capsys):
    def fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        return False

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path / "data")
    monkeypatch.setattr(run_all, "_git_commit", lambda _project_root: "test-sha")
    monkeypatch.setattr(run_all, "_run_stage", fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
        ],
    )

    rc = run_all.main()
    assert rc == 1
    captured = capsys.readouterr()
    assert "pipeline_elapsed=" in captured.out
    assert "eta~" in captured.out


def test_run_all_declared_subtype_families_are_not_noop():
    expected_scripts = {
        "funding_extreme_onset": "analyze_funding_episode_events.py",
        "funding_persistence_window": "analyze_funding_episode_events.py",
        "funding_normalization": "analyze_funding_episode_events.py",
        "oi_spike_positive": "analyze_oi_shock_events.py",
        "oi_spike_negative": "analyze_oi_shock_events.py",
        "oi_flush": "analyze_oi_shock_events.py",
    }
    chain_map = {event: script for event, script, _ in run_all.PHASE2_EVENT_CHAIN}
    for event_type, expected_script in expected_scripts.items():
        assert chain_map.get(event_type) == expected_script
