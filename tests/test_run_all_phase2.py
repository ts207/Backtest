import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines import run_all


def test_run_stage_appends_log_path_only_when_supported(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path)

    script_with_log = tmp_path / "with_log.py"
    script_with_log.write_text("parser.add_argument('--log_path')", encoding="utf-8")
    script_without_log = tmp_path / "without_log.py"
    script_without_log.write_text("print('hello')", encoding="utf-8")

    captured_cmds: list[list[str]] = []

    def _fake_run(cmd: list[str]) -> SimpleNamespace:
        captured_cmds.append(cmd)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(run_all.subprocess, "run", _fake_run)

    assert run_all._run_stage("with", script_with_log, ["--run_id", "x"], "run_x") is True
    assert run_all._run_stage("without", script_without_log, ["--run_id", "x"], "run_x") is True

    assert "--log_path" in captured_cmds[0]
    assert "--log_path" not in captured_cmds[1]


def test_recommendations_checklist_keep_research_is_non_fatal(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path)
    script_without_log = tmp_path / "without_log.py"
    script_without_log.write_text("print('hello')", encoding="utf-8")

    def _fake_run(_cmd: list[str]) -> SimpleNamespace:
        return SimpleNamespace(returncode=1)

    monkeypatch.setattr(run_all.subprocess, "run", _fake_run)

    assert run_all._run_stage("generate_recommendations_checklist", script_without_log, ["--run_id", "x"], "run_x") is True


def test_run_all_includes_phase2_chain_when_enabled(monkeypatch) -> None:
    captured = []

    def _fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, script_path, list(base_args), run_id))
        return True

    monkeypatch.setattr(run_all, "_run_stage", _fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            "run_phase2_chain",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_phase2_conditional",
            "1",
        ],
    )

    assert run_all.main() == 0

    stage_names = [x[0] for x in captured]
    assert "analyze_vol_shock_relaxation" in stage_names
    assert "phase2_conditional_hypotheses" in stage_names
    assert stage_names.index("build_features_v1") < stage_names.index("analyze_vol_shock_relaxation")
    assert stage_names.index("analyze_vol_shock_relaxation") < stage_names.index("phase2_conditional_hypotheses")
    assert stage_names.index("phase2_conditional_hypotheses") < stage_names.index("backtest_vol_compression_v1")

    phase2_args = next(base_args for stage, _, base_args, _ in captured if stage == "phase2_conditional_hypotheses")
    assert "--max_conditions" in phase2_args
    assert "--max_actions" in phase2_args
    assert "--require_phase1_pass" in phase2_args


def test_run_all_includes_recommendations_checklist_by_default(monkeypatch) -> None:
    captured = []

    def _fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append(stage)
        return True

    monkeypatch.setattr(run_all, "_run_stage", _fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            "run_checklist",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
        ],
    )

    assert run_all.main() == 0
    assert "generate_recommendations_checklist" in captured


def test_run_all_can_disable_recommendations_checklist(monkeypatch) -> None:
    captured = []

    def _fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append(stage)
        return True

    monkeypatch.setattr(run_all, "_run_stage", _fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            "run_no_checklist",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_recommendations_checklist",
            "0",
        ],
    )

    assert run_all.main() == 0
    assert "generate_recommendations_checklist" not in captured
