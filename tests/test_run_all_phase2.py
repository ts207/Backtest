import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines import run_all


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
