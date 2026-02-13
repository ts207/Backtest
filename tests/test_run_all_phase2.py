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


def test_recommendations_checklist_keep_research_is_fatal_when_strict(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path)
    script_without_log = tmp_path / "without_log.py"
    script_without_log.write_text("print('hello')", encoding="utf-8")

    def _fake_run(_cmd: list[str]) -> SimpleNamespace:
        return SimpleNamespace(returncode=1)

    monkeypatch.setattr(run_all.subprocess, "run", _fake_run)
    monkeypatch.setattr(run_all, "_STRICT_RECOMMENDATIONS_CHECKLIST", True)
    assert run_all._run_stage("generate_recommendations_checklist", script_without_log, ["--run_id", "x"], "run_x") is False


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
    assert stage_names.index("phase2_conditional_hypotheses") < stage_names.index("generate_recommendations_checklist")
    assert stage_names.index("generate_recommendations_checklist") < stage_names.index("build_strategy_candidates")

    phase2_args = next(base_args for stage, _, base_args, _ in captured if stage == "phase2_conditional_hypotheses")
    assert "--max_conditions" in phase2_args
    assert "--max_actions" in phase2_args
    assert "--require_phase1_pass" in phase2_args


def test_run_all_passes_seed_to_seeded_research_stages(monkeypatch) -> None:
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
            "run_seed_passthrough",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_phase2_conditional",
            "1",
            "--phase2_event_type",
            "vol_shock_relaxation",
            "--seed",
            "99",
        ],
    )

    assert run_all.main() == 0
    phase1_args = next(base_args for stage, _, base_args, _ in captured if stage == "analyze_vol_shock_relaxation")
    phase2_args = next(base_args for stage, _, base_args, _ in captured if stage == "phase2_conditional_hypotheses")
    assert "--seed" in phase1_args and "99" in phase1_args
    assert "--seed" in phase2_args and "99" in phase2_args


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
    assert "build_strategy_candidates" in captured


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
    assert "build_strategy_candidates" in captured


def test_run_all_can_disable_strategy_builder(monkeypatch) -> None:
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
            "run_no_strategy_builder",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_strategy_builder",
            "0",
        ],
    )

    assert run_all.main() == 0
    assert "build_strategy_candidates" not in captured


def test_run_all_includes_backtest_and_report_when_enabled(monkeypatch) -> None:
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
            "run_with_backtest",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_backtest",
            "1",
            "--run_make_report",
            "1",
            "--fees_bps",
            "3",
            "--slippage_bps",
            "1",
            "--cost_bps",
            "4",
            "--strategies",
            "vol_compression_v1",
            "--overlays",
            "funding_extreme_filter",
        ],
    )

    assert run_all.main() == 0
    stage_names = [x[0] for x in captured]
    assert "backtest_strategies" in stage_names
    assert "make_report" in stage_names
    assert stage_names.index("backtest_strategies") < stage_names.index("make_report")

    backtest_args = next(base_args for stage, _, base_args, _ in captured if stage == "backtest_strategies")
    assert "--fees_bps" in backtest_args and "3.0" in backtest_args
    assert "--slippage_bps" in backtest_args and "1.0" in backtest_args
    assert "--cost_bps" in backtest_args and "4.0" in backtest_args
    assert "--strategies" in backtest_args and "vol_compression_v1" in backtest_args
    assert "--overlays" in backtest_args and "funding_extreme_filter" in backtest_args


def test_run_all_omits_backtest_by_default(monkeypatch) -> None:
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
            "run_without_backtest",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
        ],
    )

    assert run_all.main() == 0
    assert "backtest_strategies" not in captured
    assert "make_report" not in captured


def test_run_all_requires_strategies_when_backtest_enabled(monkeypatch) -> None:
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
            "run_backtest_missing_strategies",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_backtest",
            "1",
        ],
    )

    assert run_all.main() == 1
    assert captured == []


def test_run_all_auto_adds_spot_pipeline_for_cross_venue(monkeypatch) -> None:
    captured: list[str] = []

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
            "run_cross_venue_spot_auto",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_phase2_conditional",
            "1",
            "--phase2_event_type",
            "cross_venue_desync",
        ],
    )

    assert run_all.main() == 0
    assert "ingest_binance_spot_ohlcv_15m" in captured
    assert "build_cleaned_15m_spot" in captured
    assert "build_features_v1_spot" in captured


def test_run_all_can_disable_spot_ingest_for_cross_venue(monkeypatch) -> None:
    captured: list[str] = []

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
            "run_cross_venue_no_spot_ingest",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_phase2_conditional",
            "1",
            "--phase2_event_type",
            "cross_venue_desync",
            "--skip_ingest_spot_ohlcv",
            "1",
        ],
    )

    assert run_all.main() == 0
    assert "ingest_binance_spot_ohlcv_15m" not in captured
    assert "build_cleaned_15m_spot" in captured
    assert "build_features_v1_spot" in captured
