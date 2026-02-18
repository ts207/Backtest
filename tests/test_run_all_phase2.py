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
    assert stage_names.index("generate_recommendations_checklist") < stage_names.index("compile_strategy_blueprints")
    assert stage_names.index("compile_strategy_blueprints") < stage_names.index("build_strategy_candidates")
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
    assert "compile_strategy_blueprints" in captured
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
    assert "compile_strategy_blueprints" in captured
    assert "build_strategy_candidates" not in captured


def test_run_all_can_disable_strategy_blueprint_compiler(monkeypatch) -> None:
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
            "run_no_blueprint_compiler",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_strategy_blueprint_compiler",
            "0",
        ],
    )

    assert run_all.main() == 0
    assert "compile_strategy_blueprints" not in captured


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
    assert "promote_blueprints" in stage_names
    assert "make_report" in stage_names
    assert stage_names.index("backtest_strategies") < stage_names.index("make_report")
    assert stage_names.index("backtest_strategies") < stage_names.index("promote_blueprints")
    assert stage_names.index("promote_blueprints") < stage_names.index("make_report")

    backtest_args = next(base_args for stage, _, base_args, _ in captured if stage == "backtest_strategies")
    assert "--fees_bps" in backtest_args and "3.0" in backtest_args
    assert "--slippage_bps" in backtest_args and "1.0" in backtest_args
    assert "--cost_bps" in backtest_args and "4.0" in backtest_args
    assert "--strategies" in backtest_args and "vol_compression_v1" in backtest_args
    assert "--overlays" in backtest_args and "funding_extreme_filter" in backtest_args
    promote_args = next(base_args for stage, _, base_args, _ in captured if stage == "promote_blueprints")
    assert "--allow_fallback_evidence" in promote_args
    assert promote_args[promote_args.index("--allow_fallback_evidence") + 1] == "0"
    report_args = next(base_args for stage, _, base_args, _ in captured if stage == "make_report")
    assert "--allow_backtest_artifact_fallback" in report_args
    assert report_args[report_args.index("--allow_backtest_artifact_fallback") + 1] == "0"


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
    assert "promote_blueprints" not in captured
    assert "make_report" not in captured


def test_run_all_can_disable_blueprint_promotion(monkeypatch) -> None:
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
            "run_disable_promotion",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_backtest",
            "1",
            "--run_blueprint_promotion",
            "0",
            "--strategies",
            "vol_compression_v1",
        ],
    )

    assert run_all.main() == 0
    assert "backtest_strategies" in captured
    assert "promote_blueprints" not in captured


def test_run_all_requires_execution_source_when_backtest_enabled_and_compiler_off(monkeypatch) -> None:
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
            "--run_strategy_blueprint_compiler",
            "0",
        ],
    )

    assert run_all.main() == 1
    assert captured == []


def test_run_all_accepts_blueprint_path_when_backtest_enabled(monkeypatch) -> None:
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
            "run_backtest_blueprint_mode",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_backtest",
            "1",
            "--blueprints_path",
            "data/reports/strategy_blueprints/run/blueprints.jsonl",
        ],
    )

    assert run_all.main() == 0
    assert "backtest_strategies" in captured


def test_run_all_auto_infers_blueprints_path_for_execution(monkeypatch) -> None:
    captured: list[tuple[str, list[str]]] = []

    def _fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, list(base_args)))
        return True

    monkeypatch.setattr(run_all, "_run_stage", _fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            "run_auto_bp_path",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_backtest",
            "1",
            "--run_walkforward_eval",
            "1",
        ],
    )

    assert run_all.main() == 0
    expected_path = str(run_all.DATA_ROOT / "reports" / "strategy_blueprints" / "run_auto_bp_path" / "blueprints.jsonl")
    backtest_args = next(base_args for stage, base_args in captured if stage == "backtest_strategies")
    walkforward_args = next(base_args for stage, base_args in captured if stage == "run_walkforward")
    assert "--blueprints_path" in backtest_args
    assert backtest_args[backtest_args.index("--blueprints_path") + 1] == expected_path
    assert "--blueprints_path" in walkforward_args
    assert walkforward_args[walkforward_args.index("--blueprints_path") + 1] == expected_path


def test_run_all_includes_walkforward_stage_when_enabled(monkeypatch) -> None:
    captured = []

    def _fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, list(base_args)))
        return True

    monkeypatch.setattr(run_all, "_run_stage", _fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            "run_wf_enabled",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_walkforward_eval",
            "1",
            "--strategies",
            "vol_compression_v1",
            "--walkforward_embargo_days",
            "2",
            "--walkforward_train_frac",
            "0.65",
            "--walkforward_validation_frac",
            "0.2",
        ],
    )

    assert run_all.main() == 0
    stage_names = [stage for stage, _ in captured]
    assert "run_walkforward" in stage_names
    wf_args = next(base_args for stage, base_args in captured if stage == "run_walkforward")
    assert "--embargo_days" in wf_args and "2" in wf_args
    assert "--train_frac" in wf_args and "0.65" in wf_args
    assert "--validation_frac" in wf_args and "0.2" in wf_args


def test_run_all_passes_config_to_walkforward(monkeypatch) -> None:
    captured = []

    def _fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, list(base_args)))
        return True

    monkeypatch.setattr(run_all, "_run_stage", _fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            "run_wf_cfg_passthrough",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_walkforward_eval",
            "1",
            "--strategies",
            "vol_compression_v1",
            "--config",
            "configs/pipeline.yaml",
            "--config",
            "configs/fees.yaml",
        ],
    )

    assert run_all.main() == 0
    wf_args = next(base_args for stage, base_args in captured if stage == "run_walkforward")
    assert wf_args.count("--config") == 2
    assert "configs/pipeline.yaml" in wf_args
    assert "configs/fees.yaml" in wf_args


def test_run_all_orders_walkforward_before_promotion_when_both_enabled(monkeypatch) -> None:
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
            "run_wf_before_promotion",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_backtest",
            "1",
            "--run_walkforward_eval",
            "1",
            "--run_blueprint_promotion",
            "1",
            "--strategies",
            "vol_compression_v1",
        ],
    )

    assert run_all.main() == 0
    assert "run_walkforward" in captured
    assert "promote_blueprints" in captured
    assert captured.index("run_walkforward") < captured.index("promote_blueprints")


def test_run_all_passes_promotion_fallback_flag(monkeypatch) -> None:
    captured: list[tuple[str, list[str]]] = []

    def _fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, list(base_args)))
        return True

    monkeypatch.setattr(run_all, "_run_stage", _fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            "run_promotion_fallback_flag",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_backtest",
            "1",
            "--strategies",
            "vol_compression_v1",
            "--promotion_allow_fallback_evidence",
            "1",
        ],
    )

    assert run_all.main() == 0
    promote_args = next(base_args for stage, base_args in captured if stage == "promote_blueprints")
    assert "--allow_fallback_evidence" in promote_args
    assert promote_args[promote_args.index("--allow_fallback_evidence") + 1] == "1"


def test_run_all_passes_report_fallback_flag(monkeypatch) -> None:
    captured: list[tuple[str, list[str]]] = []

    def _fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, list(base_args)))
        return True

    monkeypatch.setattr(run_all, "_run_stage", _fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            "run_report_fallback_flag",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_backtest",
            "1",
            "--strategies",
            "vol_compression_v1",
            "--report_allow_backtest_artifact_fallback",
            "1",
        ],
    )

    assert run_all.main() == 0
    report_args = next(base_args for stage, base_args in captured if stage == "make_report")
    assert "--allow_backtest_artifact_fallback" in report_args
    assert report_args[report_args.index("--allow_backtest_artifact_fallback") + 1] == "1"


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


def test_run_all_normalizes_multi_symbol_csv(monkeypatch) -> None:
    captured = []

    def _fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, list(base_args)))
        return True

    monkeypatch.setattr(run_all, "_run_stage", _fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            "run_symbols_norm",
            "--symbols",
            " btcusdt ,ETHUSDT,btcusdt ",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
        ],
    )

    assert run_all.main() == 0
    first_stage_args = captured[0][1]
    assert "--symbols" in first_stage_args
    symbols_value = first_stage_args[first_stage_args.index("--symbols") + 1]
    assert symbols_value == "BTCUSDT,ETHUSDT"


def test_run_all_passes_strategy_preparation_strictness_flags(monkeypatch) -> None:
    captured: list[tuple[str, list[str]]] = []

    def _fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, list(base_args)))
        return True

    monkeypatch.setattr(run_all, "_run_stage", _fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            "run_strategy_prep_flags",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--strategy_blueprint_ignore_checklist",
            "1",
            "--strategy_blueprint_allow_fallback",
            "1",
            "--strategy_blueprint_allow_non_executable_conditions",
            "1",
            "--strategy_builder_ignore_checklist",
            "1",
            "--strategy_builder_allow_non_promoted",
            "1",
            "--strategy_builder_allow_missing_candidate_detail",
            "1",
        ],
    )

    assert run_all.main() == 0
    blueprint_args = next(base_args for stage, base_args in captured if stage == "compile_strategy_blueprints")
    assert "--ignore_checklist" in blueprint_args and "1" in blueprint_args
    assert "--allow_fallback_blueprints" in blueprint_args and "1" in blueprint_args
    assert "--allow_non_executable_conditions" in blueprint_args and "1" in blueprint_args

    builder_args = next(base_args for stage, base_args in captured if stage == "build_strategy_candidates")
    assert "--ignore_checklist" in builder_args and "1" in builder_args
    assert "--allow_non_promoted" in builder_args and "1" in builder_args
    assert "--allow_missing_candidate_detail" in builder_args and "1" in builder_args
