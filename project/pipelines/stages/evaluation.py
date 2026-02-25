from pathlib import Path
from typing import List, Tuple

def build_evaluation_stages(
    args,
    run_id: str,
    symbols: str,
    start: str,
    end: str,
    force_flag: str,
    project_root: Path,
    data_root: Path,
) -> List[Tuple[str, Path, List[str]]]:
    stages: List[Tuple[str, Path, List[str]]] = []

    if int(args.run_strategy_blueprint_compiler):
        promoted_candidates_path = data_root / "reports" / "promotions" / run_id / "promoted_candidates.parquet"
        stages.append(
            (
                "compile_strategy_blueprints",
                project_root / "pipelines" / "research" / "compile_strategy_blueprints.py",
                [
                    "--run_id", run_id,
                    "--symbols", symbols,
                    "--max_per_event", str(int(args.strategy_blueprint_max_per_event)),
                    "--ignore_checklist", str(int(args.strategy_blueprint_ignore_checklist)),
                    "--allow_fallback_blueprints", str(int(args.strategy_blueprint_allow_fallback)),
                    "--allow_non_executable_conditions", str(int(args.strategy_blueprint_allow_non_executable_conditions)),
                    "--allow_naive_entry_fail", str(int(args.strategy_blueprint_allow_naive_entry_fail)),
                    "--min_events_floor", str(int(args.strategy_blueprint_min_events_floor)),
                    "--candidates_file", str(promoted_candidates_path),
                ],
            )
        )

    if int(args.run_strategy_builder):
        stages.append(
            (
                "build_strategy_candidates",
                project_root / "pipelines" / "research" / "build_strategy_candidates.py",
                [
                    "--run_id", run_id,
                    "--symbols", symbols,
                    "--top_k_per_event", str(int(args.strategy_builder_top_k_per_event)),
                    "--max_candidates", str(int(args.strategy_builder_max_candidates)),
                    "--include_alpha_bundle", str(int(args.strategy_builder_include_alpha_bundle)),
                    "--ignore_checklist", str(int(args.strategy_builder_ignore_checklist)),
                    "--allow_non_promoted", str(int(args.strategy_builder_allow_non_promoted)),
                    "--allow_missing_candidate_detail", str(int(args.strategy_builder_allow_missing_candidate_detail)),
                ],
            )
        )

    if int(args.run_backtest):
        stages.append(
            (
                "backtest_strategies",
                project_root / "pipelines" / "backtest" / "backtest_strategies.py",
                [
                    "--run_id", run_id,
                    "--symbols", symbols,
                    "--force", force_flag,
                    "--clean_engine_artifacts", str(int(args.clean_engine_artifacts)),
                ],
            )
        )

    if int(args.run_walkforward_eval):
        stages.append(
            (
                "run_walkforward",
                project_root / "pipelines" / "eval" / "run_walkforward.py",
                [
                    "--run_id", run_id,
                    "--symbols", symbols,
                    "--start", start,
                    "--end", end,
                    "--embargo_days", str(int(args.walkforward_embargo_days)),
                    "--train_frac", str(float(args.walkforward_train_frac)),
                    "--validation_frac", str(float(args.walkforward_validation_frac)),
                    "--regime_max_share", str(float(args.walkforward_regime_max_share)),
                    "--drawdown_cluster_top_frac", str(float(args.walkforward_drawdown_cluster_top_frac)),
                    "--drawdown_tail_q", str(float(args.walkforward_drawdown_tail_q)),
                    "--allow_unexpected_strategy_files", str(int(args.walkforward_allow_unexpected_strategy_files)),
                    "--clean_engine_artifacts", str(int(args.clean_engine_artifacts)),
                    "--force", force_flag,
                ],
            )
        )

    if int(args.run_backtest) and int(args.run_blueprint_promotion):
        stages.append(
            (
                "promote_blueprints",
                project_root / "pipelines" / "research" / "promote_blueprints.py",
                [
                    "--run_id", run_id,
                    "--allow_fallback_evidence", str(int(args.promotion_allow_fallback_evidence)),
                    "--regime_max_share", str(float(args.promotion_regime_max_share)),
                    "--max_loss_cluster_len", str(int(args.promotion_max_loss_cluster_len)),
                    "--max_cluster_loss_concentration", str(float(args.promotion_max_cluster_loss_concentration)),
                    "--min_tail_conditional_drawdown_95", str(float(args.promotion_min_tail_conditional_drawdown_95)),
                    "--max_cost_ratio_train_validation", str(float(args.promotion_max_cost_ratio_train_validation)),
                ],
            )
        )

    if int(args.run_make_report) or int(args.run_backtest):
        stages.append(
            (
                "make_report",
                project_root / "pipelines" / "report" / "make_report.py",
                [
                    "--run_id", run_id,
                    "--allow_backtest_artifact_fallback", str(int(args.report_allow_backtest_artifact_fallback)),
                ],
            )
        )

    return stages
