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
    """
    Build research-focused evaluation and promotion stages.
    Primary Goal: Produce validated blueprints.jsonl.
    """
    stages: List[Tuple[str, Path, List[str]]] = []

    # 1. Blueprint Compilation (The "Final Artifact")
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

    # 2. Strategy Candidate Building (Candidate bundling)
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

    return stages
