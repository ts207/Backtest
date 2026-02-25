from pathlib import Path
from typing import List, Tuple
from .utils import script_supports_flag, as_flag

def build_research_stages(
    args,
    run_id: str,
    symbols: str,
    start: str,
    end: str,
    research_gate_profile: str,
    project_root: Path,
    data_root: Path,
    phase2_event_chain: List[Tuple[str, str, List[str]]],
) -> List[Tuple[str, Path, List[str]]]:
    stages: List[Tuple[str, Path, List[str]]] = []

    if int(args.run_hypothesis_generator):
        stages.append(
            (
                "generate_candidate_templates",
                project_root / "pipelines" / "research" / "generate_candidate_templates.py",
                [
                    "--backlog", "research_backlog.csv",
                    "--atlas_dir", "atlas",
                ],
            )
        )
        stages.append(
            (
                "generate_candidate_plan",
                project_root / "pipelines" / "research" / "generate_candidate_plan.py",
                [
                    "--run_id", run_id,
                    "--symbols", symbols,
                    "--atlas_dir", "atlas",
                    "--allow_ontology_hash_mismatch", str(int(args.allow_ontology_hash_mismatch)),
                ],
            )
        )

    if int(args.run_phase2_conditional):
        selected_chain = phase2_event_chain
        if args.phase2_event_type != "all":
            selected_chain = [x for x in phase2_event_chain if x[0] == args.phase2_event_type]

        for event_type, script_name, extra_args in selected_chain:
            phase1_script = project_root / "pipelines" / "research" / script_name
            phase1_args = [
                "--run_id", run_id,
                "--symbols", symbols,
                *extra_args,
            ]
            if script_supports_flag(phase1_script, "--seed"):
                phase1_args.extend(["--seed", str(int(args.seed))])
            if script_name == "analyze_liquidity_vacuum.py":
                phase1_args.extend(
                    [
                        "--profile", "lenient",
                        "--min_events_calibration", "1",
                        "--vol_ratio_floor", "0.95",
                        "--range_multiplier", "1.05",
                        "--min_vacuum_bars", "1",
                        "--shock_quantiles", "0.5,0.6,0.7",
                        "--volume_window", "24",
                        "--range_window", "24",
                    ]
                )
            if script_name == "analyze_liquidation_cascade.py":
                phase1_args.extend(
                    [
                        "--liq_vol_th", str(args.liq_vol_th),
                        "--oi_drop_th", str(args.oi_drop_th),
                    ]
                )
            stages.append((script_name.removesuffix(".py"), phase1_script, phase1_args))

            registry_stage_name = "build_event_registry" if len(selected_chain) == 1 else f"build_event_registry_{event_type}"
            stages.append(
                (
                    registry_stage_name,
                    project_root / "pipelines" / "research" / "build_event_registry.py",
                    [
                        "--run_id", run_id,
                        "--symbols", symbols,
                        "--event_type", event_type,
                        "--timeframe", "5m",
                    ],
                )
            )

            phase2_stage_name = "phase2_conditional_hypotheses" if len(selected_chain) == 1 else f"phase2_conditional_hypotheses_{event_type}"
            candidate_plan_path = data_root / "reports" / "hypothesis_generator" / run_id / "candidate_plan.jsonl"
            phase2_args = [
                "--run_id", run_id,
                "--event_type", event_type,
                "--symbols", symbols,
                "--shift_labels_k", str(int(args.phase2_shift_labels_k)),
                "--mode", str(args.mode),
                "--gate_profile", str(args.phase2_gate_profile),
                "--cost_calibration_mode", str(args.phase2_cost_calibration_mode),
                "--cost_min_tob_coverage", str(float(args.phase2_cost_min_tob_coverage)),
                "--cost_tob_tolerance_minutes", str(int(args.phase2_cost_tob_tolerance_minutes)),
                "--allow_ontology_hash_mismatch", str(int(args.allow_ontology_hash_mismatch)),
            ]
            if int(args.run_hypothesis_generator):
                phase2_args.extend(["--candidate_plan", str(candidate_plan_path)])
                phase2_args.extend(["--atlas_mode", str(int(args.atlas_mode))])
            
            stages.append(
                (
                    phase2_stage_name,
                    project_root / "pipelines" / "research" / "phase2_candidate_discovery.py",
                    phase2_args,
                )
            )

            if int(args.run_bridge_eval_phase2):
                bridge_stage_name = "bridge_evaluate_phase2" if len(selected_chain) == 1 else f"bridge_evaluate_phase2_{event_type}"
                stages.append(
                    (
                        bridge_stage_name,
                        project_root / "pipelines" / "research" / "bridge_evaluate_phase2.py",
                        [
                            "--run_id", run_id,
                            "--event_type", event_type,
                            "--symbols", symbols,
                            "--start", start,
                            "--end", end,
                            "--train_frac", str(float(args.bridge_train_frac)),
                            "--validation_frac", str(float(args.bridge_validation_frac)),
                            "--embargo_days", str(int(args.bridge_embargo_days)),
                            "--edge_cost_k", str(float(args.bridge_edge_cost_k)),
                            "--stressed_cost_multiplier", str(float(args.bridge_stressed_cost_multiplier)),
                            "--min_validation_trades", str(int(args.bridge_min_validation_trades)),
                            "--mode", str(args.mode),
                        ],
                    )
                )

    if int(args.run_phase2_conditional) and int(args.run_discovery_quality_summary):
        stages.append(
            (
                "summarize_discovery_quality",
                project_root / "pipelines" / "research" / "summarize_discovery_quality.py",
                ["--run_id", run_id],
            )
        )

    if int(args.run_naive_entry_eval) and int(args.run_phase2_conditional):
        stages.append(
            (
                "evaluate_naive_entry",
                project_root / "pipelines" / "research" / "evaluate_naive_entry.py",
                [
                    "--run_id", run_id,
                    "--symbols", symbols,
                    "--min_trades", str(int(args.naive_min_trades)),
                    "--min_expectancy_after_cost", str(float(args.naive_min_expectancy_after_cost)),
                    "--max_drawdown", str(float(args.naive_max_drawdown)),
                ],
            )
        )

    if int(args.run_candidate_promotion) and int(args.run_phase2_conditional):
        stages.append(
            (
                "promote_candidates",
                project_root / "pipelines" / "research" / "promote_candidates.py",
                [
                    "--run_id", run_id,
                    "--max_q_value", str(float(args.candidate_promotion_max_q_value)),
                    "--min_events", str(int(args.candidate_promotion_min_events)),
                    "--min_stability_score", str(float(args.candidate_promotion_min_stability_score)),
                    "--min_sign_consistency", str(float(args.candidate_promotion_min_sign_consistency)),
                    "--min_cost_survival_ratio", str(float(args.candidate_promotion_min_cost_survival_ratio)),
                    "--max_negative_control_pass_rate", str(float(args.candidate_promotion_max_negative_control_pass_rate)),
                    "--require_hypothesis_audit", str(int(args.candidate_promotion_require_hypothesis_audit)),
                    "--allow_missing_negative_controls", str(int(args.candidate_promotion_allow_missing_negative_controls)),
                ],
            )
        )

    if int(args.run_edge_registry_update) and int(args.run_candidate_promotion) and int(args.run_phase2_conditional):
        stages.append(
            (
                "update_edge_registry",
                project_root / "pipelines" / "research" / "update_edge_registry.py",
                ["--run_id", run_id],
            )
        )

    if int(args.run_edge_candidate_universe):
        stages.append(
            (
                "export_edge_candidates",
                project_root / "pipelines" / "research" / "export_edge_candidates.py",
                [
                    "--run_id", run_id,
                    "--symbols", symbols,
                    "--execute", "0",
                    "--run_hypothesis_generator", as_flag(args.run_hypothesis_generator),
                    "--hypothesis_datasets", str(args.hypothesis_datasets),
                    "--hypothesis_max_fused", str(int(args.hypothesis_max_fused)),
                ],
            )
        )

    if int(args.run_expectancy_analysis):
        stages.append(
            (
                "analyze_conditional_expectancy",
                project_root / "pipelines" / "research" / "analyze_conditional_expectancy.py",
                ["--run_id", run_id, "--symbols", symbols],
            )
        )

    if int(args.run_expectancy_robustness):
        stages.append(
            (
                "validate_expectancy_traps",
                project_root / "pipelines" / "research" / "validate_expectancy_traps.py",
                [
                    "--run_id", run_id,
                    "--symbols", symbols,
                    "--gate_profile", research_gate_profile,
                ],
            )
        )

    if int(args.run_recommendations_checklist):
        stages.append(
            (
                "generate_recommendations_checklist",
                project_root / "pipelines" / "research" / "generate_recommendations_checklist.py",
                ["--run_id", run_id, "--gate_profile", research_gate_profile],
            )
        )

    return stages
