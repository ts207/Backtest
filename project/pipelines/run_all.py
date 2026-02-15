from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import List, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
PHASE2_EVENT_CHAIN: List[Tuple[str, str, List[str]]] = [
    ("vol_shock_relaxation", "analyze_vol_shock_relaxation.py", ["--timeframe", "15m"]),
    ("liquidity_refill_lag_window", "analyze_liquidity_refill_lag_window.py", []),
    ("liquidity_absence_window", "analyze_liquidity_absence_window.py", []),
    ("vol_aftershock_window", "analyze_vol_aftershock_window.py", []),
    ("directional_exhaustion_after_forced_flow", "analyze_directional_exhaustion_after_forced_flow.py", []),
    ("cross_venue_desync", "analyze_cross_venue_desync.py", []),
    ("liquidity_vacuum", "analyze_liquidity_vacuum.py", ["--timeframe", "15m"]),
    ("funding_extreme_reversal_window", "analyze_funding_extreme_reversal_window.py", []),
    ("range_compression_breakout_window", "analyze_range_compression_breakout_window.py", []),
]
_STRICT_RECOMMENDATIONS_CHECKLIST = False


def _run_id_default() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


@lru_cache(maxsize=None)
def _script_supports_log_path(script_path: Path) -> bool:
    try:
        return "--log_path" in script_path.read_text(encoding="utf-8")
    except OSError:
        return False


@lru_cache(maxsize=None)
def _script_supports_flag(script_path: Path, flag: str) -> bool:
    try:
        return flag in script_path.read_text(encoding="utf-8")
    except OSError:
        return False


def _run_stage(stage: str, script_path: Path, base_args: List[str], run_id: str) -> bool:
    runs_dir = DATA_ROOT / "runs" / run_id
    runs_dir.mkdir(parents=True, exist_ok=True)
    log_path = runs_dir / f"{stage}.log"
    manifest_path = runs_dir / f"{stage}.json"

    cmd = [sys.executable, str(script_path)] + base_args
    if _script_supports_log_path(script_path):
        cmd.extend(["--log_path", str(log_path)])
    result = subprocess.run(cmd)

    allowed_nonzero = {}
    if not _STRICT_RECOMMENDATIONS_CHECKLIST:
        allowed_nonzero["generate_recommendations_checklist"] = {1}
    accepted_codes = {0} | allowed_nonzero.get(stage, set())
    if result.returncode not in accepted_codes:
        print(f"Stage failed: {stage}", file=sys.stderr)
        print(f"Stage log: {log_path}", file=sys.stderr)
        print(f"Stage manifest: {manifest_path}", file=sys.stderr)
        return False
    return True


def _as_flag(value: int) -> str:
    return str(int(value))


def _print_artifact_summary(run_id: str) -> None:
    artifact_paths = [
        ("runs", DATA_ROOT / "runs" / run_id),
        ("phase2", DATA_ROOT / "reports" / "phase2" / run_id),
        ("edge_candidates", DATA_ROOT / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.csv"),
        ("strategy_builder", DATA_ROOT / "reports" / "strategy_builder" / run_id),
        ("report", DATA_ROOT / "reports" / "vol_compression_expansion_v1" / run_id / "summary.md"),
    ]
    print("Artifact summary:")
    print(f"  - BACKTEST_DATA_ROOT: {DATA_ROOT}")
    for label, path in artifact_paths:
        status = "found" if path.exists() else "missing"
        print(f"  - {label} ({status}): {path}")


def _parse_symbols_csv(symbols_csv: str) -> List[str]:
    symbols = [s.strip().upper() for s in str(symbols_csv).split(",") if s.strip()]
    unique_symbols: List[str] = []
    seen = set()
    for symbol in symbols:
        if symbol not in seen:
            unique_symbols.append(symbol)
            seen.add(symbol)
    return unique_symbols


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run discovery-first pipeline for multi-symbol idea generation under one run_id; "
            "validate per-symbol before deployment."
        )
    )
    parser.add_argument("--run_id", required=False)
    parser.add_argument(
        "--symbols",
        required=True,
        help=(
            "Comma-separated symbols for a single discovery run_id "
            "(example: BTCUSDT,ETHUSDT,SOLUSDT)."
        ),
    )
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--skip_ingest_ohlcv", type=int, default=0)
    parser.add_argument("--skip_ingest_funding", type=int, default=0)
    parser.add_argument("--skip_ingest_spot_ohlcv", type=int, default=0)
    parser.add_argument("--enable_cross_venue_spot_pipeline", type=int, default=1)
    parser.add_argument("--allow_missing_funding", type=int, default=0)
    parser.add_argument("--allow_constant_funding", type=int, default=0)
    parser.add_argument("--allow_funding_timestamp_rounding", type=int, default=0)
    parser.add_argument("--config", action="append", default=[])

    parser.add_argument("--run_ingest_liquidation_snapshot", type=int, default=0)
    parser.add_argument("--run_ingest_open_interest_hist", type=int, default=0)
    parser.add_argument("--open_interest_period", default="5m")

    parser.add_argument("--run_hypothesis_generator", type=int, default=0)
    parser.add_argument("--hypothesis_datasets", default="auto")
    parser.add_argument("--hypothesis_max_fused", type=int, default=24)

    parser.add_argument("--run_phase2_conditional", type=int, default=0)
    parser.add_argument(
        "--phase2_event_type",
        default="vol_shock_relaxation",
        choices=["all", *[event for event, _, _ in PHASE2_EVENT_CHAIN]],
    )
    parser.add_argument("--phase2_max_conditions", type=int, default=20)
    parser.add_argument("--phase2_max_actions", type=int, default=9)
    parser.add_argument("--phase2_min_regime_stable_splits", type=int, default=2)
    parser.add_argument("--phase2_require_phase1_pass", type=int, default=1)

    parser.add_argument("--run_edge_candidate_universe", type=int, default=0)
    parser.add_argument("--run_strategy_builder", type=int, default=1)
    parser.add_argument("--strategy_builder_top_k_per_event", type=int, default=2)
    parser.add_argument("--strategy_builder_max_candidates", type=int, default=20)
    parser.add_argument("--strategy_builder_include_alpha_bundle", type=int, default=1)
    parser.add_argument("--run_expectancy_analysis", type=int, default=0)
    parser.add_argument("--run_expectancy_robustness", type=int, default=0)
    parser.add_argument("--run_recommendations_checklist", type=int, default=1)
    parser.add_argument("--strict_recommendations_checklist", type=int, default=0)
    parser.add_argument("--seed", type=int, default=13)
    parser.add_argument("--run_backtest", type=int, default=0)
    parser.add_argument("--run_make_report", type=int, default=0)
    parser.add_argument("--fees_bps", type=float, default=None)
    parser.add_argument("--slippage_bps", type=float, default=None)
    parser.add_argument("--cost_bps", type=float, default=None)
    parser.add_argument("--strategies", default=None)
    parser.add_argument("--overlays", default="")

    args = parser.parse_args()
    global _STRICT_RECOMMENDATIONS_CHECKLIST
    _STRICT_RECOMMENDATIONS_CHECKLIST = bool(int(args.strict_recommendations_checklist))

    if int(args.run_backtest) and not (args.strategies and str(args.strategies).strip()):
        print("run_backtest requires --strategies (comma-separated strategy ids).", file=sys.stderr)
        return 1

    run_id = args.run_id or _run_id_default()
    parsed_symbols = _parse_symbols_csv(args.symbols)
    if not parsed_symbols:
        print("--symbols must include at least one symbol (comma-separated).", file=sys.stderr)
        return 1
    symbols = ",".join(parsed_symbols)
    start = args.start
    end = args.end
    force_flag = _as_flag(args.force)
    allow_missing_funding_flag = _as_flag(args.allow_missing_funding)
    allow_constant_funding_flag = _as_flag(args.allow_constant_funding)
    allow_funding_timestamp_rounding_flag = _as_flag(args.allow_funding_timestamp_rounding)

    cross_venue_requested = bool(
        int(args.run_phase2_conditional)
        and args.phase2_event_type in {"all", "cross_venue_desync"}
    )
    run_spot_pipeline = bool(int(args.enable_cross_venue_spot_pipeline) and cross_venue_requested)

    stages: List[Tuple[str, Path, List[str]]] = []

    if not args.skip_ingest_ohlcv:
        stages.append(
            (
                "ingest_binance_um_ohlcv_15m",
                PROJECT_ROOT / "pipelines" / "ingest" / "ingest_binance_um_ohlcv_15m.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--start",
                    start,
                    "--end",
                    end,
                    "--force",
                    force_flag,
                ],
            )
        )

    if not args.skip_ingest_funding:
        stages.append(
            (
                "ingest_binance_um_funding",
                PROJECT_ROOT / "pipelines" / "ingest" / "ingest_binance_um_funding.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--start",
                    start,
                    "--end",
                    end,
                    "--force",
                    force_flag,
                ],
            )
        )

    if run_spot_pipeline and not args.skip_ingest_spot_ohlcv:
        stages.append(
            (
                "ingest_binance_spot_ohlcv_15m",
                PROJECT_ROOT / "pipelines" / "ingest" / "ingest_binance_spot_ohlcv_15m.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--start",
                    start,
                    "--end",
                    end,
                    "--force",
                    force_flag,
                ],
            )
        )

    if int(args.run_ingest_liquidation_snapshot):
        stages.append(
            (
                "ingest_binance_um_liquidation_snapshot",
                PROJECT_ROOT / "pipelines" / "ingest" / "ingest_binance_um_liquidation_snapshot.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--start",
                    start,
                    "--end",
                    end,
                    "--force",
                    force_flag,
                ],
            )
        )

    if int(args.run_ingest_open_interest_hist):
        stages.append(
            (
                "ingest_binance_um_open_interest_hist",
                PROJECT_ROOT / "pipelines" / "ingest" / "ingest_binance_um_open_interest_hist.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--start",
                    start,
                    "--end",
                    end,
                    "--period",
                    str(args.open_interest_period),
                    "--force",
                    force_flag,
                ],
            )
        )

    stages.extend(
        [
            (
                "build_cleaned_15m",
                PROJECT_ROOT / "pipelines" / "clean" / "build_cleaned_15m.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--start",
                    start,
                    "--end",
                    end,
                    "--force",
                    force_flag,
                    "--allow_missing_funding",
                    allow_missing_funding_flag,
                    "--allow_constant_funding",
                    allow_constant_funding_flag,
                    "--allow_funding_timestamp_rounding",
                    allow_funding_timestamp_rounding_flag,
                ],
            ),
            (
                "build_features_v1",
                PROJECT_ROOT / "pipelines" / "features" / "build_features_v1.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--force",
                    force_flag,
                    "--allow_missing_funding",
                    allow_missing_funding_flag,
                ],
            ),
            (
                "build_universe_snapshots",
                PROJECT_ROOT / "pipelines" / "ingest" / "build_universe_snapshots.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--market",
                    "perp",
                    "--force",
                    force_flag,
                ],
            ),
            (
                "build_context_features",
                PROJECT_ROOT / "pipelines" / "features" / "build_context_features.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--timeframe",
                    "15m",
                    "--start",
                    start,
                    "--end",
                    end,
                    "--force",
                    force_flag,
                ],
            ),
            (
                "build_market_context",
                PROJECT_ROOT / "pipelines" / "features" / "build_market_context.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--timeframe",
                    "15m",
                    "--start",
                    start,
                    "--end",
                    end,
                    "--force",
                    force_flag,
                ],
            ),
        ]
    )

    if run_spot_pipeline:
        stages.extend(
            [
                (
                    "build_cleaned_15m_spot",
                    PROJECT_ROOT / "pipelines" / "clean" / "build_cleaned_15m.py",
                    [
                        "--run_id",
                        run_id,
                        "--symbols",
                        symbols,
                        "--market",
                        "spot",
                        "--start",
                        start,
                        "--end",
                        end,
                        "--force",
                        force_flag,
                    ],
                ),
                (
                    "build_features_v1_spot",
                    PROJECT_ROOT / "pipelines" / "features" / "build_features_v1.py",
                    [
                        "--run_id",
                        run_id,
                        "--symbols",
                        symbols,
                        "--market",
                        "spot",
                        "--force",
                        force_flag,
                    ],
                ),
            ]
        )

    if int(args.run_hypothesis_generator):
        stages.append(
            (
                "generate_hypothesis_queue",
                PROJECT_ROOT / "pipelines" / "research" / "generate_hypothesis_queue.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--datasets",
                    str(args.hypothesis_datasets),
                    "--max_fused",
                    str(int(args.hypothesis_max_fused)),
                ],
            )
        )

    if int(args.run_phase2_conditional):
        selected_chain = PHASE2_EVENT_CHAIN
        if args.phase2_event_type != "all":
            selected_chain = [x for x in PHASE2_EVENT_CHAIN if x[0] == args.phase2_event_type]

        for event_type, script_name, extra_args in selected_chain:
            phase1_script = PROJECT_ROOT / "pipelines" / "research" / script_name
            phase1_args = [
                "--run_id",
                run_id,
                "--symbols",
                symbols,
                *extra_args,
            ]
            if _script_supports_flag(phase1_script, "--seed"):
                phase1_args.extend(["--seed", str(int(args.seed))])
            stages.append(
                (
                    script_name.removesuffix(".py"),
                    phase1_script,
                    phase1_args,
                )
            )
            phase2_stage_name = (
                "phase2_conditional_hypotheses"
                if len(selected_chain) == 1
                else f"phase2_conditional_hypotheses_{event_type}"
            )
            stages.append(
                (
                    phase2_stage_name,
                    PROJECT_ROOT / "pipelines" / "research" / "phase2_conditional_hypotheses.py",
                    [
                        "--run_id",
                        run_id,
                        "--event_type",
                        event_type,
                        "--symbols",
                        symbols,
                        "--max_conditions",
                        str(int(args.phase2_max_conditions)),
                        "--max_actions",
                        str(int(args.phase2_max_actions)),
                        "--min_regime_stable_splits",
                        str(int(args.phase2_min_regime_stable_splits)),
                        "--require_phase1_pass",
                        _as_flag(args.phase2_require_phase1_pass),
                        "--seed",
                        str(int(args.seed)),
                    ],
                )
            )

    if int(args.run_edge_candidate_universe):
        stages.append(
            (
                "export_edge_candidates",
                PROJECT_ROOT / "pipelines" / "research" / "export_edge_candidates.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--execute",
                    "0",
                    "--run_hypothesis_generator",
                    _as_flag(args.run_hypothesis_generator),
                    "--hypothesis_datasets",
                    str(args.hypothesis_datasets),
                    "--hypothesis_max_fused",
                    str(int(args.hypothesis_max_fused)),
                ],
            )
        )

    if int(args.run_expectancy_analysis):
        stages.append(
            (
                "analyze_conditional_expectancy",
                PROJECT_ROOT / "pipelines" / "research" / "analyze_conditional_expectancy.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                ],
            )
        )

    if int(args.run_expectancy_robustness):
        stages.append(
            (
                "validate_expectancy_traps",
                PROJECT_ROOT / "pipelines" / "research" / "validate_expectancy_traps.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                ],
            )
        )

    if int(args.run_recommendations_checklist):
        stages.append(
            (
                "generate_recommendations_checklist",
                PROJECT_ROOT / "pipelines" / "research" / "generate_recommendations_checklist.py",
                ["--run_id", run_id],
            )
        )

    if int(args.run_strategy_builder):
        stages.append(
            (
                "build_strategy_candidates",
                PROJECT_ROOT / "pipelines" / "research" / "build_strategy_candidates.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--top_k_per_event",
                    str(int(args.strategy_builder_top_k_per_event)),
                    "--max_candidates",
                    str(int(args.strategy_builder_max_candidates)),
                    "--include_alpha_bundle",
                    str(int(args.strategy_builder_include_alpha_bundle)),
                ],
            )
        )

    if int(args.run_backtest):
        stages.append(
            (
                "backtest_strategies",
                PROJECT_ROOT / "pipelines" / "backtest" / "backtest_strategies.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--force",
                    force_flag,
                ],
            )
        )

    if int(args.run_make_report) or int(args.run_backtest):
        stages.append(
            (
                "make_report",
                PROJECT_ROOT / "pipelines" / "report" / "make_report.py",
                ["--run_id", run_id],
            )
        )

    stages_with_config = {
        "build_cleaned_15m",
        "build_features_v1",
        "build_context_features",
        "build_market_context",
        "backtest_strategies",
        "make_report",
    }
    for stage_name, _, base_args in stages:
        if stage_name in stages_with_config:
            for config_path in args.config:
                base_args.extend(["--config", config_path])
        if stage_name == "backtest_strategies":
            if args.fees_bps is not None:
                base_args.extend(["--fees_bps", str(args.fees_bps)])
            if args.slippage_bps is not None:
                base_args.extend(["--slippage_bps", str(args.slippage_bps)])
            if args.cost_bps is not None:
                base_args.extend(["--cost_bps", str(args.cost_bps)])
            if args.strategies is not None:
                base_args.extend(["--strategies", str(args.strategies)])
            if args.overlays:
                base_args.extend(["--overlays", str(args.overlays)])

    stage_timings: List[Tuple[str, float]] = []
    print(f"Planned stages: {len(stages)}")
    for idx, (stage, script, base_args) in enumerate(stages, start=1):
        print(f"[{idx}/{len(stages)}] Starting stage: {stage}")
        started = time.perf_counter()
        ok = _run_stage(stage, script, base_args, run_id)
        elapsed_sec = time.perf_counter() - started
        stage_timings.append((stage, elapsed_sec))
        print(f"[{idx}/{len(stages)}] Finished stage: {stage} ({elapsed_sec:.1f}s)")
        if not ok:
            print("Slow-stage summary before failure:")
            for stage_name, duration in sorted(stage_timings, key=lambda x: x[1], reverse=True):
                print(f"  - {stage_name}: {duration:.1f}s")
            return 1

    print("Stage timing summary (slowest first):")
    for stage_name, duration in sorted(stage_timings, key=lambda x: x[1], reverse=True):
        print(f"  - {stage_name}: {duration:.1f}s")

    print(f"Pipeline run completed: {run_id}")
    _print_artifact_summary(run_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
