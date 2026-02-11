from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))

PHASE1_ANALYZERS = {
    "vol_shock_relaxation": (
        "analyze_vol_shock_relaxation",
        PROJECT_ROOT / "pipelines" / "research" / "analyze_vol_shock_relaxation.py",
        ["--timeframe", "15m"],
    ),
    "directional_exhaustion_after_forced_flow": (
        "analyze_directional_exhaustion_after_forced_flow",
        PROJECT_ROOT / "pipelines" / "research" / "analyze_directional_exhaustion_after_forced_flow.py",
        [],
    ),
    "liquidity_refill_lag_window": (
        "analyze_liquidity_refill_lag_window",
        PROJECT_ROOT / "pipelines" / "research" / "analyze_liquidity_refill_lag_window.py",
        [],
    ),
}


def _run_id_default() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


@lru_cache(maxsize=None)
def _script_supports_log_path(script_path: Path) -> bool:
    try:
        return "--log_path" in script_path.read_text(encoding="utf-8")
    except OSError:
        return False


def _git_commit_hash() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT.parent).decode().strip()
    except Exception:
        return "unknown"


def _run_stage(stage: str, script_path: Path, base_args: List[str], run_id: str) -> bool:
    runs_dir = DATA_ROOT / "runs" / run_id
    runs_dir.mkdir(parents=True, exist_ok=True)
    log_path = runs_dir / f"{stage}.log"
    manifest_path = runs_dir / f"{stage}.json"

    cmd = [sys.executable, str(script_path)] + base_args
    if _script_supports_log_path(script_path):
        cmd.extend(["--log_path", str(log_path)])

    result = subprocess.run(cmd)
    allowed_nonzero = {"generate_recommendations_checklist": {1}}
    accepted_codes = {0} | allowed_nonzero.get(stage, set())
    if result.returncode not in accepted_codes:
        print(f"Stage failed: {stage}", file=sys.stderr)
        print(f"Stage log: {log_path}", file=sys.stderr)
        print(f"Stage manifest: {manifest_path}", file=sys.stderr)
        return False
    return True


def _print_execution_plan(workflow: str, stages: List[Tuple[str, Path, List[str]]], run_id: str) -> None:
    print(f"Workflow: {workflow}")
    print(f"Run ID: {run_id}")
    print("Execution order:")
    for idx, (name, _, _) in enumerate(stages, start=1):
        print(f"  {idx:02d}. {name}")


def _write_run_meta(run_id: str, args: argparse.Namespace) -> Path:
    runs_dir = DATA_ROOT / "runs" / run_id
    runs_dir.mkdir(parents=True, exist_ok=True)
    path = runs_dir / "run_meta.json"
    payload = {
        "run_id": run_id,
        "git_commit": _git_commit_hash(),
        "workflow": args.workflow,
        "symbols": args.symbols,
        "start": args.start,
        "end": args.end,
        "phase2_event_type": args.phase2_event_type if int(args.run_phase2_conditional) else None,
        "audit_cost": {
            "fee_bps_per_side": float(args.promoted_edge_audit_fee_bps),
            "spread_bps_per_side": float(args.promoted_edge_audit_spread_bps),
            "top_n": int(args.promoted_edge_audit_top_n),
            "horizon_bars": int(args.promoted_edge_audit_horizon_bars),
        },
        "config_paths": list(args.config),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def _verify_run_contract(run_id: str, args: argparse.Namespace) -> bool:
    verify_script = PROJECT_ROOT.parent / "scripts" / "verify_run_contract.sh"
    if not verify_script.exists():
        print("verify_run_contract.sh not found; skipping")
        return True
    cmd = [
        str(verify_script),
        run_id,
        args.workflow,
        args.phase2_event_type,
    ]
    if int(args.run_promoted_edge_audits):
        cmd.append("--expect-audits")
    result = subprocess.run(cmd)
    return result.returncode == 0


def _core_prereq_stages(args: argparse.Namespace, run_id: str) -> List[Tuple[str, Path, List[str]]]:
    stages: List[Tuple[str, Path, List[str]]] = []
    force_flag = str(int(args.force))
    if not args.skip_ingest_ohlcv:
        stages.append(
            (
                "ingest_binance_um_ohlcv_15m",
                PROJECT_ROOT / "pipelines" / "ingest" / "ingest_binance_um_ohlcv_15m.py",
                ["--run_id", run_id, "--symbols", args.symbols, "--start", args.start, "--end", args.end, "--force", force_flag],
            )
        )
    if not args.skip_ingest_funding:
        stages.append(
            (
                "ingest_binance_um_funding",
                PROJECT_ROOT / "pipelines" / "ingest" / "ingest_binance_um_funding.py",
                ["--run_id", run_id, "--symbols", args.symbols, "--start", args.start, "--end", args.end, "--force", force_flag],
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
                    args.symbols,
                    "--start",
                    args.start,
                    "--end",
                    args.end,
                    "--force",
                    force_flag,
                    "--allow_missing_funding",
                    str(int(args.allow_missing_funding)),
                    "--allow_constant_funding",
                    str(int(args.allow_constant_funding)),
                    "--allow_funding_timestamp_rounding",
                    str(int(args.allow_funding_timestamp_rounding)),
                ],
            ),
            (
                "build_features_v1",
                PROJECT_ROOT / "pipelines" / "features" / "build_features_v1.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    args.symbols,
                    "--force",
                    force_flag,
                    "--allow_missing_funding",
                    str(int(args.allow_missing_funding)),
                ],
            ),
            (
                "build_context_features",
                PROJECT_ROOT / "pipelines" / "features" / "build_context_features.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    args.symbols,
                    "--timeframe",
                    "15m",
                    "--start",
                    args.start,
                    "--end",
                    args.end,
                    "--force",
                    force_flag,
                ],
            ),
        ]
    )
    return stages


def _core_post_stages(args: argparse.Namespace, run_id: str) -> List[Tuple[str, Path, List[str]]]:
    force_flag = str(int(args.force))
    return [
        (
            "backtest_vol_compression_v1",
            PROJECT_ROOT / "pipelines" / "backtest" / "backtest_vol_compression_v1.py",
            ["--run_id", run_id, "--symbols", args.symbols, "--force", force_flag],
        ),
        (
            "make_report",
            PROJECT_ROOT / "pipelines" / "report" / "make_report.py",
            ["--run_id", run_id],
        ),
    ]


def _research_stages(args: argparse.Namespace, run_id: str) -> List[Tuple[str, Path, List[str]]]:
    stages: List[Tuple[str, Path, List[str]]] = []

    if int(args.run_phase1_aftershock):
        stages.append(
            (
                "analyze_vol_aftershock_window",
                PROJECT_ROOT / "pipelines" / "research" / "analyze_vol_aftershock_window.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    args.symbols,
                    "--window_start",
                    str(args.aftershock_window_start),
                    "--window_end",
                    str(args.aftershock_window_end),
                ],
            )
        )

    if int(args.run_phase2_conditional):
        phase1_name, phase1_script, phase1_extra = PHASE1_ANALYZERS[args.phase2_event_type]
        stages.extend(
            [
                (phase1_name, phase1_script, ["--run_id", run_id, "--symbols", args.symbols, *phase1_extra]),
                (
                    "phase2_conditional_hypotheses",
                    PROJECT_ROOT / "pipelines" / "research" / "phase2_conditional_hypotheses.py",
                    [
                        "--run_id",
                        run_id,
                        "--event_type",
                        args.phase2_event_type,
                        "--symbols",
                        args.symbols,
                        "--max_conditions",
                        str(args.phase2_max_conditions),
                        "--max_actions",
                        str(args.phase2_max_actions),
                        "--bootstrap_iters",
                        str(args.phase2_bootstrap_iters),
                        "--cost_floor",
                        str(args.phase2_cost_floor),
                        "--require_phase1_pass",
                        str(int(args.phase2_require_phase1_pass)),
                    ],
                ),
            ]
        )

    if int(args.run_recommendations_checklist):
        stages.append(
            (
                "generate_recommendations_checklist",
                PROJECT_ROOT / "pipelines" / "research" / "generate_recommendations_checklist.py",
                ["--run_id", run_id],
            )
        )

    if int(args.run_promoted_edge_audits):
        stages.append(
            (
                "run_promoted_edge_audits",
                PROJECT_ROOT / "pipelines" / "research" / "run_promoted_edge_audits.py",
                [
                    "--run_id",
                    run_id,
                    "--top_n",
                    str(int(args.promoted_edge_audit_top_n)),
                    "--horizon_bars",
                    str(int(args.promoted_edge_audit_horizon_bars)),
                    "--fee_bps_per_side",
                    str(float(args.promoted_edge_audit_fee_bps)),
                    "--spread_bps_per_side",
                    str(float(args.promoted_edge_audit_spread_bps)),
                ],
            )
        )
    return stages


def _apply_configs_and_backtest_overrides(stages: List[Tuple[str, Path, List[str]]], args: argparse.Namespace) -> None:
    stages_with_config = {
        "build_cleaned_15m",
        "build_features_v1",
        "build_context_features",
        "backtest_vol_compression_v1",
        "make_report",
    }
    for stage_name, _, base_args in stages:
        if stage_name in stages_with_config:
            for config_path in args.config:
                base_args.extend(["--config", config_path])
        if stage_name == "backtest_vol_compression_v1":
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Run pipeline workflows")
    parser.add_argument("--run_id", required=False)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--workflow", default="core", choices=["core", "research", "full"])
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--skip_ingest_ohlcv", type=int, default=0)
    parser.add_argument("--skip_ingest_funding", type=int, default=0)
    parser.add_argument("--allow_missing_funding", type=int, default=0)
    parser.add_argument("--allow_constant_funding", type=int, default=0)
    parser.add_argument("--allow_funding_timestamp_rounding", type=int, default=0)
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--fees_bps", type=float, default=None)
    parser.add_argument("--slippage_bps", type=float, default=None)
    parser.add_argument("--cost_bps", type=float, default=None)
    parser.add_argument("--strategies", default=None)
    parser.add_argument("--overlays", default="")
    parser.add_argument("--run_phase2_conditional", type=int, default=0)
    parser.add_argument(
        "--phase2_event_type",
        default="vol_shock_relaxation",
        choices=list(PHASE1_ANALYZERS.keys()),
    )
    parser.add_argument("--phase2_max_conditions", type=int, default=20)
    parser.add_argument("--phase2_max_actions", type=int, default=9)
    parser.add_argument("--phase2_bootstrap_iters", type=int, default=1000)
    parser.add_argument("--phase2_cost_floor", type=float, default=0.01)
    parser.add_argument("--phase2_require_phase1_pass", type=int, default=1)
    parser.add_argument("--run_phase1_aftershock", type=int, default=0)
    parser.add_argument("--aftershock_window_start", type=int, default=0)
    parser.add_argument("--aftershock_window_end", type=int, default=96)
    parser.add_argument("--run_recommendations_checklist", type=int, default=1)
    parser.add_argument("--run_promoted_edge_audits", type=int, default=0)
    parser.add_argument("--promoted_edge_audit_top_n", type=int, default=3)
    parser.add_argument("--promoted_edge_audit_horizon_bars", type=int, default=1)
    parser.add_argument("--promoted_edge_audit_fee_bps", type=float, default=8.0)
    parser.add_argument("--promoted_edge_audit_spread_bps", type=float, default=2.0)
    parser.add_argument("--verify_contract", type=int, default=0)
    args = parser.parse_args()

    run_id = args.run_id or _run_id_default()

    if args.workflow in {"research", "full"}:
        args.run_phase2_conditional = 1

    prereq = _core_prereq_stages(args, run_id)
    core_post = _core_post_stages(args, run_id)
    research = _research_stages(args, run_id)

    if args.workflow == "core":
        stages = prereq + core_post
    elif args.workflow == "research":
        stages = prereq + research
    else:
        stages = prereq + core_post + research

    _apply_configs_and_backtest_overrides(stages, args)

    _print_execution_plan(args.workflow, stages, run_id)
    meta_path = _write_run_meta(run_id, args)
    print(f"Run metadata written: {meta_path}")

    for stage, script, base_args in stages:
        if not _run_stage(stage, script, base_args, run_id):
            return 1

    if int(args.verify_contract) and not _verify_run_contract(run_id, args):
        return 1

    print(
        "Artifact contract check command: "
        f"bash scripts/verify_run_contract.sh {run_id} {args.workflow} {args.phase2_event_type}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
