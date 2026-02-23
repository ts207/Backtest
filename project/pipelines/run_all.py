from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.spec_utils import get_spec_hashes


DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
PHASE2_EVENT_CHAIN: List[Tuple[str, str, List[str]]] = [
    ("vol_shock_relaxation", "analyze_vol_shock_relaxation.py", ["--timeframe", "5m"]),
    ("liquidity_refill_lag_window", "analyze_liquidity_refill_lag_window.py", []),
    ("liquidity_absence_window", "analyze_liquidity_absence_window.py", []),
    ("vol_aftershock_window", "analyze_vol_aftershock_window.py", []),
    ("directional_exhaustion_after_forced_flow", "analyze_directional_exhaustion_after_forced_flow.py", []),
    ("cross_venue_desync", "analyze_cross_venue_desync.py", []),
    ("liquidity_vacuum", "analyze_liquidity_vacuum.py", ["--timeframe", "5m"]),
    ("funding_extreme_reversal_window", "analyze_funding_extreme_reversal_window.py", []),
    ("range_compression_breakout_window", "analyze_range_compression_breakout_window.py", []),
    ("funding_episodes", "analyze_funding_episode_events.py", []),
    ("funding_extreme_onset", "no_op.py", []),
    ("funding_persistence_window", "no_op.py", []),
    ("funding_normalization", "no_op.py", []),
    ("oi_shocks", "analyze_oi_shock_events.py", []),
    ("oi_spike_positive", "no_op.py", []),
    ("oi_spike_negative", "no_op.py", []),
    ("oi_flush", "no_op.py", []),
    ("LIQUIDATION_CASCADE", "analyze_liquidation_cascade.py", []),
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


def _checklist_json_path(run_id: str) -> Path:
    return DATA_ROOT / "runs" / run_id / "research_checklist" / "checklist.json"


def _load_checklist_decision(run_id: str) -> str | None:
    path = _checklist_json_path(run_id)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[warn] failed to load checklist JSON at {path}: {e}", file=sys.stderr)
        return None
    if not isinstance(payload, dict):
        return None
    decision = str(payload.get("decision", "")).strip().upper()
    return decision or None


def _upsert_cli_flag(base_args: List[str], flag: str, value: str) -> None:
    try:
        idx = base_args.index(flag)
    except ValueError:
        base_args.extend([flag, value])
        return
    if idx + 1 < len(base_args):
        base_args[idx + 1] = value
    else:
        base_args.append(value)


def _as_flag(value: int) -> str:
    return str(int(value))


def _print_artifact_summary(run_id: str) -> None:
    artifact_paths = [
        ("runs", DATA_ROOT / "runs" / run_id),
        ("phase2", DATA_ROOT / "reports" / "phase2" / run_id),
        ("phase2_quality_summary", DATA_ROOT / "reports" / "phase2" / run_id / "discovery_quality_summary.json"),
        ("bridge_eval", DATA_ROOT / "reports" / "bridge_eval" / run_id),
        ("edge_candidates", DATA_ROOT / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.csv"),
        ("promotions", DATA_ROOT / "reports" / "promotions" / run_id / "promotion_report.json"),
        ("strategy_builder", DATA_ROOT / "reports" / "strategy_builder" / run_id),
        ("report", DATA_ROOT / "reports" / "vol_compression_expansion_v1" / run_id / "summary.md"),
    ]
    print("Artifact summary:")
    print(f"  - BACKTEST_DATA_ROOT: {DATA_ROOT}")
    for label, path in artifact_paths:
        status = "found" if path.exists() else "missing"
        print(f"  - {label} ({status}): {path}")


def _parse_symbols_csv(symbols_csv: str) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in str(symbols_csv).split(","):
        symbol = raw.strip().upper()
        if symbol and symbol not in seen:
            out.append(symbol)
            seen.add(symbol)
    return out


def _default_blueprints_path(run_id: str) -> Path:
    return DATA_ROOT / "reports" / "strategy_blueprints" / run_id / "blueprints.jsonl"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _git_commit(project_root: Path) -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(project_root), "rev-parse", "HEAD"],
            text=True,
        ).strip()
    except Exception as e:
        print(f"[warn] failed to resolve git commit hash: {e}", file=sys.stderr)
        return "unknown"


def _config_digest(config_paths: List[str]) -> str:
    chunks: List[str] = []
    for raw_path in config_paths:
        path = Path(raw_path)
        if not path.is_absolute():
            path = PROJECT_ROOT / raw_path
        if path.exists():
            try:
                payload = path.read_text(encoding="utf-8")
            except Exception as e:
                print(f"[warn] failed to read config file {path}: {e}", file=sys.stderr)
                payload = ""
            chunks.append(f"{path}:{payload}")
        else:
            chunks.append(f"{path}:<missing>")
    return _sha256_text("\n".join(chunks))


def _feature_schema_metadata() -> tuple[str, str]:
    schema_path = PROJECT_ROOT / "schemas" / "feature_schema_v1.json"
    if not schema_path.exists():
        return "unknown", _sha256_text("")
    try:
        payload = json.loads(schema_path.read_text(encoding="utf-8"))
        version = str(payload.get("version", "feature_schema_v1"))
    except Exception:
        version = "feature_schema_v1"
    schema_hash = hashlib.sha256(schema_path.read_bytes()).hexdigest()
    return version, schema_hash


def _data_hash(symbols: List[str]) -> str:
    roots: List[Path] = []
    for symbol in symbols:
        roots.append(DATA_ROOT / "lake" / "raw" / "binance" / "perp" / symbol)
        roots.append(DATA_ROOT / "lake" / "raw" / "binance" / "spot" / symbol)

    entries: List[str] = []
    for root in roots:
        if not root.exists():
            continue
        for path in sorted([p for p in root.rglob("*") if p.is_file()]):
            try:
                stat = path.stat()
            except OSError:
                continue
            entries.append(
                "|".join(
                    [
                        str(path.relative_to(DATA_ROOT)),
                        str(int(stat.st_size)),
                        str(int(stat.st_mtime_ns)),
                    ]
                )
            )
    return _sha256_text("\n".join(entries))


def _write_run_manifest(run_id: str, payload: dict) -> None:
    out_dir = DATA_ROOT / "runs" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "run_manifest.json"
    
    if manifest_path.exists():
        try:
            existing = json.loads(manifest_path.read_text(encoding="utf-8"))
            for k, v in existing.items():
                if k not in payload or payload[k] is None:
                    payload[k] = v
                elif isinstance(v, dict) and isinstance(payload[k], dict):
                    merged = dict(v)
                    merged.update(payload[k])
                    payload[k] = merged
        except Exception:
            pass

    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _claim_map_hash(project_root: Path) -> str:
    path = project_root / "claim_test_map.csv"
    if not path.exists():
        return _sha256_text("")
    return hashlib.sha256(path.read_bytes()).hexdigest()

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
        choices=["all", *[event for event, _, _ in PHASE2_EVENT_CHAIN], "LIQUIDATION_CASCADE"],
    )
    parser.add_argument("--phase2_max_conditions", type=int, default=20)
    parser.add_argument("--phase2_max_actions", type=int, default=9)
    parser.add_argument("--phase2_min_regime_stable_splits", type=int, default=2)
    parser.add_argument("--phase2_require_phase1_pass", type=int, default=1)
    parser.add_argument("--phase2_min_ess", type=float, default=150.0)
    parser.add_argument("--phase2_ess_max_lag", type=int, default=24)
    parser.add_argument("--phase2_multiplicity_k", type=float, default=1.0)
    parser.add_argument("--phase2_parameter_curvature_max_penalty", type=float, default=0.50)
    parser.add_argument("--phase2_delay_grid_bars", default="0,4,8,16,30")
    parser.add_argument("--phase2_min_delay_positive_ratio", type=float, default=0.60)
    parser.add_argument("--phase2_min_delay_robustness_score", type=float, default=0.60)
    parser.add_argument("--phase2_shift_labels_k", type=int, default=0)
    parser.add_argument("--run_bridge_eval_phase2", type=int, default=1)
    parser.add_argument("--bridge_edge_cost_k", type=float, default=2.0)
    parser.add_argument("--bridge_stressed_cost_multiplier", type=float, default=1.5)
    parser.add_argument("--bridge_min_validation_trades", type=int, default=20)
    parser.add_argument("--bridge_train_frac", type=float, default=0.6)
    parser.add_argument("--bridge_validation_frac", type=float, default=0.2)
    parser.add_argument("--bridge_embargo_days", type=int, default=0)
    parser.add_argument("--run_discovery_quality_summary", type=int, default=1)

    parser.add_argument("--run_edge_candidate_universe", type=int, default=0)
    parser.add_argument("--run_naive_entry_eval", type=int, default=1)
    parser.add_argument("--naive_min_trades", type=int, default=100)
    parser.add_argument("--naive_min_expectancy_after_cost", type=float, default=0.0)
    parser.add_argument("--naive_max_drawdown", type=float, default=-0.25)
    parser.add_argument("--run_strategy_blueprint_compiler", type=int, default=1)
    parser.add_argument("--strategy_blueprint_max_per_event", type=int, default=2)
    parser.add_argument("--strategy_blueprint_ignore_checklist", type=int, default=0)
    parser.add_argument("--strategy_blueprint_allow_fallback", type=int, default=0)
    parser.add_argument("--strategy_blueprint_allow_non_executable_conditions", type=int, default=0)
    parser.add_argument("--strategy_blueprint_allow_naive_entry_fail", type=int, default=0)
    parser.add_argument("--strategy_blueprint_min_events_floor", type=int, default=100)
    parser.add_argument("--run_strategy_builder", type=int, default=1)
    parser.add_argument("--strategy_builder_top_k_per_event", type=int, default=2)
    parser.add_argument("--strategy_builder_max_candidates", type=int, default=20)
    parser.add_argument("--strategy_builder_include_alpha_bundle", type=int, default=1)
    parser.add_argument("--strategy_builder_ignore_checklist", type=int, default=0)
    parser.add_argument("--strategy_builder_allow_non_promoted", type=int, default=0)
    parser.add_argument("--strategy_builder_allow_missing_candidate_detail", type=int, default=0)
    parser.add_argument("--run_expectancy_analysis", type=int, default=0)
    parser.add_argument("--run_expectancy_robustness", type=int, default=0)
    parser.add_argument("--run_recommendations_checklist", type=int, default=1)
    parser.add_argument("--strict_recommendations_checklist", type=int, default=0)
    parser.add_argument("--auto_continue_on_keep_research", type=int, default=0)
    parser.add_argument("--liq_vol_th", type=float, default=100000.0)
    parser.add_argument("--oi_drop_th", type=float, default=-500000.0)
    parser.add_argument("--seed", type=int, default=13)
    parser.add_argument("--run_backtest", type=int, default=0)
    parser.add_argument("--backtest_timeframe", default="5m", help="Bar timeframe for backtest/walkforward data loading (e.g. '5m', '15m')")
    parser.add_argument("--clean_engine_artifacts", type=int, default=1)
    parser.add_argument("--run_blueprint_promotion", type=int, default=1)
    parser.add_argument("--promotion_allow_fallback_evidence", type=int, default=0)
    parser.add_argument("--run_walkforward_eval", type=int, default=0)
    parser.add_argument("--walkforward_allow_unexpected_strategy_files", type=int, default=0)
    parser.add_argument("--walkforward_embargo_days", type=int, default=1)  # B2: default 1, never 0
    parser.add_argument("--walkforward_train_frac", type=float, default=0.6)
    parser.add_argument("--walkforward_validation_frac", type=float, default=0.2)
    parser.add_argument("--walkforward_regime_max_share", type=float, default=0.80)
    parser.add_argument("--walkforward_drawdown_cluster_top_frac", type=float, default=0.10)
    parser.add_argument("--walkforward_drawdown_tail_q", type=float, default=0.05)
    parser.add_argument("--promotion_regime_max_share", type=float, default=0.80)
    parser.add_argument("--promotion_max_loss_cluster_len", type=int, default=64)
    parser.add_argument("--promotion_max_cluster_loss_concentration", type=float, default=0.50)
    parser.add_argument("--promotion_min_tail_conditional_drawdown_95", type=float, default=-0.20)
    parser.add_argument("--promotion_max_cost_ratio_train_validation", type=float, default=0.60)
    parser.add_argument("--report_allow_backtest_artifact_fallback", type=int, default=0)
    parser.add_argument("--run_make_report", type=int, default=0)
    parser.add_argument("--fees_bps", type=float, default=None)
    parser.add_argument("--slippage_bps", type=float, default=None)
    parser.add_argument("--cost_bps", type=float, default=None)
    parser.add_argument("--strategies", default=None)
    parser.add_argument("--overlays", default="")
    parser.add_argument("--blueprints_path", default=None)
    parser.add_argument("--blueprints_top_k", type=int, default=10)
    parser.add_argument("--blueprints_filter_event_type", default="all")
    parser.add_argument("--atlas_mode", type=int, default=0)
    parser.add_argument("--mode", choices=["research", "production", "certification"], default="research")

    args = parser.parse_args()
    
    # B1: Unconditional ban — fallback blueprints bypass BH-FDR and can never appear in evaluation.
    if args.strategy_blueprint_allow_fallback:
        print(
            "EVALUATION GUARD [INV_NO_FALLBACK_IN_MEASUREMENT]: "
            "--strategy_blueprint_allow_fallback=1 is unconditionally banned. "
            "Fallback blueprints bypass BH-FDR and cannot appear in evaluation artifacts. "
            "Remediation: set spec/gates.yaml gate_v1_fallback.promotion_eligible_regardless_of_fdr: false.",
            file=sys.stderr,
        )
        return 1

    # Mode-based validation
    if args.mode in {"production", "certification"}:
        if args.strategy_blueprint_allow_naive_entry_fail or args.strategy_builder_allow_non_promoted:
             print(f"Error: Fallback/override flags are strictly forbidden in {args.mode} mode.", file=sys.stderr)
             return 1
        if args.strategy_blueprint_ignore_checklist or args.strategy_builder_ignore_checklist:
             print(f"Error: Checklist overrides are strictly forbidden in {args.mode} mode.", file=sys.stderr)
             return 1

    global _STRICT_RECOMMENDATIONS_CHECKLIST
    _STRICT_RECOMMENDATIONS_CHECKLIST = bool(int(args.strict_recommendations_checklist))

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
    effective_strategies = str(args.strategies).strip() if args.strategies and str(args.strategies).strip() else None
    effective_blueprints_path = (
        str(args.blueprints_path).strip() if args.blueprints_path and str(args.blueprints_path).strip() else None
    )
    execution_requested = bool(int(args.run_backtest) or int(args.run_walkforward_eval))
    if execution_requested:
        if effective_strategies and effective_blueprints_path:
            print("Execution requires only one of --strategies or --blueprints_path.", file=sys.stderr)
            return 1
        if not effective_strategies and not effective_blueprints_path:
            inferred_blueprints = _default_blueprints_path(run_id)
            if int(args.run_strategy_blueprint_compiler) or inferred_blueprints.exists():
                effective_blueprints_path = str(inferred_blueprints)
            else:
                print(
                    "Execution requires --strategies or --blueprints_path "
                    "(or enable blueprint compiler / provide existing blueprint artifact).",
                    file=sys.stderr,
                )
                return 1

    cross_venue_requested = bool(
        int(args.run_phase2_conditional)
        and args.phase2_event_type in {"all", "cross_venue_desync"}
    )
    run_spot_pipeline = bool(int(args.enable_cross_venue_spot_pipeline) and cross_venue_requested)

    stages: List[Tuple[str, Path, List[str]]] = []

    if not args.skip_ingest_ohlcv:
        stages.append(
            (
                "ingest_binance_um_ohlcv_5m",
                PROJECT_ROOT / "pipelines" / "ingest" / "ingest_binance_um_ohlcv_5m.py",
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
                    "1", # Forced to 1 for debugging
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
                "ingest_binance_spot_ohlcv_5m",
                PROJECT_ROOT / "pipelines" / "ingest" / "ingest_binance_spot_ohlcv_5m.py",
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
                "build_cleaned_5m",
                PROJECT_ROOT / "pipelines" / "clean" / "build_cleaned_5m.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--market",
                    "perp",
                    "--start",
                    start,
                    "--end",
                    end,
                    "--force",
                    force_flag,
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
                    "5m",
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
                    "5m",
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
                    "build_cleaned_5m_spot",
                    PROJECT_ROOT / "pipelines" / "clean" / "build_cleaned_5m.py",
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
        # Stage 1: Global Candidate Template Generation (Knowledge Atlas)
        stages.append(
            (
                "generate_candidate_templates",
                PROJECT_ROOT / "pipelines" / "research" / "generate_candidate_templates.py",
                [
                    "--backlog", "research_backlog.csv",
                    "--atlas_dir", "atlas",
                ],
            )
        )
        # Stage 2: Per-Run Candidate Plan Enumeration (Bounded & Budgeted)
        stages.append(
            (
                "generate_candidate_plan",
                PROJECT_ROOT / "pipelines" / "research" / "generate_candidate_plan.py",
                [
                    "--run_id", run_id,
                    "--symbols", symbols,
                    "--atlas_dir", "atlas",
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
            stages.append(
                (
                    script_name.removesuffix(".py"),
                    phase1_script,
                    phase1_args,
                )
            )
            registry_stage_name = (
                "build_event_registry"
                if len(selected_chain) == 1
                else f"build_event_registry_{event_type}"
            )
            stages.append(
                (
                    registry_stage_name,
                    PROJECT_ROOT / "pipelines" / "research" / "build_event_registry.py",
                    [
                        "--run_id",
                        run_id,
                        "--symbols",
                        symbols,
                        "--event_type",
                        event_type,
                        "--timeframe",
                        "5m",
                    ],
                )
            )
            phase2_stage_name = (
                "phase2_conditional_hypotheses"
                if len(selected_chain) == 1
                else f"phase2_conditional_hypotheses_{event_type}"
            )
            candidate_plan_path = DATA_ROOT / "reports" / "hypothesis_generator" / run_id / "candidate_plan.jsonl"
            phase2_args = [
                "--run_id",
                run_id,
                "--event_type",
                event_type,
                "--symbols",
                symbols,
                "--shift_labels_k",
                str(int(args.phase2_shift_labels_k)),
            ]
            if candidate_plan_path.exists():
                phase2_args.extend(["--candidate_plan", str(candidate_plan_path)])
                phase2_args.extend(["--atlas_mode", str(int(args.atlas_mode))])
            
            stages.append(
                (
                    phase2_stage_name,
                    PROJECT_ROOT / "pipelines" / "research" / "phase2_candidate_discovery.py",
                    phase2_args,
                )
            )
            if int(args.run_bridge_eval_phase2):
                bridge_stage_name = (
                    "bridge_evaluate_phase2"
                    if len(selected_chain) == 1
                    else f"bridge_evaluate_phase2_{event_type}"
                )
                stages.append(
                    (
                        bridge_stage_name,
                        PROJECT_ROOT / "pipelines" / "research" / "bridge_evaluate_phase2.py",
                        [
                            "--run_id",
                            run_id,
                            "--event_type",
                            event_type,
                            "--symbols",
                            symbols,
                            "--start",
                            start,
                            "--end",
                            end,
                            "--train_frac",
                            str(float(args.bridge_train_frac)),
                            "--validation_frac",
                            str(float(args.bridge_validation_frac)),
                            "--embargo_days",
                            str(int(args.bridge_embargo_days)),
                            "--edge_cost_k",
                            str(float(args.bridge_edge_cost_k)),
                            "--stressed_cost_multiplier",
                            str(float(args.bridge_stressed_cost_multiplier)),
                            "--min_validation_trades",
                            str(int(args.bridge_min_validation_trades)),
                        ],
                    )
                )

    if int(args.run_phase2_conditional) and int(args.run_discovery_quality_summary):
        stages.append(
            (
                "summarize_discovery_quality",
                PROJECT_ROOT / "pipelines" / "research" / "summarize_discovery_quality.py",
                [
                    "--run_id",
                    run_id,
                ],
            )
        )

    if int(args.run_naive_entry_eval):
        stages.append(
            (
                "evaluate_naive_entry",
                PROJECT_ROOT / "pipelines" / "research" / "evaluate_naive_entry.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--min_trades",
                    str(int(args.naive_min_trades)),
                    "--min_expectancy_after_cost",
                    str(float(args.naive_min_expectancy_after_cost)),
                    "--max_drawdown",
                    str(float(args.naive_max_drawdown)),
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

    if int(args.run_strategy_blueprint_compiler):
        stages.append(
            (
                "compile_strategy_blueprints",
                PROJECT_ROOT / "pipelines" / "research" / "compile_strategy_blueprints.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--max_per_event",
                    str(int(args.strategy_blueprint_max_per_event)),
                    "--ignore_checklist",
                    str(int(args.strategy_blueprint_ignore_checklist)),
                    "--allow_fallback_blueprints",
                    str(int(args.strategy_blueprint_allow_fallback)),
                    "--allow_non_executable_conditions",
                    str(int(args.strategy_blueprint_allow_non_executable_conditions)),
                    "--allow_naive_entry_fail",
                    str(int(args.strategy_blueprint_allow_naive_entry_fail)),
                    "--min_events_floor",
                    str(int(args.strategy_blueprint_min_events_floor)),
                ],
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
                    "--ignore_checklist",
                    str(int(args.strategy_builder_ignore_checklist)),
                    "--allow_non_promoted",
                    str(int(args.strategy_builder_allow_non_promoted)),
                    "--allow_missing_candidate_detail",
                    str(int(args.strategy_builder_allow_missing_candidate_detail)),
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
                    "--clean_engine_artifacts",
                    str(int(args.clean_engine_artifacts)),
                ],
            )
        )

    if int(args.run_walkforward_eval):
        stages.append(
            (
                "run_walkforward",
                PROJECT_ROOT / "pipelines" / "eval" / "run_walkforward.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--start",
                    start,
                    "--end",
                    end,
                    "--embargo_days",
                    str(int(args.walkforward_embargo_days)),
                    "--train_frac",
                    str(float(args.walkforward_train_frac)),
                    "--validation_frac",
                    str(float(args.walkforward_validation_frac)),
                    "--regime_max_share",
                    str(float(args.walkforward_regime_max_share)),
                    "--drawdown_cluster_top_frac",
                    str(float(args.walkforward_drawdown_cluster_top_frac)),
                    "--drawdown_tail_q",
                    str(float(args.walkforward_drawdown_tail_q)),
                    "--allow_unexpected_strategy_files",
                    str(int(args.walkforward_allow_unexpected_strategy_files)),
                    "--clean_engine_artifacts",
                    str(int(args.clean_engine_artifacts)),
                    "--force",
                    force_flag,
                ],
            )
        )

    if int(args.run_backtest) and int(args.run_blueprint_promotion):
        stages.append(
            (
                "promote_blueprints",
                PROJECT_ROOT / "pipelines" / "research" / "promote_blueprints.py",
                [
                    "--run_id",
                    run_id,
                    "--allow_fallback_evidence",
                    str(int(args.promotion_allow_fallback_evidence)),
                    "--regime_max_share",
                    str(float(args.promotion_regime_max_share)),
                    "--max_loss_cluster_len",
                    str(int(args.promotion_max_loss_cluster_len)),
                    "--max_cluster_loss_concentration",
                    str(float(args.promotion_max_cluster_loss_concentration)),
                    "--min_tail_conditional_drawdown_95",
                    str(float(args.promotion_min_tail_conditional_drawdown_95)),
                    "--max_cost_ratio_train_validation",
                    str(float(args.promotion_max_cost_ratio_train_validation)),
                ],
            )
        )

    if int(args.run_make_report) or int(args.run_backtest):
        stages.append(
            (
                "make_report",
                PROJECT_ROOT / "pipelines" / "report" / "make_report.py",
                [
                    "--run_id",
                    run_id,
                    "--allow_backtest_artifact_fallback",
                    str(int(args.report_allow_backtest_artifact_fallback)),
                ],
            )
        )

    stages_with_config = {
        "build_cleaned_5m",
        "build_features_v1",
        "build_context_features",
        "build_market_context",
        "backtest_strategies",
        "run_walkforward",
        "make_report",
    }
    for stage_name, _, base_args in stages:
        if stage_name in stages_with_config or stage_name == "compile_strategy_blueprints" or stage_name.startswith("phase2_conditional_hypotheses"):
            for config_path in args.config:
                base_args.extend(["--config", config_path])
        if stage_name == "backtest_strategies":
            if args.fees_bps is not None:
                base_args.extend(["--fees_bps", str(args.fees_bps)])
            if args.slippage_bps is not None:
                base_args.extend(["--slippage_bps", str(args.slippage_bps)])
            if args.cost_bps is not None:
                base_args.extend(["--cost_bps", str(args.cost_bps)])
            if effective_strategies is not None:
                base_args.extend(["--strategies", str(effective_strategies)])
            if args.overlays:
                base_args.extend(["--overlays", str(args.overlays)])
            if effective_blueprints_path is not None:
                base_args.extend(["--blueprints_path", str(effective_blueprints_path)])
                base_args.extend(["--blueprints_top_k", str(int(args.blueprints_top_k))])
                base_args.extend(["--blueprints_filter_event_type", str(args.blueprints_filter_event_type)])
            base_args.extend(["--timeframe", str(args.backtest_timeframe)])
        if stage_name.startswith("phase2_conditional_hypotheses") or stage_name == "compile_strategy_blueprints":
            if args.fees_bps is not None:
                base_args.extend(["--fees_bps", str(args.fees_bps)])
            if args.slippage_bps is not None:
                base_args.extend(["--slippage_bps", str(args.slippage_bps)])
            if args.cost_bps is not None:
                base_args.extend(["--cost_bps", str(args.cost_bps)])
        if stage_name == "run_walkforward":
            if args.fees_bps is not None:
                base_args.extend(["--fees_bps", str(args.fees_bps)])
            if args.slippage_bps is not None:
                base_args.extend(["--slippage_bps", str(args.slippage_bps)])
            if args.cost_bps is not None:
                base_args.extend(["--cost_bps", str(args.cost_bps)])
            if effective_strategies is not None:
                base_args.extend(["--strategies", str(effective_strategies)])
            if args.overlays:
                base_args.extend(["--overlays", str(args.overlays)])
            if effective_blueprints_path is not None:
                base_args.extend(["--blueprints_path", str(effective_blueprints_path)])
                base_args.extend(["--blueprints_top_k", str(int(args.blueprints_top_k))])
                base_args.extend(["--blueprints_filter_event_type", str(args.blueprints_filter_event_type)])
            base_args.extend(["--timeframe", str(args.backtest_timeframe)])

    stage_timings: List[Tuple[str, float]] = []
    feature_schema_version, feature_schema_hash = _feature_schema_metadata()
    run_manifest = {
        "run_id": run_id,
        "started_at": _utc_now_iso(),
        "finished_at": None,
        "ended_at": None,
        "status": "running",
        "run_mode": args.mode,
        "claim_map_hash": _claim_map_hash(PROJECT_ROOT.parent),
        "symbols": parsed_symbols,
        "start": start,
        "end": end,
        "git_commit": _git_commit(PROJECT_ROOT),
        "data_hash": _data_hash(parsed_symbols),
        "spec_hashes": get_spec_hashes(PROJECT_ROOT.parent),
        "feature_schema_version": feature_schema_version,
        "feature_schema_hash": feature_schema_hash,
        "config_digest": _config_digest([str(x) for x in args.config]),
        "planned_stages": [stage for stage, _, _ in stages],
        "stage_timings_sec": {},
        "failed_stage": None,
        "checklist_decision": None,
        "auto_continue_applied": False,
        "auto_continue_reason": "",
        "execution_blocked_by_checklist": False,
        "non_production_overrides": [],
    }
    _write_run_manifest(run_id, run_manifest)

    print(f"Planned stages: {len(stages)}")
    auto_continue_applied = False
    checklist_decision: str | None = None
    auto_continue_reason = ""
    non_production_overrides: List[str] = []
    # Record non-production bypass flags passed explicitly at startup so the
    # manifest audit trail captures every gate override, not just those injected
    # by auto-continue logic.
    _STARTUP_NON_PROD_FLAGS: List[tuple] = [
        ("strategy_blueprint_allow_fallback", "compile_strategy_blueprints", "--allow_fallback_blueprints"),
        ("strategy_blueprint_allow_naive_entry_fail", "compile_strategy_blueprints", "--allow_naive_entry_fail"),
        ("strategy_blueprint_allow_non_executable_conditions", "compile_strategy_blueprints", "--allow_non_executable_conditions"),
        ("strategy_builder_allow_non_promoted", "build_strategy_candidates", "--allow_non_promoted"),
        ("walkforward_allow_unexpected_strategy_files", "run_walkforward", "--allow_unexpected_strategy_files"),
        ("promotion_allow_fallback_evidence", "promote_blueprints", "--allow_fallback_evidence"),
        ("report_allow_backtest_artifact_fallback", "make_report", "--allow_backtest_artifact_fallback"),
    ]
    for _attr, _stage_name, _cli_flag in _STARTUP_NON_PROD_FLAGS:
        if bool(int(getattr(args, _attr, 0))):
            non_production_overrides.append(f"{_stage_name}:{_cli_flag}=1")
    def _has_hypothesis_entries(run_id: str, event_type: str) -> bool:
        # Check Atlas candidate plan first
        plan_path = DATA_ROOT / "reports" / "hypothesis_generator" / run_id / "candidate_plan.jsonl"
        if plan_path.exists():
            try:
                for line in plan_path.read_text(encoding="utf-8").splitlines():
                    if not line.strip(): continue
                    row = json.loads(line)
                    if row.get("event_type") == event_type:
                        return True
            except Exception:
                pass
        
        # Check classic Phase 1 queue
        queue_path = DATA_ROOT / "reports" / "hypothesis_generator" / run_id / "phase1_hypothesis_queue.jsonl"
        if queue_path.exists():
            try:
                for line in queue_path.read_text(encoding="utf-8").splitlines():
                    if not line.strip(): continue
                    row = json.loads(line)
                    targets = row.get("target_phase2_event_types", [])
                    if isinstance(targets, list) and event_type in targets:
                        return True
            except Exception:
                pass
        return False

    for idx, (stage, script, base_args) in enumerate(stages, start=1):
        # Event-specific gating: skip if queue has no entries for this event type.
        skip_stage = False
        for prefix in ["phase2_conditional_hypotheses_", "bridge_evaluate_phase2_", "build_event_registry_"]:
            if stage.startswith(prefix):
                et = stage.removeprefix(prefix)
                if not _has_hypothesis_entries(run_id, et):
                    print(f"[{idx}/{len(stages)}] Skipping stage: {stage} (no hypothesis entries for {et})")
                    skip_stage = True
                    # Record a zero-time timing entry to keep manifest aligned.
                    stage_timings.append((stage, 0.0))
                break
        
        if skip_stage:
            continue

        print(f"[{idx}/{len(stages)}] Starting stage: {stage}")
        started = time.perf_counter()
        ok = _run_stage(stage, script, base_args, run_id)
        elapsed_sec = time.perf_counter() - started
        stage_timings.append((stage, elapsed_sec))
        print(f"[{idx}/{len(stages)}] Finished stage: {stage} ({elapsed_sec:.1f}s)")
        if not ok:
            run_manifest["finished_at"] = _utc_now_iso()
            run_manifest["ended_at"] = run_manifest["finished_at"]
            run_manifest["status"] = "failed"
            run_manifest["failed_stage"] = stage
            run_manifest["stage_timings_sec"] = {name: round(duration, 3) for name, duration in stage_timings}
            _write_run_manifest(run_id, run_manifest)
            print("Slow-stage summary before failure:")
            for stage_name, duration in sorted(stage_timings, key=lambda x: x[1], reverse=True):
                print(f"  - {stage_name}: {duration:.1f}s")
            return 1
        if stage == "generate_recommendations_checklist":
            checklist_decision = _load_checklist_decision(run_id)
            run_manifest["checklist_decision"] = checklist_decision
            if checklist_decision == "KEEP_RESEARCH" and execution_requested:
                if bool(int(args.auto_continue_on_keep_research)):
                    # B1: Hard fail — auto_continue would inject --allow_fallback_blueprints=1
                    # into compile_strategy_blueprints, producing fallback-track blueprints that
                    # bypass BH-FDR. Write manifest before exiting. [INV_NO_FALLBACK_IN_MEASUREMENT]
                    run_manifest["finished_at"] = _utc_now_iso()
                    run_manifest["ended_at"] = run_manifest["finished_at"]
                    run_manifest["status"] = "failed"
                    run_manifest["failed_stage"] = "checklist_gate"
                    run_manifest["evaluation_guard_violation"] = (
                        "auto_continue_on_keep_research blocked: would inject "
                        "--allow_fallback_blueprints=1, producing fallback blueprints that "
                        "bypass BH-FDR [INV_NO_FALLBACK_IN_MEASUREMENT]"
                    )
                    run_manifest["stage_timings_sec"] = {
                        name: round(duration, 3) for name, duration in stage_timings
                    }
                    _write_run_manifest(run_id, run_manifest)
                    print(
                        "EVALUATION GUARD [INV_NO_FALLBACK_IN_MEASUREMENT]: "
                        "--auto_continue_on_keep_research is disabled. It would inject "
                        "--allow_fallback_blueprints=1 which produces fallback blueprints "
                        "that bypass BH-FDR and cannot appear in evaluation artifacts. "
                        "Remediation: ensure discovery produces standard-track survivors, "
                        "or do not request execution stages when checklist=KEEP_RESEARCH.",
                        file=sys.stderr,
                    )
                    return 1
                else:
                    blocked_stage_names = [
                        pending_stage
                        for pending_stage, _, _ in stages[idx:]
                        if pending_stage
                        in {
                            "compile_strategy_blueprints",
                            "build_strategy_candidates",
                            "backtest_strategies",
                            "run_walkforward",
                            "promote_blueprints",
                            "make_report",
                        }
                    ]
                    first_blocked = blocked_stage_names[0] if blocked_stage_names else "execution_stages"
                    run_manifest["finished_at"] = _utc_now_iso()
                    run_manifest["ended_at"] = run_manifest["finished_at"]
                    run_manifest["status"] = "failed"
                    run_manifest["failed_stage"] = "checklist_gate"
                    run_manifest["execution_blocked_by_checklist"] = True
                    run_manifest["stage_timings_sec"] = {
                        name: round(duration, 3) for name, duration in stage_timings
                    }
                    auto_continue_reason = (
                        "checklist decision KEEP_RESEARCH with execution requested; "
                        "fail-closed default blocked execution stages"
                    )
                    run_manifest["auto_continue_applied"] = False
                    run_manifest["auto_continue_reason"] = auto_continue_reason
                    run_manifest["non_production_overrides"] = []
                    _write_run_manifest(run_id, run_manifest)
                    print(
                        "Checklist gate blocked execution because decision=KEEP_RESEARCH. "
                        f"First blocked stage: {first_blocked}. "
                        "Remediation: address checklist findings before requesting execution stages.",
                        file=sys.stderr,
                    )
                    return 1
            run_manifest["auto_continue_applied"] = bool(auto_continue_applied)
            run_manifest["auto_continue_reason"] = auto_continue_reason
            run_manifest["execution_blocked_by_checklist"] = False
            run_manifest["non_production_overrides"] = sorted(set(non_production_overrides))
            _write_run_manifest(run_id, run_manifest)

    print("Stage timing summary (slowest first):")
    for stage_name, duration in sorted(stage_timings, key=lambda x: x[1], reverse=True):
        print(f"  - {stage_name}: {duration:.1f}s")

    print(f"Pipeline run completed: {run_id}")
    _print_artifact_summary(run_id)
    run_manifest["finished_at"] = _utc_now_iso()
    run_manifest["ended_at"] = run_manifest["finished_at"]
    run_manifest["status"] = "success"
    run_manifest["stage_timings_sec"] = {name: round(duration, 3) for name, duration in stage_timings}
    run_manifest["checklist_decision"] = checklist_decision
    run_manifest["auto_continue_applied"] = bool(auto_continue_applied)
    run_manifest["auto_continue_reason"] = auto_continue_reason
    run_manifest["execution_blocked_by_checklist"] = False
    run_manifest["non_production_overrides"] = sorted(set(non_production_overrides))
    _write_run_manifest(run_id, run_manifest)
    return 0


if __name__ == "__main__":
    sys.exit(main())


def _assert_artifact_exists(path, stage):
    import os
    if not os.path.exists(path):
        raise RuntimeError(f"Skip requested but artifact missing for stage: {stage} -> {path}")
