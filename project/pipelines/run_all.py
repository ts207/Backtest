from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.spec_utils import get_spec_hashes
from pipelines._lib.ontology_contract import (
    ontology_component_hash_fields,
    ontology_component_hashes,
    ontology_spec_hash,
)
from events.registry import EVENT_REGISTRY_SPECS
from events.phase2 import PHASE2_EVENT_CHAIN
from pipelines.stages import (
    build_ingest_stages,
    build_core_stages,
    build_research_stages,
    build_evaluation_stages,
)


DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))

_STRICT_RECOMMENDATIONS_CHECKLIST = False
_CURRENT_PIPELINE_SESSION_ID: Optional[str] = None
_CURRENT_STAGE_INSTANCE_ID: Optional[str] = None
_FEATURE_SCHEMA_VERSION: str = "v1"


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


def _flag_value(args: List[str], flag: str) -> Optional[str]:
    try:
        idx = args.index(flag)
    except ValueError:
        return None
    if idx + 1 >= len(args):
        return None
    return str(args[idx + 1]).strip()


def _stage_instance_base(stage: str, base_args: List[str]) -> str:
    event_type = _flag_value(base_args, "--event_type")
    if event_type and stage in {
        "build_event_registry",
        "phase2_conditional_hypotheses",
        "bridge_evaluate_phase2",
    }:
        return f"{stage}_{event_type}"
    return stage


def _compute_stage_instance_ids(stages: List[Tuple[str, Path, List[str]]]) -> List[str]:
    counts: Dict[str, int] = {}
    out: List[str] = []
    for stage, _, base_args in stages:
        base = _stage_instance_base(stage, base_args)
        n = counts.get(base, 0) + 1
        counts[base] = n
        out.append(base if n == 1 else f"{base}__{n}")
    return out


def _build_timing_map(rows: List[Tuple[str, float]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for name, duration in rows:
        out[name] = round(float(out.get(name, 0.0) + float(duration)), 3)
    return out


def _read_run_manifest(run_id: str) -> Dict[str, object]:
    path = DATA_ROOT / "runs" / run_id / "run_manifest.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _collect_late_artifacts(
    run_id: str,
    artifact_cutoff_utc: str | None,
    *,
    tolerance_sec: float = 1.0,
    max_examples: int = 20,
) -> tuple[int, List[str]]:
    if not artifact_cutoff_utc:
        return 0, []
    try:
        cutoff_ts = datetime.fromisoformat(artifact_cutoff_utc).timestamp() + float(tolerance_sec)
    except Exception:
        return 0, []

    paths: List[Path] = []
    run_root = DATA_ROOT / "runs" / run_id
    if run_root.exists():
        paths.extend(sorted(p for p in run_root.rglob("*") if p.is_file()))

    reports_root = DATA_ROOT / "reports"
    if reports_root.exists():
        for stage_dir in sorted(p for p in reports_root.iterdir() if p.is_dir()):
            run_dir = stage_dir / run_id
            if run_dir.exists() and run_dir.is_dir():
                paths.extend(sorted(p for p in run_dir.rglob("*") if p.is_file()))

    late: List[str] = []
    for path in paths:
        try:
            if path.stat().st_mtime > cutoff_ts:
                late.append(str(path.relative_to(DATA_ROOT)))
        except OSError:
            continue
    late_sorted = sorted(set(late))
    return len(late_sorted), late_sorted[:max_examples]


def _apply_run_terminal_audit(run_id: str, manifest: Dict[str, object]) -> None:
    cutoff = str(manifest.get("finished_at") or manifest.get("ended_at") or "").strip() or None
    manifest["artifact_cutoff_utc"] = cutoff
    late_count, late_examples = _collect_late_artifacts(run_id, cutoff)
    manifest["late_artifact_count"] = int(late_count)
    manifest["late_artifact_examples"] = late_examples


def _compute_stage_input_hash(script_path: Path, base_args: List[str], run_id: str) -> str:
    """Hash the stage command + script content for robust cache-hit detection."""
    try:
        script_hash = hashlib.sha256(script_path.read_bytes()).hexdigest()[:16]
    except OSError:
        script_hash = "unknown"
    payload = f"{script_path}:{script_hash}:{' '.join(base_args)}:{run_id}"
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _run_stage(stage: str, script_path: Path, base_args: List[str], run_id: str) -> bool:
    runs_dir = DATA_ROOT / "runs" / run_id
    runs_dir.mkdir(parents=True, exist_ok=True)
    stage_instance_id = _CURRENT_STAGE_INSTANCE_ID or _stage_instance_base(stage, base_args)
    log_path = runs_dir / f"{stage_instance_id}.log"
    manifest_path = runs_dir / f"{stage_instance_id}.json"

    # Stage output caching: skip if manifest exists with matching input_hash.
    # Enable by setting env var BACKTEST_STAGE_CACHE=1.
    if os.environ.get("BACKTEST_STAGE_CACHE", "0") == "1":
        input_hash = _compute_stage_input_hash(script_path, base_args, run_id)
        if manifest_path.exists():
            try:
                cached = json.loads(manifest_path.read_text(encoding="utf-8"))
                if cached.get("input_hash") == input_hash and cached.get("status") == "success":
                    print(f"[CACHE HIT] {stage} (input_hash={input_hash}) — skipping.")
                    return True
            except Exception:
                pass
    else:
        input_hash = None

    cmd = [sys.executable, str(script_path)] + base_args
    if _script_supports_log_path(script_path):
        cmd.extend(["--log_path", str(log_path)])
    env = os.environ.copy()
    env["BACKTEST_STAGE_INSTANCE_ID"] = stage_instance_id
    env["BACKTEST_FEATURE_SCHEMA_VERSION"] = _FEATURE_SCHEMA_VERSION
    if _CURRENT_PIPELINE_SESSION_ID:
        env["BACKTEST_PIPELINE_SESSION_ID"] = _CURRENT_PIPELINE_SESSION_ID
    result = subprocess.run(cmd, env=env)

    # Stamp input_hash into stage manifest on success for future cache reads.
    if input_hash and result.returncode == 0 and manifest_path.exists():
        try:
            mdata = json.loads(manifest_path.read_text(encoding="utf-8"))
            mdata["input_hash"] = input_hash
            manifest_path.write_text(json.dumps(mdata, indent=2), encoding="utf-8")
        except Exception:
            pass

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


def _run_stages_parallel(
    stages: List[Tuple[str, Path, List[str]]],
    run_id: str,
    max_workers: int,
    *,
    continue_on_failure: bool = False,
) -> Tuple[bool, List[Tuple[str, float]]]:
    """Run a batch of independent stages in parallel using subprocess workers.

    Returns (all_ok, [(stage_name, elapsed_sec), ...]) in completion order.
    """
    timings: List[Tuple[str, float]] = []
    all_ok = True
    effective_workers = max(1, min(max_workers, len(stages)))

    def _worker(args: Tuple[str, Path, List[str]]) -> Tuple[str, bool, float]:
        stage_name, script, base_args = args
        t0 = time.perf_counter()
        ok = _run_stage(stage_name, script, base_args, run_id)
        return stage_name, ok, time.perf_counter() - t0

    with concurrent.futures.ThreadPoolExecutor(max_workers=effective_workers) as pool:
        futures = {pool.submit(_worker, s): s[0] for s in stages}
        for fut in concurrent.futures.as_completed(futures):
            stage_name, ok, elapsed = fut.result()
            timings.append((stage_name, elapsed))
            if not ok:
                all_ok = False
                if not continue_on_failure:
                    # Cancel remaining — they will still be waited on but won't start
                    for remaining in futures:
                        remaining.cancel()
                    break
    return all_ok, timings



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
        ("edge_registry", DATA_ROOT / "runs" / run_id / "research" / "edge_registry.parquet"),
        ("strategy_builder", DATA_ROOT / "reports" / "strategy_builder" / run_id),
        ("report", DATA_ROOT / "reports" / "vol_compression_expansion_v1" / run_id / "summary.md"),
    ]
    print("Artifact summary:")
    print(f"  - BACKTEST_DATA_ROOT: {DATA_ROOT}")
    for label, path in artifact_paths:
        status = "found" if path.exists() else "missing"
        print(f"  - {label} ({status}): {path}")


def _validate_phase2_event_chain() -> List[str]:
    issues: List[str] = []
    seen: set[str] = set()
    research_root = PROJECT_ROOT / "pipelines" / "research"
    for event_type, script_name, _ in PHASE2_EVENT_CHAIN:
        event = str(event_type).strip()
        script = str(script_name).strip()
        if not event:
            issues.append("Empty event_type entry in PHASE2_EVENT_CHAIN")
            continue
        if event in seen:
            issues.append(f"Duplicate event_type in PHASE2_EVENT_CHAIN: {event}")
        seen.add(event)

        if event not in EVENT_REGISTRY_SPECS:
            issues.append(f"Missing event spec/registry entry for phase2 event: {event}")

        script_path = research_root / script
        if not script_path.exists():
            issues.append(f"Missing phase2 analyzer script for {event}: {script_path}")
    missing_registry_events = sorted(set(EVENT_REGISTRY_SPECS.keys()) - seen)
    if missing_registry_events:
        issues.append(
            "PHASE2_EVENT_CHAIN is missing registry-declared event families: "
            + ", ".join(missing_registry_events)
        )
    return issues


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
            stderr=subprocess.DEVNULL,
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
    version = str(os.getenv("BACKTEST_FEATURE_SCHEMA_VERSION", "v1")).strip().lower()
    if version not in ("v1", "v2"):
        version = "v1"
    schema_path = PROJECT_ROOT / "schemas" / f"feature_schema_{version}.json"
    if not schema_path.exists():
        return "unknown", _sha256_text("")
    try:
        payload = json.loads(schema_path.read_text(encoding="utf-8"))
        version_label = str(payload.get("version", f"feature_schema_{version}"))
    except Exception:
        version_label = f"feature_schema_{version}"
    schema_hash = hashlib.sha256(schema_path.read_bytes()).hexdigest()
    return version_label, schema_hash


def _fast_walk_stat(root_path: Path) -> List[str]:
    import os
    entries = []
    # Use os.walk directly which is heavily optimized in Python 3.5+ (uses scandir)
    for dirpath, dirnames, filenames in os.walk(str(root_path)):
        for name in filenames:
            path = os.path.join(dirpath, name)
            try:
                st = os.stat(path)
                # Keep the same payload format: relative_path|size|mtime_ns
                rel_path = os.path.relpath(path, str(DATA_ROOT))
                entries.append(f"{rel_path}|{int(st.st_size)}|{int(st.st_mtime_ns)}")
            except OSError:
                continue
    return entries

def _data_hash(symbols: List[str]) -> str:
    import concurrent.futures
    roots: List[Path] = []
    for symbol in symbols:
        roots.append(DATA_ROOT / "lake" / "raw" / "binance" / "perp" / symbol)
        roots.append(DATA_ROOT / "lake" / "raw" / "binance" / "spot" / symbol)

    entries: List[str] = []
    valid_roots = [r for r in roots if r.exists()]
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for result in executor.map(_fast_walk_stat, valid_roots):
            entries.extend(result)
            
    # Sort after collecting all to ensure consistent hash
    entries.sort()
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
        default="dynamic",
        help=(
            "Comma-separated symbols for a single discovery run_id "
            "or 'dynamic' to auto-resolve from spec/historical_universe.csv."
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
    parser.add_argument(
        "--run_ontology_consistency_audit",
        type=int,
        default=1,
        help="If 1, run ontology consistency audit preflight before stage execution.",
    )
    parser.add_argument(
        "--ontology_consistency_fail_on_missing",
        type=int,
        default=1,
        help="If 1, ontology consistency preflight fails closed on missing/invalid contract links.",
    )
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument(
        "--allow_ontology_hash_mismatch",
        type=int,
        default=0,
        help="If 1, allow reusing run_id when ontology_spec_hash differs from existing run_manifest.",
    )

    parser.add_argument("--run_ingest_liquidation_snapshot", type=int, default=0)
    parser.add_argument("--run_ingest_open_interest_hist", type=int, default=0)
    parser.add_argument("--open_interest_period", default="5m")

    parser.add_argument("--run_hypothesis_generator", type=int, default=0)
    parser.add_argument("--hypothesis_datasets", default="auto")
    parser.add_argument("--hypothesis_max_fused", type=int, default=24)

    parser.add_argument("--run_phase2_conditional", type=int, default=0)
    parser.add_argument(
        "--phase2_event_type",
        default="VOL_SHOCK",
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
    parser.add_argument(
        "--phase2_cost_calibration_mode",
        choices=["static", "tob_regime"],
        default="static",
        help="Phase2 candidate cost calibration mode. static uses fees.yaml only; tob_regime calibrates slippage from ToB regimes.",
    )
    parser.add_argument(
        "--phase2_cost_min_tob_coverage",
        type=float,
        default=0.60,
        help="Minimum event-to-ToB alignment coverage for tob_regime calibration before fallback to static costs.",
    )
    parser.add_argument(
        "--phase2_cost_tob_tolerance_minutes",
        type=int,
        default=10,
        help="Backward asof tolerance (minutes) for event-to-ToB alignment in cost calibration.",
    )
    parser.add_argument(
        "--phase2_gate_profile",
        choices=["auto", "discovery", "promotion"],
        default="auto",
        help="Phase2 gate profile. auto => discovery for research mode, promotion otherwise.",
    )
    parser.add_argument("--run_bridge_eval_phase2", type=int, default=1)
    parser.add_argument("--bridge_edge_cost_k", type=float, default=2.0)
    parser.add_argument("--bridge_stressed_cost_multiplier", type=float, default=1.5)
    parser.add_argument("--bridge_min_validation_trades", type=int, default=20)
    parser.add_argument("--bridge_train_frac", type=float, default=0.6)
    parser.add_argument("--bridge_validation_frac", type=float, default=0.2)
    parser.add_argument("--bridge_embargo_days", type=int, default=1)
    parser.add_argument("--run_discovery_quality_summary", type=int, default=1)
    parser.add_argument(
        "--max_analyzer_workers",
        type=int,
        default=min(os.cpu_count() or 1, 8),
        help="Max parallel workers for Phase 1 event analyzer stages. Set to 1 for sequential (default auto).",
    )

    parser.add_argument("--run_edge_candidate_universe", type=int, default=0)
    parser.add_argument("--run_naive_entry_eval", type=int, default=1)
    parser.add_argument("--naive_min_trades", type=int, default=100)
    parser.add_argument("--naive_min_expectancy_after_cost", type=float, default=0.0)
    parser.add_argument("--naive_max_drawdown", type=float, default=-0.25)
    parser.add_argument("--run_candidate_promotion", type=int, default=1)
    parser.add_argument("--candidate_promotion_max_q_value", type=float, default=0.10)
    parser.add_argument("--candidate_promotion_min_events", type=int, default=100)
    parser.add_argument("--candidate_promotion_min_stability_score", type=float, default=0.05)
    parser.add_argument("--candidate_promotion_min_sign_consistency", type=float, default=0.67)
    parser.add_argument("--candidate_promotion_min_cost_survival_ratio", type=float, default=0.75)
    parser.add_argument("--candidate_promotion_min_tob_coverage", type=float, default=0.80)
    parser.add_argument("--candidate_promotion_max_negative_control_pass_rate", type=float, default=0.01)
    parser.add_argument("--candidate_promotion_require_hypothesis_audit", type=int, default=0)
    parser.add_argument("--candidate_promotion_allow_missing_negative_controls", type=int, default=1)
    parser.add_argument("--run_edge_registry_update", type=int, default=1)
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
    parser.add_argument("--run_interaction_lift", type=int, default=1)
    parser.add_argument("--strict_recommendations_checklist", type=int, default=0)
    parser.add_argument("--auto_continue_on_keep_research", type=int, default=0)
    parser.add_argument("--liq_vol_th", type=float, default=100000.0)
    parser.add_argument("--oi_drop_th", type=float, default=-500000.0)
    parser.add_argument("--volume_collapse_th", type=float, default=0.5)
    parser.add_argument("--range_spike_th", type=float, default=2.0)
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
    parser.add_argument("--feature_schema_version", choices=["v1", "v2"], default="v1")
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


    chain_issues = _validate_phase2_event_chain()
    if chain_issues:
        print("Phase2 event chain validation failed:", file=sys.stderr)
        for issue in chain_issues:
            print(f"  - {issue}", file=sys.stderr)
        return 1

    if int(args.run_ontology_consistency_audit):
        audit_cmd = [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "ontology_consistency_audit.py"),
            "--repo-root",
            str(PROJECT_ROOT.parent),
        ]
        if int(args.ontology_consistency_fail_on_missing):
            audit_cmd.append("--fail-on-missing")
        audit_result = subprocess.run(audit_cmd)
        if audit_result.returncode != 0:
            print("Ontology consistency audit preflight failed.", file=sys.stderr)
            return int(audit_result.returncode) or 1
    
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
        if args.phase2_gate_profile == "discovery":
            print(
                f"Error: --phase2_gate_profile=discovery is forbidden in {args.mode} mode. "
                "Use promotion or auto.",
                file=sys.stderr,
            )
            return 1

    global _STRICT_RECOMMENDATIONS_CHECKLIST, _FEATURE_SCHEMA_VERSION
    _STRICT_RECOMMENDATIONS_CHECKLIST = bool(int(args.strict_recommendations_checklist))
    _FEATURE_SCHEMA_VERSION = str(args.feature_schema_version).strip().lower()

    run_id = args.run_id or _run_id_default()
    
    # Resolve dynamic universe
    resolved_symbols = str(args.symbols).strip()
    if resolved_symbols.lower() == "dynamic":
        seed_path = PROJECT_ROOT.parent / "spec" / "historical_universe.csv"
        if seed_path.exists():
            try:
                import pandas as pd
                df = pd.read_csv(seed_path)
                if "symbol" in df.columns:
                    resolved_symbols = ",".join(df["symbol"].dropna().astype(str).str.strip().tolist())
                else:
                    print("Error: historical_universe.csv missing 'symbol' column.", file=sys.stderr)
                    return 1
            except Exception as e:
                print(f"Error reading dynamic universe seed file: {e}", file=sys.stderr)
                return 1
        else:
            print("Error: dynamic set for --symbols, but spec/historical_universe.csv is missing.", file=sys.stderr)
            return 1

    parsed_symbols = _parse_symbols_csv(resolved_symbols)
    if not parsed_symbols:
        print("--symbols must include at least one symbol.", file=sys.stderr)
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

    if int(args.run_strategy_blueprint_compiler) and not int(args.run_candidate_promotion):
        print(
            "compile_strategy_blueprints requires promoted candidates. "
            "Enable --run_candidate_promotion 1.",
            file=sys.stderr,
        )
        return 1

    if int(args.run_strategy_builder) and not int(args.run_candidate_promotion):
        print(
            "run_strategy_builder requires promoted candidates. "
            "Enable --run_candidate_promotion 1.",
            file=sys.stderr,
        )
        return 1

    cross_venue_requested = bool(
        int(args.run_phase2_conditional)
        and args.phase2_event_type in {"all", "CROSS_VENUE_DESYNC"}
    )
    run_spot_pipeline = bool(int(args.enable_cross_venue_spot_pipeline) and cross_venue_requested)
    research_gate_profile = "promotion" if args.mode in {"production", "certification"} else "discovery"

    stages: List[Tuple[str, Path, List[str]]] = []

    stages.extend(build_ingest_stages(
        args=args,
        run_id=run_id,
        symbols=symbols,
        start=start,
        end=end,
        force_flag=force_flag,
        run_spot_pipeline=run_spot_pipeline,
        project_root=PROJECT_ROOT,
    ))

    stages.extend(build_core_stages(
        args=args,
        run_id=run_id,
        symbols=symbols,
        start=start,
        end=end,
        force_flag=force_flag,
        allow_missing_funding_flag=allow_missing_funding_flag,
        run_spot_pipeline=run_spot_pipeline,
        project_root=PROJECT_ROOT,
    ))

    stages.extend(build_research_stages(
        args=args,
        run_id=run_id,
        symbols=symbols,
        start=start,
        end=end,
        research_gate_profile=research_gate_profile,
        project_root=PROJECT_ROOT,
        data_root=DATA_ROOT,
        phase2_event_chain=PHASE2_EVENT_CHAIN,
    ))

    stages.extend(build_evaluation_stages(
        args=args,
        run_id=run_id,
        symbols=symbols,
        start=start,
        end=end,
        force_flag=force_flag,
        project_root=PROJECT_ROOT,
        data_root=DATA_ROOT,
    ))

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

    planned_stage_instances = _compute_stage_instance_ids(stages)
    stage_timings: List[Tuple[str, float]] = []
    stage_instance_timings: List[Tuple[str, float]] = []
    pipeline_session_id = _sha256_text(f"{run_id}:{time.time_ns()}:{os.getpid()}")
    global _CURRENT_PIPELINE_SESSION_ID
    _CURRENT_PIPELINE_SESSION_ID = pipeline_session_id
    feature_schema_version, feature_schema_hash = _feature_schema_metadata()
    ontology_hash = ontology_spec_hash(PROJECT_ROOT.parent)
    ontology_component_fields = ontology_component_hash_fields(
        ontology_component_hashes(PROJECT_ROOT.parent)
    )
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
        "ontology_spec_hash": ontology_hash,
        "taxonomy_hash": ontology_component_fields.get("taxonomy_hash"),
        "canonical_event_registry_hash": ontology_component_fields.get("canonical_event_registry_hash"),
        "state_registry_hash": ontology_component_fields.get("state_registry_hash"),
        "verb_lexicon_hash": ontology_component_fields.get("verb_lexicon_hash"),
        "feature_schema_version": feature_schema_version,
        "feature_schema_hash": feature_schema_hash,
        "pipeline_session_id": pipeline_session_id,
        "config_digest": _config_digest([str(x) for x in args.config]),
        "planned_stages": [stage for stage, _, _ in stages],
        "planned_stage_instances": planned_stage_instances,
        "stage_timings_sec": {},
        "stage_instance_timings_sec": {},
        "failed_stage": None,
        "checklist_decision": None,
        "auto_continue_applied": False,
        "auto_continue_reason": "",
        "execution_blocked_by_checklist": False,
        "non_production_overrides": [],
        "artifact_cutoff_utc": None,
        "late_artifact_count": 0,
        "late_artifact_examples": [],
    }
    existing_manifest_path = DATA_ROOT / "runs" / run_id / "run_manifest.json"
    if existing_manifest_path.exists():
        try:
            existing_manifest = json.loads(existing_manifest_path.read_text(encoding="utf-8"))
        except Exception:
            existing_manifest = {}
        existing_ontology_hash = str(existing_manifest.get("ontology_spec_hash", "")).strip()
        if (
            existing_ontology_hash
            and existing_ontology_hash != ontology_hash
            and not bool(int(args.allow_ontology_hash_mismatch))
        ):
            raise ValueError(
                "Ontology hash mismatch for existing run_id. "
                f"existing={existing_ontology_hash}, current={ontology_hash}. "
                "Use --allow_ontology_hash_mismatch 1 only for explicit override."
            )
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
        # If hypothesis generator was explicitly disabled, assume we want fallback/broad discovery
        # and allow all event types to proceed to Phase 2.
        if not int(args.run_hypothesis_generator):
            return True

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

    pipeline_started = time.perf_counter()
    total_stages = len(stages)
    for idx, ((stage, script, base_args), stage_instance) in enumerate(
        zip(stages, planned_stage_instances), start=1
    ):
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
                    stage_instance_timings.append((stage_instance, 0.0))
                break
        
        if skip_stage:
            continue

        disk_manifest = _read_run_manifest(run_id)
        disk_status = str(disk_manifest.get("status", "")).strip().lower()
        if disk_status in {"success", "failed"}:
            run_manifest["finished_at"] = _utc_now_iso()
            run_manifest["ended_at"] = run_manifest["finished_at"]
            run_manifest["status"] = "failed"
            run_manifest["failed_stage"] = "terminal_state_guard"
            run_manifest["failed_stage_instance"] = stage_instance
            run_manifest["terminal_guard_observed_status"] = disk_status
            run_manifest["stage_timings_sec"] = _build_timing_map(stage_timings)
            run_manifest["stage_instance_timings_sec"] = _build_timing_map(stage_instance_timings)
            _apply_run_terminal_audit(run_id, run_manifest)
            _write_run_manifest(run_id, run_manifest)
            print(
                "Terminal run manifest detected before stage launch; aborting further stage execution.",
                file=sys.stderr,
            )
            return 1

        # --- Parallel analyzer batch execution ---
        # Event analyzer stages (build_event_registry_*, phase1_analyze_*) are
        # independent across event types and can safely run concurrently.
        # Group consecutive analyzer stages and dispatch them as one parallel batch,
        # then resume sequential execution for the remaining stages.
        max_workers = int(getattr(args, "max_analyzer_workers", 1))
        is_analyzer = lambda s: (  # noqa: E731
            s.startswith("build_event_registry_")
            or s.startswith("phase1_analyze_")
        )
        if max_workers > 1 and is_analyzer(stage):
            # Accumulate this and all following consecutive analyzer stages into a batch.
            batch = [(stage, script, base_args)]
            batch_instances = [stage_instance]
            # Peek ahead to collect the full consecutive batch
            lookahead = idx  # current idx is already 1-based
            remaining_stages_iter = list(zip(stages[lookahead:], planned_stage_instances[lookahead:]))
            for (lk_stage, lk_script, lk_base_args), lk_instance in remaining_stages_iter:
                if is_analyzer(lk_stage):
                    batch.append((lk_stage, lk_script, lk_base_args))
                    batch_instances.append(lk_instance)
                else:
                    break
            # Advance the outer loop by consuming these from the iterator — we
            # reconstruct stages/instances slices so next iteration sees correct idx.
            # Since the for-loop is over enumerate(zip(stages, planned_stage_instances))
            # we cannot easily skip ahead; instead we place a sentinel. 
            # Simplest approach: run the batch now and let subsequent iterations
            # skip stages already completed (tracked in _completed_parallel set).
            if not hasattr(_run_stages_parallel, "_completed"):
                _run_stages_parallel._completed = set()  # type: ignore[attr-defined]
            if stage in getattr(_run_stages_parallel, "_completed", set()):
                continue  # already handled in a prior batch

            batch_start = time.perf_counter()
            print(
                f"[PARALLEL] Dispatching {len(batch)} analyzer stages with {max_workers} workers: "
                + ", ".join(s for s, _, __ in batch)
            )
            par_ok, par_timings = _run_stages_parallel(batch, run_id, max_workers)
            for s_name, s_elapsed in par_timings:
                stage_timings.append((s_name, s_elapsed))
                stage_instance_timings.append((s_name, s_elapsed))
                getattr(_run_stages_parallel, "_completed", set()).add(s_name)
            print(
                f"[PARALLEL] Batch done in {time.perf_counter()-batch_start:.1f}s "
                f"({'OK' if par_ok else 'FAILED'})"
            )
            if not par_ok:
                run_manifest["finished_at"] = _utc_now_iso()
                run_manifest["ended_at"] = run_manifest["finished_at"]
                run_manifest["status"] = "failed"
                run_manifest["failed_stage"] = "parallel_analyzer_batch"
                run_manifest["stage_timings_sec"] = _build_timing_map(stage_timings)
                run_manifest["stage_instance_timings_sec"] = _build_timing_map(stage_instance_timings)
                _apply_run_terminal_audit(run_id, run_manifest)
                _write_run_manifest(run_id, run_manifest)
                return 1
            continue

        elapsed_pipeline_before = time.perf_counter() - pipeline_started
        print(
            f"[{idx}/{total_stages}] Starting stage: {stage} "
            f"(pipeline_elapsed={elapsed_pipeline_before:.1f}s)"
        )
        started = time.perf_counter()
        global _CURRENT_STAGE_INSTANCE_ID
        _CURRENT_STAGE_INSTANCE_ID = stage_instance
        try:
            ok = _run_stage(stage, script, base_args, run_id)
        finally:
            _CURRENT_STAGE_INSTANCE_ID = None
        elapsed_sec = time.perf_counter() - started
        stage_timings.append((stage, elapsed_sec))
        stage_instance_timings.append((stage_instance, elapsed_sec))
        elapsed_pipeline = time.perf_counter() - pipeline_started
        avg_stage = elapsed_pipeline / float(idx)
        remaining = max(0, total_stages - idx)
        eta = remaining * avg_stage
        print(
            f"[{idx}/{total_stages}] Finished stage: {stage} ({elapsed_sec:.1f}s) "
            f"| pipeline_elapsed={elapsed_pipeline:.1f}s | eta~{eta:.1f}s"
        )
        if not ok:
            run_manifest["finished_at"] = _utc_now_iso()
            run_manifest["ended_at"] = run_manifest["finished_at"]
            run_manifest["status"] = "failed"
            run_manifest["failed_stage"] = stage
            run_manifest["failed_stage_instance"] = stage_instance
            run_manifest["stage_timings_sec"] = _build_timing_map(stage_timings)
            run_manifest["stage_instance_timings_sec"] = _build_timing_map(stage_instance_timings)
            _apply_run_terminal_audit(run_id, run_manifest)
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
                    run_manifest["stage_timings_sec"] = _build_timing_map(stage_timings)
                    run_manifest["stage_instance_timings_sec"] = _build_timing_map(stage_instance_timings)
                    _apply_run_terminal_audit(run_id, run_manifest)
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
                    run_manifest["stage_timings_sec"] = _build_timing_map(stage_timings)
                    run_manifest["stage_instance_timings_sec"] = _build_timing_map(stage_instance_timings)
                    auto_continue_reason = (
                        "checklist decision KEEP_RESEARCH with execution requested; "
                        "fail-closed default blocked execution stages"
                    )
                    run_manifest["auto_continue_applied"] = False
                    run_manifest["auto_continue_reason"] = auto_continue_reason
                    run_manifest["non_production_overrides"] = []
                    _apply_run_terminal_audit(run_id, run_manifest)
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
    run_manifest["stage_timings_sec"] = _build_timing_map(stage_timings)
    run_manifest["stage_instance_timings_sec"] = _build_timing_map(stage_instance_timings)
    run_manifest["checklist_decision"] = checklist_decision
    run_manifest["auto_continue_applied"] = bool(auto_continue_applied)
    run_manifest["auto_continue_reason"] = auto_continue_reason
    run_manifest["execution_blocked_by_checklist"] = False
    run_manifest["non_production_overrides"] = sorted(set(non_production_overrides))
    _apply_run_terminal_audit(run_id, run_manifest)
    _write_run_manifest(run_id, run_manifest)
    _CURRENT_PIPELINE_SESSION_ID = None
    return 0


if __name__ == "__main__":
    sys.exit(main())


def _assert_artifact_exists(path, stage):
    import os
    if not os.path.exists(path):
        raise RuntimeError(f"Skip requested but artifact missing for stage: {stage} -> {path}")
