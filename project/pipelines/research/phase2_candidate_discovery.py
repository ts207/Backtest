from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import hashlib
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional

import numpy as np
import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import (
    ensure_dir,
    read_parquet,
    list_parquet_files,
    run_scoped_lake_path,
    choose_partition_dir,
)
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.execution_costs import resolve_execution_costs
from pipelines._lib.spec_loader import load_global_defaults
from pipelines._lib.timeframe_constants import HORIZON_BARS_BY_TIMEFRAME
from events.registry import EVENT_REGISTRY_SPECS
from events.registry import load_registry_events
from pipelines._lib.spec_utils import get_spec_hashes
from pipelines.research.analyze_conditional_expectancy import (
    _bh_adjust,
    _distribution_stats,
    _two_sided_p_from_t,
)

PRIMARY_OUTPUT_COLUMNS = [
    "candidate_id", "condition", "condition_desc", "action", "action_family", "candidate_type",
    "overlay_base_candidate_id", "sample_size", "train_samples", "validation_samples", "test_samples",
    "baseline_mode", "delta_adverse_mean", "delta_adverse_ci_low", "delta_adverse_ci_high",
    "delta_opportunity_mean", "delta_opportunity_ci_low", "delta_opportunity_ci_high", "delta_exposure_mean",
    "opportunity_forward_mean", "opportunity_tail_mean", "opportunity_composite_mean", "opportunity_cost_mean",
    "net_benefit_mean", "expectancy_per_trade", "robustness_score", "event_frequency", "capacity_proxy",
    "profit_density_score", "quality_score", "gate_a_ci_separated", "gate_b_time_stable", "gate_b_year_signs",
    "gate_c_regime_stable", "gate_c_stable_splits", "gate_c_required_splits", "gate_d_friction_floor",
    "gate_f_exposure_guard", "gate_g_net_benefit", "gate_h_executable_condition", "gate_h_executable_action",
    "gate_e_simplicity", "validation_delta_adverse_mean", "test_delta_adverse_mean", "val_delta_adverse_mean",
    "oos1_delta_adverse_mean", "val_t_stat", "val_p_value", "val_p_value_adj_bh", "oos1_t_stat", "oos1_p_value",
    "test_t_stat", "test_p_value", "test_p_value_adj_bh", "num_tests_event_family", "ess_effective", "ess_lag_used",
    "multiplicity_penalty", "expectancy_after_multiplicity", "expectancy_left", "expectancy_center", "expectancy_right",
    "curvature_penalty", "neighborhood_positive_count", "gate_parameter_curvature", "delay_expectancy_map",
    "delay_positive_ratio", "delay_dispersion", "delay_robustness_score", "gate_delay_robustness",
    "gate_oos_min_samples", "gate_oos_validation", "gate_oos_validation_test", "gate_oos_consistency_strict",
    "gate_multiplicity", "gate_multiplicity_strict", "gate_ess", "after_cost_expectancy_per_trade",
    "stressed_after_cost_expectancy_per_trade", "turnover_proxy_mean", "avg_dynamic_cost_bps", "cost_input_coverage",
    "cost_model_valid", "cost_ratio", "gate_after_cost_positive", "gate_after_cost_stressed_positive",
    "gate_cost_model_valid", "gate_cost_ratio", "bridge_eval_status", "bridge_train_after_cost_bps",
    "bridge_validation_after_cost_bps", "bridge_validation_stressed_after_cost_bps", "bridge_validation_trades",
    "bridge_effective_cost_bps_per_trade", "bridge_gross_edge_bps_per_trade", "gate_bridge_has_trades_validation",
    "gate_bridge_after_cost_positive_validation", "gate_bridge_after_cost_stressed_positive_validation",
    "gate_bridge_edge_cost_ratio", "gate_bridge_turnover_controls", "gate_bridge_tradable", "selection_score_executed",
    "gate_pass", "gate_all_research", "gate_all", "supporting_hypothesis_count", "supporting_hypothesis_ids",
    "fail_reasons",
]

# Rule template → directional multiplier applied to forward returns.
# mean_reversion: we expect price to revert, so fade the move (direction = -1).
# continuation:  we expect the move to continue (direction = +1).
_TEMPLATE_DIRECTION: Dict[str, int] = {
    "mean_reversion": -1,
    "continuation": 1,
    "carry": 1,
    "breakout": 1,
}


def _make_family_id(symbol: str, event_type: str, rule: str, horizon: str, cond_label: str) -> str:
    """BH family key: stratified by symbol so FDR is controlled per-symbol (F-3 fix)."""
    return f"{symbol}_{event_type}_{rule}_{horizon}_{cond_label}"


def _resolved_sample_size(joined_event_count: int, symbol_event_count: int) -> int:
    """Sample size for a candidate must reflect joined observations, not symbol totals."""
    try:
        joined = int(joined_event_count)
    except (TypeError, ValueError):
        joined = 0
    try:
        symbol_total = int(symbol_event_count)
    except (TypeError, ValueError):
        symbol_total = 0
    return max(0, min(joined, symbol_total if symbol_total > 0 else joined))


def _apply_multiplicity_controls(raw_df: pd.DataFrame, max_q: float) -> pd.DataFrame:
    """Apply BH correction per-family, then a global BH over family-adjusted q-values."""
    if raw_df.empty:
        out = raw_df.copy()
        out["q_value_family"] = pd.Series(dtype=float)
        out["is_discovery_family"] = pd.Series(dtype=bool)
        out["q_value"] = pd.Series(dtype=float)
        out["is_discovery"] = pd.Series(dtype=bool)
        return out

    family_frames: List[pd.DataFrame] = []
    for _, family_df in raw_df.groupby("family_id"):
        fam = family_df.copy()
        fam["q_value_family"] = _bh_adjust(fam["p_value"])
        fam["is_discovery_family"] = fam["q_value_family"] <= float(max_q)
        family_frames.append(fam)

    out = pd.concat(family_frames, ignore_index=True)
    out["q_value"] = _bh_adjust(out["q_value_family"])
    out["is_discovery"] = out["q_value"] <= float(max_q)
    return out


def _load_gates_spec() -> Dict[str, Any]:
    path = PROJECT_ROOT / "spec" / "gates.yaml"
    if not path.exists():
        return {}
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _load_family_spec() -> Dict[str, Any]:
    path = PROJECT_ROOT / "spec" / "multiplicity" / "families.yaml"
    if not path.exists():
        return {}
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _load_global_defaults() -> Dict[str, Any]:
    return load_global_defaults(project_root=PROJECT_ROOT)


def _load_features(run_id: str, symbol: str) -> pd.DataFrame:
    """Load PIT features table from lake."""
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "features", "perp", symbol, "5m", "features_v1"),
        DATA_ROOT / "lake" / "features" / "perp" / symbol / "5m" / "features_v1",
    ]
    features_dir = choose_partition_dir(candidates)
    if not features_dir:
        return pd.DataFrame()
    files = list_parquet_files(features_dir)
    if not files:
        return pd.DataFrame()
    df = read_parquet(files)
    if df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    return df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)


def _horizon_to_bars(horizon: str) -> int:
    return HORIZON_BARS_BY_TIMEFRAME.get(horizon.lower().strip(), 12)


def _compute_forward_returns(features_df: pd.DataFrame, horizon_bars: int) -> pd.Series:
    """Compute simple forward log-return at horizon_bars from close prices."""
    close = features_df["close"].astype(float)
    fwd = close.shift(-horizon_bars) / close - 1.0
    return fwd


def _join_events_to_features(
    events_df: pd.DataFrame,
    features_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge event timestamps (enter_ts or timestamp) to the features table using
    a backward merge (most-recent feature bar at or before the event timestamp).
    Returns a merged DataFrame with feature rows for each event.
    """
    ts_col = "enter_ts" if "enter_ts" in events_df.columns else "timestamp"
    if ts_col not in events_df.columns:
        return pd.DataFrame()

    evt = events_df.copy()
    evt["_event_ts"] = pd.to_datetime(evt[ts_col], utc=True, errors="coerce")
    evt = evt.dropna(subset=["_event_ts"]).sort_values("_event_ts").reset_index(drop=True)

    feat = features_df.sort_values("timestamp").reset_index(drop=True)

    # Use merge_asof: for each event, find the latest feature bar <= event_ts
    merged = pd.merge_asof(
        evt[["_event_ts"]].rename(columns={"_event_ts": "timestamp"}),
        feat,
        on="timestamp",
        direction="backward",
    )
    return merged


def calculate_expectancy(
    sym_events: pd.DataFrame,
    features_df: pd.DataFrame,
    rule: str,
    horizon: str,
    shift_labels_k: int = 0,
    entry_lag_bars: int = 1,
    min_samples: int = 30,
) -> Tuple[float, float, float, bool]:
    """
    Real PIT expectancy calculation.

    Args:
        shift_labels_k: If > 0, shift the forward label by an additional k bars
            (future_pos = pos + entry_lag_bars + horizon_bars + shift_labels_k).  This implements
            a *true* misalignment canary: deterministic, and tests specifically
            whether the pipeline is sensitive to label-offset errors.

    Returns:
        (mean_return, p_value, n_events, stability_pass)

    stability_pass: True if sign of mean_return is consistent across first/second
    time half of events.
    """
    if sym_events.empty or features_df.empty:
        return 0.0, 1.0, 0.0, False

    if int(entry_lag_bars) < 1:
        raise ValueError("entry_lag_bars must be >= 1 to prevent same-bar entry leakage")

    horizon_bars = _horizon_to_bars(horizon)
    direction = _TEMPLATE_DIRECTION.get(rule, 1)

    # Join events to features at t0
    merged = _join_events_to_features(sym_events, features_df)
    if merged.empty or "close" not in merged.columns:
        return 0.0, 1.0, 0.0, False

    # Compute forward return at t0 using aligned feature rows.
    # We use the features_df index to look up close horizon_bars later.
    feat_close = features_df["close"].astype(float).values
    # Use DatetimeIndex for timezone-safe searchsorted (handles UTC-aware ts).
    feat_ts_idx = pd.DatetimeIndex(features_df["timestamp"])

    event_returns: List[float] = []
    event_ts_list: List[pd.Timestamp] = []

    for _, row in merged.iterrows():
        ts = row["timestamp"]
        # Find position of this feature row in features_df
        pos = int(feat_ts_idx.searchsorted(ts, side="left"))
        entry_pos = pos + int(entry_lag_bars)
        future_pos = entry_pos + horizon_bars + shift_labels_k
        if (
            pos < 0
            or pos >= len(feat_close)
            or entry_pos < 0
            or entry_pos >= len(feat_close)
            or future_pos >= len(feat_close)
        ):
            continue
        close_t0 = feat_close[entry_pos]
        close_fwd = feat_close[future_pos]
        if close_t0 == 0 or pd.isna(close_t0) or pd.isna(close_fwd):
            continue
        fwd_ret = (close_fwd / close_t0) - 1.0

        directional_ret = float(fwd_ret) * direction
        event_returns.append(directional_ret)
        event_ts_list.append(ts)

    if len(event_returns) < min_samples:
        return 0.0, 1.0, float(len(event_returns)), False

    returns_series = pd.Series(event_returns, dtype=float)
    stats = _distribution_stats(returns_series)
    mean_ret = stats["mean_return"]
    t_stat = stats["t_stat"]
    p_value = _two_sided_p_from_t(t_stat)

    # Sign stability: first vs second time half
    n = len(event_returns)
    mid = n // 2
    first_half = pd.Series(event_returns[:mid], dtype=float)
    second_half = pd.Series(event_returns[mid:], dtype=float)
    first_sign = first_half.mean()
    second_sign = second_half.mean()
    stability_pass = (np.sign(first_sign) == np.sign(second_sign)) and (first_sign != 0) and (second_sign != 0)

    return mean_ret, p_value, float(n), bool(stability_pass)


# Naming convention for analysis-only (non-runtime) bucket prefixes.
_BUCKET_PREFIXES = ("severity_bucket_", "quantile_",)

# Rule template names must never appear as condition strings.
_RULE_TEMPLATE_NAMES = ("mean_reversion", "continuation", "carry", "breakout")


def _condition_for_cond_name(
    cond_name: str,
    *,
    run_symbols=None,
    strict: bool = True,
) -> str:
    """Map a conditioning bucket name to an executable DSL condition string.

    Modes
    -----
    strict=True (research default):
        Unknown names that are not known analysis buckets → return '__BLOCKED__'.
        Callers should mark the candidate compile_eligible=False.
    strict=False (permissive / debug):
        Unknown names fall back to 'all' silently.

    This function MUST NEVER return an 'all__<name>' prefixed string — that
    format silently drops runtime enforcement even for mapped conditions.
    """
    from strategy_dsl.contract_v1 import is_executable_condition

    name = str(cond_name or "").strip()
    if not name or name == "all":
        return "all"

    # Severity / quantile buckets are research-only labels — never runtime
    if any(name.startswith(pfx) for pfx in _BUCKET_PREFIXES):
        return "all"

    if is_executable_condition(name, run_symbols=run_symbols):
        return name

    # Unknown name
    if strict:
        return "__BLOCKED__"
    return "all"


def _condition_routing(
    cond_name: str,
    *,
    run_symbols=None,
    strict: bool = True,
):
    """Return (condition_str, condition_source) tuple.

    condition_source values:
        'runtime'              — maps to a real ConditionNodeSpec
        'bucket_non_runtime'   — research-only bucket (severity_bucket_*, etc.)
        'blocked'              — strict mode rejected unknown name
        'permissive_fallback'  — permissive mode fell back to 'all'
        'unconditional'        — was 'all' or empty to begin with
    """
    from strategy_dsl.contract_v1 import is_executable_condition

    name = str(cond_name or "").strip()
    if not name or name == "all":
        return "all", "unconditional"

    if any(name.startswith(pfx) for pfx in _BUCKET_PREFIXES):
        return "all", "bucket_non_runtime"

    if is_executable_condition(name, run_symbols=run_symbols):
        return name, "runtime"

    if strict:
        return "__BLOCKED__", "blocked"
    return "all", "permissive_fallback"


def _resolve_phase2_costs(
    args: argparse.Namespace,
    project_root: Path,
) -> Tuple["ResolvedExecutionCosts_bps", dict]:  # type: ignore[name-defined]
    """
    Resolve execution costs from spec configs (fees.yaml / pipeline.yaml).

    Returns:
        (cost_bps, cost_coordinate)

    cost_bps: total round-trip cost in basis points (fee + slippage).
    cost_coordinate: dict suitable for embedding in candidate rows and report.json,
        recording the exact config digest so results are reproducible.
    """
    costs = resolve_execution_costs(
        project_root=project_root,
        config_paths=getattr(args, "config", []),
        fees_bps=getattr(args, "fees_bps", None),
        slippage_bps=getattr(args, "slippage_bps", None),
        cost_bps=getattr(args, "cost_bps", None),
    )
    coordinate = {
        "config_digest": costs.config_digest,
        "cost_bps": costs.cost_bps,
        "fee_bps_per_side": costs.fee_bps_per_side,
        "slippage_bps_per_fill": costs.slippage_bps_per_fill,
    }
    return costs.cost_bps, coordinate


def _make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--event_type", required=True)
    parser.add_argument("--symbols", required=True)
    # Spec-bound cost overrides (optional; defaults come from fees.yaml)
    parser.add_argument("--fees_bps", type=float, default=None,
                        help="Override fee_bps_per_side from fees.yaml.")
    parser.add_argument("--slippage_bps", type=float, default=None,
                        help="Override slippage_bps_per_fill from fees.yaml.")
    parser.add_argument("--cost_bps", type=float, default=None,
                        help="Override total cost_bps directly (skips fee+slippage sum).")
    parser.add_argument("--config", action="append", default=[],
                        help="Path to execution cost config (fees.yaml).")
    parser.add_argument("--shift_labels_k", type=int, default=0,
                        help="If > 0, shift labels by k bars (true misalignment canary).")
    parser.add_argument("--entry_lag_bars", type=int, default=1,
                        help="Entry lag in bars after event timestamp. Must be >= 1 (default: 1).")
    parser.add_argument("--candidate_plan", default=None,
                        help="Path to Atlas candidate plan (jsonl). If provided, drives discovery.")
    parser.add_argument("--atlas_mode", type=int, default=0,
                        help="If 1, require --candidate_plan and fail if missing.")
    parser.add_argument("--min_samples", type=int, default=30,
                        help="Minimum number of events required to compute expectancy.")
    return parser


def main():
    parser = _make_parser()
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger(__name__)

    # Use standard manifest helpers
    manifest = start_manifest("phase2_conditional_hypotheses", args.run_id, vars(args), [], [])
    if int(args.entry_lag_bars) < 1:
        raise ValueError("entry_lag_bars must be >= 1 to prevent same-bar entry leakage")

    # 1. Lock in Invariants (Spec-Binding)
    spec_hashes = get_spec_hashes(PROJECT_ROOT)
    gates = _load_gates_spec().get("gate_v1_phase2", {})
    families_spec = _load_family_spec()

    # Resolve execution costs from spec configs — not a CLI float.
    cost_bps, cost_coordinate = _resolve_phase2_costs(args, PROJECT_ROOT)

    max_q = gates.get("max_q_value", 0.05)
    min_after_cost = gates.get("min_after_cost_expectancy_bps", 0.1) / 10000.0  # bps → decimal
    quality_floor_fallback = float(gates.get("quality_floor_fallback", 0.66))
    min_events_fallback = int(gates.get("min_events_fallback", 100))

    # 2. Minimal Template Set & Horizons
    global_defaults = _load_global_defaults()
    fam_config = families_spec.get("families", {}).get(args.event_type, {})
    templates = fam_config.get("templates", global_defaults.get("rule_templates", ["mean_reversion", "continuation", "carry"]))
    horizons = fam_config.get("horizons", global_defaults.get("horizons", ["5m", "15m", "60m"]))
    max_cands = fam_config.get("max_candidates_per_run", 1000)

    symbols = [s.strip() for s in args.symbols.split(",")]
    if len(symbols) * len(templates) * len(horizons) > max_cands:
        log.error("Search budget exceeded for %s. Limit: %d", args.event_type, max_cands)
        sys.exit(1)

    reports_root = DATA_ROOT / "reports" / "phase2" / args.run_id / args.event_type

    # Resolve Phase 1 path via registry
    if args.event_type not in EVENT_REGISTRY_SPECS:
        log.error("event_type %s not in registry.", args.event_type)
        sys.exit(1)

    spec = EVENT_REGISTRY_SPECS[args.event_type]
    phase1_reports_root = DATA_ROOT / "reports" / spec.reports_dir / args.run_id
    events_path = phase1_reports_root / spec.events_file

    if not events_path.exists():
        log.error("Events file not found: %s", events_path)
        sys.exit(1)

    events_df = load_registry_events(
        data_root=DATA_ROOT,
        run_id=args.run_id,
        event_type=args.event_type,
        symbols=symbols,
    )
    if events_df.empty:
        try:
            events_df = pd.read_csv(events_path)
        except pd.errors.EmptyDataError:
            events_df = pd.DataFrame()

    # Load and merge market_state to support conditioning
    if not events_df.empty and "symbol" in events_df.columns:
        if "enter_ts" not in events_df.columns:
             for col in ["timestamp", "anchor_ts", "event_ts"]:
                 if col in events_df.columns:
                     events_df["enter_ts"] = events_df[col]
                     break
        
        if "enter_ts" in events_df.columns:
            events_df["enter_ts"] = pd.to_datetime(events_df["enter_ts"], utc=True, errors="coerce")
            
            merged_dfs = []
            unique_symbols = events_df["symbol"].dropna().unique()
            
            for sym in unique_symbols:
                sym_events = events_df[events_df["symbol"] == sym].copy()
                
                ms_path = run_scoped_lake_path(DATA_ROOT, args.run_id, "context", "market_state", sym, "5m.parquet")
                if not ms_path.exists():
                     ms_path = DATA_ROOT / "lake" / "context" / "market_state" / sym / "5m.parquet"
                
                if ms_path.exists():
                    try:
                        ms_df = pd.read_parquet(ms_path)
                        if "timestamp" in ms_df.columns:
                            ms_df["timestamp"] = pd.to_datetime(ms_df["timestamp"], utc=True, errors="coerce")
                            if "symbol" in ms_df.columns:
                                ms_df = ms_df.drop(columns=["symbol"])
                            
                            sym_events = sym_events.sort_values("enter_ts")
                            ms_df = ms_df.sort_values("timestamp")
                            
                            sym_events = pd.merge_asof(
                                sym_events, 
                                ms_df, 
                                left_on="enter_ts", 
                                right_on="timestamp", 
                                by=None, 
                                direction="backward",
                                tolerance=pd.Timedelta("1h")
                            )
                    except Exception as e:
                        log.warning(f"Failed to load/merge market_state for {sym}: {e}")
                
                merged_dfs.append(sym_events)
            
            if merged_dfs:
                events_df = pd.concat(merged_dfs, ignore_index=True)

    # 3. Candidate Generation (Family = event_type × rule × horizon × condition)
    results = []
    
    candidate_plan_hash = ""
    plan_rows = []
    if args.candidate_plan:
        path = Path(args.candidate_plan)
        if path.exists():
            raw_bytes = path.read_bytes()
            candidate_plan_hash = "sha256:" + hashlib.sha256(raw_bytes).hexdigest()
            for line in raw_bytes.decode("utf-8").splitlines():
                if line.strip():
                    plan_rows.append(json.loads(line))
            
            # Assert plan rows uniqueness
            plan_ids = [r.get("plan_row_id") for r in plan_rows if r.get("plan_row_id")]
            if len(plan_ids) != len(set(plan_ids)):
                log.error("Phase 2 aborted: Duplicate plan_row_ids detected in input candidate plan.")
                sys.exit(1)
        else:
            log.warning("Candidate plan file not found: %s", path)

    if int(args.atlas_mode) and not plan_rows:
        log.error("Atlas-driven mode active but no candidate plan provided/found.")
        sys.exit(1)

    if plan_rows:
        # Atlas-driven discovery
        for plan_row in plan_rows:
            symbol = plan_row["symbol"]
            rule = plan_row["rule_template"]
            horizon = plan_row["horizon"]
            event_type = plan_row["event_type"]
            
            if event_type != args.event_type:
                continue
                
            conditioning_map = plan_row.get("conditioning", {})
            min_samples = plan_row.get("min_events", args.min_samples)
            
            # Filter events for this symbol
            sym_events = events_df[events_df["symbol"] == symbol] if "symbol" in events_df.columns else events_df
            if sym_events.empty:
                continue
                
            # Apply conditioning if any
            bucket_events = sym_events
            cond_label = "all"
            if conditioning_map:
                for col, val in conditioning_map.items():
                    if col in bucket_events.columns:
                        bucket_events = bucket_events[bucket_events[col] == val]
                        cond_label = f"{col}_{val}"
            
            if len(bucket_events) < min_samples:
                # print(f"DEBUG: Insufficient samples for {cond_label}: {len(bucket_events)}")
                continue
                
            features_df = _load_features(args.run_id, symbol)
            if features_df.empty:
                continue
                
            effect, pval, n_joined, stability_pass = calculate_expectancy(
                bucket_events, features_df, rule, horizon,
                shift_labels_k=args.shift_labels_k,
                entry_lag_bars=args.entry_lag_bars,
                min_samples=min_samples,
            )
            
            # Helper to add results with Atlas lineage
            cost = cost_bps / 10000.0
            conservative_cost = cost * 1.5
            after_cost = effect - cost
            after_cost_conservative = effect - conservative_cost
            econ_pass = after_cost >= min_after_cost
            econ_pass_conservative = after_cost_conservative >= min_after_cost
            fail_reasons = []
            if not econ_pass: fail_reasons.append("ECONOMIC_GATE")
            if not econ_pass_conservative: fail_reasons.append("ECONOMIC_CONSERVATIVE")
            if not stability_pass: fail_reasons.append("STABILITY_GATE")
            gate_phase2_final = econ_pass_conservative and stability_pass

            # Compute condition routing before building result
            _cond_str, _cond_source = _condition_routing(cond_label)
            _compile_eligible = _cond_source != "blocked"
            if not _compile_eligible:
                fail_reasons.append("NON_EXECUTABLE_CONDITION")

            _p2_quality_score = (float(econ_pass) + float(econ_pass_conservative) + float(stability_pass)) / 3.0
            _p2_quality_components = json.dumps({"econ": int(econ_pass), "econ_cons": int(econ_pass_conservative), "stability": int(stability_pass)}, sort_keys=True)
            _compile_eligible_fallback = _p2_quality_score >= quality_floor_fallback and int(n_joined) >= min_events_fallback
            _promotion_track = "standard" if gate_phase2_final else "fallback_only"
            results.append({
                "candidate_id": f"{event_type}_{rule}_{horizon}_{symbol}_{cond_label}",
                "family_id": _make_family_id(symbol, event_type, rule, horizon, cond_label),
                "event_type": event_type,
                "rule_template": rule,
                "horizon": horizon,
                "symbol": symbol,
                "conditioning": cond_label,
                "expectancy": effect,
                "after_cost_expectancy": after_cost,
                "after_cost_expectancy_per_trade": after_cost,
                "stressed_after_cost_expectancy_per_trade": after_cost_conservative,
                "cost_ratio": cost / effect if effect != 0 else 1.0,
                "p_value": pval,
                "sample_size": _resolved_sample_size(n_joined, len(sym_events)),
                "n_events": int(n_joined),
                "sign": 1 if effect > 0 else -1,
                "gate_economic": econ_pass,
                "gate_economic_conservative": econ_pass_conservative,
                "gate_stability": stability_pass,
                "gate_phase2_final": gate_phase2_final,
                "robustness_score": _p2_quality_score,
                # Explicit semantic columns — do not conflate these
                "is_discovery": False,  # Populated after BH-FDR pass
                "phase2_quality_score": _p2_quality_score,
                "phase2_quality_components": _p2_quality_components,
                "compile_eligible_phase2_fallback": _compile_eligible_fallback,
                "promotion_track": _promotion_track,
                "fail_reasons": ",".join(fail_reasons),
                "condition": _cond_str,
                "condition_raw": cond_label,   # pre-routing label for Atlas feedback
                "condition_source": _cond_source,
                "compile_eligible": _compile_eligible,
                "action": "enter_long_market" if _TEMPLATE_DIRECTION.get(rule, 1) > 0 else "enter_short_market",
                "cost_config_digest": cost_coordinate["config_digest"],
                "cost_bps_resolved": cost_coordinate["cost_bps"],
                # Atlas Lineage
                "plan_row_id": plan_row.get("plan_row_id", ""),
                "source_claim_ids": ",".join(plan_row.get("source_claim_ids", [])),
                "source_concept_ids": ",".join(plan_row.get("source_concept_ids", [])),
                "candidate_plan_hash": candidate_plan_hash,
            })
    else:
        # Default fallback discovery (original loop)
        # Pre-define allowed conditioning columns to avoid explosion
        CONDITIONING_COLS = global_defaults.get("conditioning_cols", ["severity_bucket", "vol_regime"])

        for symbol in symbols:
            sym_all_events = events_df[events_df["symbol"] == symbol] if "symbol" in events_df.columns else events_df
            features_df = _load_features(args.run_id, symbol)

            if features_df.empty:
                log.warning("No features found for %s / %s — skipping symbol.", args.event_type, symbol)
                continue

            for rule in templates:
                for horizon in horizons:
                    # 3a. Base candidate (all events for this symbol)
                    effect, pval, n_joined, stability_pass = calculate_expectancy(
                        sym_all_events, features_df, rule, horizon,
                        shift_labels_k=args.shift_labels_k,
                        entry_lag_bars=args.entry_lag_bars,
                        min_samples=args.min_samples,
                    )
                    
                    def _add_res(eff, pv, n, stab, cond_name="all"):
                        # Economic Gate (from spec — cost resolved from fees.yaml)
                        cost = cost_bps / 10000.0
                        conservative_cost = cost * 1.5

                        aft_cost = eff - cost
                        aft_cost_conservative = eff - conservative_cost

                        ec_pass = aft_cost >= min_after_cost
                        ec_pass_conservative = aft_cost_conservative >= min_after_cost

                        f_reasons = []
                        if not ec_pass:
                            f_reasons.append(f"ECONOMIC_GATE ({aft_cost:.6f} < {min_after_cost:.6f})")
                        if not ec_pass_conservative:
                            f_reasons.append("ECONOMIC_CONSERVATIVE")
                        if not stab:
                            f_reasons.append("STABILITY_GATE")

                        # Composite Phase 2 gate (economic + stability).
                        g_phase2_final = ec_pass_conservative and stab

                        # Condition routing
                        _cond_str, _cond_source = _condition_routing(cond_name)
                        _compile_eligible = _cond_source != "blocked"
                        if not _compile_eligible:
                            f_reasons.append("NON_EXECUTABLE_CONDITION")

                        _p2_qs = (float(ec_pass) + float(ec_pass_conservative) + float(stab)) / 3.0
                        _p2_qc = json.dumps({"econ": int(ec_pass), "econ_cons": int(ec_pass_conservative), "stability": int(stab)}, sort_keys=True)
                        _fb_eligible = _p2_qs >= quality_floor_fallback and int(n) >= min_events_fallback
                        _p_track = "standard" if g_phase2_final else "fallback_only"
                        results.append({
                            "candidate_id": f"{args.event_type}_{rule}_{horizon}_{symbol}_{cond_name}",
                            "family_id": _make_family_id(symbol, args.event_type, rule, horizon, cond_name),
                            "event_type": args.event_type,
                            "rule_template": rule,
                            "horizon": horizon,
                            "symbol": symbol,
                            "conditioning": cond_name,
                            "expectancy": eff,
                            "after_cost_expectancy": aft_cost,
                            "after_cost_expectancy_per_trade": aft_cost,
                            "stressed_after_cost_expectancy_per_trade": aft_cost_conservative,
                            "cost_ratio": cost / eff if eff != 0 else 1.0,
                            "p_value": pv,
                            "sample_size": _resolved_sample_size(n, len(sym_all_events)),
                            "n_events": int(n),
                            "sign": 1 if eff > 0 else -1,
                            "gate_economic": ec_pass,
                            "gate_economic_conservative": ec_pass_conservative,
                            "gate_stability": stab,
                            "gate_phase2_final": g_phase2_final,
                            "robustness_score": _p2_qs,
                            # Explicit semantic columns
                            "is_discovery": False,  # Populated after BH-FDR pass
                            "phase2_quality_score": _p2_qs,
                            "phase2_quality_components": _p2_qc,
                            "compile_eligible_phase2_fallback": _fb_eligible,
                            "promotion_track": _p_track,
                            "fail_reasons": ",".join(f_reasons),
                            "condition": _cond_str,
                            "condition_raw": cond_name,   # pre-routing label for Atlas feedback
                            "condition_source": _cond_source,
                            "compile_eligible": _compile_eligible,
                            "action": "enter_long_market" if _TEMPLATE_DIRECTION.get(rule, 1) > 0 else "enter_short_market",
                            "cost_config_digest": cost_coordinate["config_digest"],
                            "cost_bps_resolved": cost_coordinate["cost_bps"],
                            "candidate_plan_hash": "",
                        })

                    _add_res(effect, pval, n_joined, stability_pass, "all")

                    # 3b. Conditioned candidates (refined buckets)
                    for col in CONDITIONING_COLS:
                        if col not in sym_all_events.columns:
                            continue
                        
                        buckets = sym_all_events[col].dropna().unique()
                        for val in buckets:
                            # Only test high-signal buckets to avoid noise
                            if col == "severity_bucket" and val not in ["extreme_5pct", "top_10pct"]:
                                continue
                            if col == "vol_regime" and val not in ["high"]:
                                continue

                            bucket_events = sym_all_events[sym_all_events[col] == val]
                            # Allow lower sample size for extreme buckets (higher signal expected)
                            # We further lower this to 10 to allow longer horizons (15m, 60m) to surface
                            effective_min_samples = 10 if "extreme" in str(val) else (20 if "top_10pct" in str(val) else args.min_samples)
                            
                            if len(bucket_events) < effective_min_samples:
                                continue
                                
                            eff_b, pv_b, n_b, stab_b = calculate_expectancy(
                                bucket_events, features_df, rule, horizon,
                                shift_labels_k=args.shift_labels_k,
                                entry_lag_bars=args.entry_lag_bars,
                                min_samples=effective_min_samples,
                            )
                            _add_res(eff_b, pv_b, n_b, stab_b, f"{col}_{val}")

    if not results:
        log.warning("No results produced — check features/events availability for %s. Continuing.", args.event_type)
        # Emit empty artifacts to satisfy pipeline expectations
        ensure_dir(reports_root)
        pd.DataFrame(columns=PRIMARY_OUTPUT_COLUMNS).to_parquet(reports_root / "phase2_candidates_raw.parquet", index=False)
        pd.DataFrame(columns=PRIMARY_OUTPUT_COLUMNS).to_csv(reports_root / "phase2_candidates.csv", index=False)
        sys.exit(0)

    raw_df = pd.DataFrame(results)

    # 4. Multiplicity Control: family BH + global BH over family-adjusted q-values.
    fdr_df = _apply_multiplicity_controls(raw_df=raw_df, max_q=max_q)

    # After multiplicity control: refresh promotion_track and
    # compile_eligible_phase2_fallback from final discovery status.

    # Invariant: Phase 2 Final Pass REQUIRES discovery (multiplicity pass)
    fdr_df["gate_phase2_final"] = fdr_df["gate_phase2_final"] & fdr_df["is_discovery"]

    # Refresh promotion_track: standard only if gate_phase2_final, else fallback_only
    fdr_df["promotion_track"] = np.where(fdr_df["gate_phase2_final"], "standard", "fallback_only")

    # Refresh compile_eligible_phase2_fallback using the now-definitive quality score
    if "phase2_quality_score" not in fdr_df.columns:
        fdr_df["phase2_quality_score"] = fdr_df["robustness_score"]
    fdr_df["compile_eligible_phase2_fallback"] = (
        (fdr_df["phase2_quality_score"] >= quality_floor_fallback)
        & (fdr_df["n_events"] >= min_events_fallback)
    )

    # 5. Emit Artifacts
    ensure_dir(reports_root)

    # Ensure lineage fields exist in fdr_df before writing
    lineage_cols = ["plan_row_id", "source_claim_ids", "source_concept_ids", "candidate_plan_hash"]
    for col in lineage_cols:
        if col not in fdr_df.columns:
            fdr_df[col] = ""

    fdr_df.to_parquet(reports_root / "phase2_candidates_raw.parquet", index=False)
    fdr_df.to_csv(reports_root / "phase2_candidates.csv", index=False)

    fdr_df[["candidate_id", "family_id", "p_value", "sign"]].to_parquet(
        reports_root / "phase2_pvals.parquet", index=False
    )

    fdr_df[["candidate_id", "q_value", "is_discovery"]].to_parquet(
        reports_root / "phase2_fdr.parquet", index=False
    )

    report = {
        "spec_hashes": spec_hashes,
        "inputs": {
            "candidate_plan_hash": candidate_plan_hash
        },
        "family_definition": "Option B (symbol, event_type, rule_template, horizon) — F-3 fix",
        "cost_coordinate": cost_coordinate,
        "thresholds": {
            "max_q_value": max_q,
            "min_after_cost_expectancy_bps": gates.get("min_after_cost_expectancy_bps", 0.1),
            "entry_lag_bars": int(args.entry_lag_bars),
        },
        "summary": {
            "total_tested": int(len(fdr_df)),
            "discoveries_statistical": int(fdr_df["is_discovery"].sum()),
            "survivors_phase2": int(fdr_df["gate_phase2_final"].sum()),
            "stability_pass": int(fdr_df["gate_stability"].sum()),
            "sum_p_values_discoveries": float(fdr_df[fdr_df["is_discovery"]]["p_value"].sum()) if fdr_df["is_discovery"].any() else 0.0,
            "expected_false_discoveries_fdr": float(fdr_df[fdr_df["is_discovery"]]["q_value"].sum()) if fdr_df["is_discovery"].any() else 0.0,
            "fallback_eligible_compile": int(fdr_df["compile_eligible_phase2_fallback"].sum()) if "compile_eligible_phase2_fallback" in fdr_df.columns else 0,
            "quality_floor_fallback": quality_floor_fallback,
            "min_events_fallback": min_events_fallback,
        },
    }

    # 6. Invariant Assertions (Fail Closed)
    summary = report["summary"]
    if summary["discoveries_statistical"] == 0:
        if summary["sum_p_values_discoveries"] != 0:
            raise ValueError(f"Invariant violation: sum_p_values_discoveries must be 0 if discoveries is 0, got {summary['sum_p_values_discoveries']}")
    
    if not (0 <= summary["discoveries_statistical"] <= summary["total_tested"]):
        raise ValueError(f"Invariant violation: Invalid discoveries count {summary['discoveries_statistical']} for total_tested {summary['total_tested']}")
        
    if not (summary["survivors_phase2"] <= summary["total_tested"]):
        raise ValueError(f"Invariant violation: survivors_phase2 {summary['survivors_phase2']} exceeds total_tested {summary['total_tested']}")

    # Every survivor must be a statistical discovery
    if not (summary["survivors_phase2"] <= summary["discoveries_statistical"]):
        raise ValueError(f"Invariant violation: survivors_phase2 {summary['survivors_phase2']} exceeds discoveries_statistical {summary['discoveries_statistical']}")

    with open(reports_root / "phase2_report.json", "w") as f:
        json.dump(report, f, indent=2)

    finalize_manifest(
        manifest,
        "success",
        stats={
            "total_tested": int(len(fdr_df)),
            "discoveries_statistical": int(fdr_df["is_discovery"].sum()),
            "survivors_phase2": int(fdr_df["gate_phase2_final"].sum()),
        },
    )

    log.info(
        "Phase 2 complete: %d tested, %d discoveries_statistical, %d survivors_phase2 → %s",
        len(fdr_df), report["summary"]["discoveries_statistical"],
        report["summary"]["survivors_phase2"],
        reports_root / "phase2_report.json",
    )


if __name__ == "__main__":
    main()
