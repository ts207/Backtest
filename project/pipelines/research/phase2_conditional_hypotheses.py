from __future__ import annotations

import argparse
import ast
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from engine.execution_model import estimate_transaction_cost_bps
from events.registry import EVENT_REGISTRY_SPECS, load_registry_events, normalize_phase1_events
from pipelines._lib.execution_costs import resolve_execution_costs
from pipelines._lib.io_utils import (
    choose_partition_dir,
    ensure_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
)
from pipelines._lib.selection_log import append_selection_log
from strategy_dsl.contract_v1 import is_executable_action, is_executable_condition
from pipelines.research.analyze_conditional_expectancy import (
    _bh_adjust,
    _two_sided_p_from_t,
    build_walk_forward_split_labels,
)


PRIMARY_OUTPUT_COLUMNS = [
    "candidate_id",
    "condition",
    "condition_desc",
    "action",
    "action_family",
    "candidate_type",
    "overlay_base_candidate_id",
    "sample_size",
    "train_samples",
    "validation_samples",
    "test_samples",
    "baseline_mode",
    "delta_adverse_mean",
    "delta_adverse_ci_low",
    "delta_adverse_ci_high",
    "delta_opportunity_mean",
    "delta_opportunity_ci_low",
    "delta_opportunity_ci_high",
    "delta_exposure_mean",
    "opportunity_forward_mean",
    "opportunity_tail_mean",
    "opportunity_composite_mean",
    "opportunity_cost_mean",
    "net_benefit_mean",
    "expectancy_per_trade",
    "robustness_score",
    "event_frequency",
    "capacity_proxy",
    "profit_density_score",
    "quality_score",
    "gate_a_ci_separated",
    "gate_b_time_stable",
    "gate_b_year_signs",
    "gate_c_regime_stable",
    "gate_c_stable_splits",
    "gate_c_required_splits",
    "gate_d_friction_floor",
    "gate_f_exposure_guard",
    "gate_g_net_benefit",
    "gate_h_executable_condition",
    "gate_h_executable_action",
    "gate_e_simplicity",
    "validation_delta_adverse_mean",
    "test_delta_adverse_mean",
    "val_delta_adverse_mean",
    "oos1_delta_adverse_mean",
    "val_t_stat",
    "val_p_value",
    "val_p_value_adj_bh",
    "oos1_t_stat",
    "oos1_p_value",
    "test_t_stat",
    "test_p_value",
    "test_p_value_adj_bh",
    "num_tests_event_family",
    "ess_effective",
    "ess_lag_used",
    "multiplicity_penalty",
    "expectancy_after_multiplicity",
    "expectancy_left",
    "expectancy_center",
    "expectancy_right",
    "curvature_penalty",
    "neighborhood_positive_count",
    "gate_parameter_curvature",
    "delay_expectancy_map",
    "delay_positive_ratio",
    "delay_dispersion",
    "delay_robustness_score",
    "gate_delay_robustness",
    "gate_oos_min_samples",
    "gate_oos_validation",
    "gate_oos_validation_test",
    "gate_oos_consistency_strict",
    "gate_multiplicity",
    "gate_multiplicity_strict",
    "gate_ess",
    "after_cost_expectancy_per_trade",
    "stressed_after_cost_expectancy_per_trade",
    "turnover_proxy_mean",
    "avg_dynamic_cost_bps",
    "cost_input_coverage",
    "cost_model_valid",
    "cost_ratio",
    "gate_after_cost_positive",
    "gate_after_cost_stressed_positive",
    "gate_cost_model_valid",
    "gate_cost_ratio",
    "bridge_eval_status",
    "bridge_train_after_cost_bps",
    "bridge_validation_after_cost_bps",
    "bridge_validation_stressed_after_cost_bps",
    "bridge_validation_trades",
    "bridge_effective_cost_bps_per_trade",
    "bridge_gross_edge_bps_per_trade",
    "gate_bridge_has_trades_validation",
    "gate_bridge_after_cost_positive_validation",
    "gate_bridge_after_cost_stressed_positive_validation",
    "gate_bridge_edge_cost_ratio",
    "gate_bridge_turnover_controls",
    "gate_bridge_tradable",
    "selection_score_executed",
    "gate_pass",
    "gate_all_research",
    "gate_all",
    "supporting_hypothesis_count",
    "supporting_hypothesis_ids",
    "fail_reasons",
]

SYMBOL_OUTPUT_COLUMNS = [
    "candidate_id",
    "event_type",
    "condition",
    "action",
    "symbol",
    "sample_size",
    "ev",
    "variance",
    "sharpe_like",
    "stability_score",
    "window_consistency",
    "sign_persistence",
    "drawdown_profile",
    "capacity_proxy",
    "rejection_reason_codes",
    "promotion_status",
    "deployable",
]

REJECTION_NEGATIVE_EV = "NEGATIVE_EV"
REJECTION_UNSTABLE = "UNSTABLE"
REJECTION_LOW_SAMPLE = "LOW_SAMPLE"

DEPLOYMENT_MODE_CONCENTRATE = "concentrate"
DEPLOYMENT_MODE_DIVERSIFY = "diversify"
HYPOTHESIS_QUEUE_DIR = "hypothesis_generator"
NUMERIC_CONDITION_PATTERN = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|==|>|<)\s*(-?\d+(?:\.\d+)?)\s*$")
COST_INPUT_COVERAGE_MIN = 0.80


@dataclass(frozen=True)
class Phase1EventSource:
    event_type: str
    reports_dir: str
    events_file: str
    controls_file: str
    summary_file: str


PHASE1_EVENT_SOURCES: Dict[str, Phase1EventSource] = {
    "VOL_SHOCK": Phase1EventSource(
        event_type="VOL_SHOCK",
        reports_dir="vol_shock_relaxation",
        events_file="vol_shock_relaxation_events.csv",
        controls_file="vol_shock_relaxation_controls.csv",
        summary_file="vol_shock_relaxation_summary.json",
    ),
    "FORCED_FLOW_EXHAUSTION": Phase1EventSource(
        event_type="FORCED_FLOW_EXHAUSTION",
        reports_dir="directional_exhaustion_after_forced_flow",
        events_file="directional_exhaustion_after_forced_flow_events.csv",
        controls_file="directional_exhaustion_after_forced_flow_controls.csv",
        summary_file="directional_exhaustion_after_forced_flow_summary.json",
    ),
    "CROSS_VENUE_DESYNC": Phase1EventSource(
        event_type="CROSS_VENUE_DESYNC",
        reports_dir="cross_venue_desync",
        events_file="cross_venue_desync_events.csv",
        controls_file="cross_venue_desync_controls.csv",
        summary_file="cross_venue_desync_summary.json",
    ),
    "LIQUIDITY_VACUUM": Phase1EventSource(
        event_type="LIQUIDITY_VACUUM",
        reports_dir="liquidity_vacuum",
        events_file="liquidity_vacuum_events.csv",
        controls_file="liquidity_vacuum_controls.csv",
        summary_file="liquidity_vacuum_summary.json",
    ),
    "FUNDING_EXTREME_ONSET": Phase1EventSource(
        event_type="FUNDING_EXTREME_ONSET",
        reports_dir="funding_events",
        events_file="funding_episode_events.csv",
        controls_file="funding_episode_baselines.csv",
        summary_file="funding_episode_summary.json",
    ),
    "FUNDING_PERSISTENCE_TRIGGER": Phase1EventSource(
        event_type="FUNDING_PERSISTENCE_TRIGGER",
        reports_dir="funding_events",
        events_file="funding_episode_events.csv",
        controls_file="funding_episode_baselines.csv",
        summary_file="funding_episode_summary.json",
    ),
    "FUNDING_NORMALIZATION_TRIGGER": Phase1EventSource(
        event_type="FUNDING_NORMALIZATION_TRIGGER",
        reports_dir="funding_events",
        events_file="funding_episode_events.csv",
        controls_file="funding_episode_baselines.csv",
        summary_file="funding_episode_summary.json",
    ),
    "OI_SPIKE_POSITIVE": Phase1EventSource(
        event_type="OI_SPIKE_POSITIVE",
        reports_dir="oi_shocks",
        events_file="oi_shock_events.csv",
        controls_file="oi_shock_controls.csv",
        summary_file="oi_shock_summary.json",
    ),
    "OI_SPIKE_NEGATIVE": Phase1EventSource(
        event_type="OI_SPIKE_NEGATIVE",
        reports_dir="oi_shocks",
        events_file="oi_shock_events.csv",
        controls_file="oi_shock_controls.csv",
        summary_file="oi_shock_summary.json",
    ),
    "OI_FLUSH": Phase1EventSource(
        event_type="OI_FLUSH",
        reports_dir="oi_shocks",
        events_file="oi_shock_events.csv",
        controls_file="oi_shock_controls.csv",
        summary_file="oi_shock_summary.json",
    ),
    "LIQUIDATION_CASCADE": Phase1EventSource(
        event_type="LIQUIDATION_CASCADE",
        reports_dir="liquidation_cascade",
        events_file="liquidation_cascade_events.csv",
        controls_file="liquidation_cascade_controls.csv",
        summary_file="liquidation_cascade_summary.json",
    ),
}


def _first_existing(df: pd.DataFrame, candidates: List[str]) -> str | None:
    for name in candidates:
        if name in df.columns:
            return name
    return None


def _numeric_non_negative(df: pd.DataFrame, col: str | None, n: int) -> pd.Series:
    if col is None:
        return pd.Series(0.0, index=df.index)
    out = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float).clip(lower=0.0)
    if len(out) != n:
        return pd.Series(0.0, index=df.index)
    return out


def _numeric_any(df: pd.DataFrame, col: str | None, n: int, default: float = np.nan) -> pd.Series:
    if col is None:
        return pd.Series(default, index=df.index)
    out = pd.to_numeric(df[col], errors="coerce").astype(float)
    if len(out) != n:
        return pd.Series(default, index=df.index)
    return out


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _normalize_phase1_frames(events: pd.DataFrame, controls: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    out_events = events.copy()
    out_controls = controls.copy()

    if "event_id" not in out_events.columns:
        if "parent_event_id" in out_events.columns:
            out_events["event_id"] = out_events["parent_event_id"].astype(str)
        else:
            out_events["event_id"] = [f"event_{i:08d}" for i in range(len(out_events))]

    if "event_id" not in out_controls.columns and "parent_event_id" in out_controls.columns:
        out_controls["event_id"] = out_controls["parent_event_id"].astype(str)

    if "enter_ts" not in out_events.columns:
        for col in ["anchor_ts", "timestamp"]:
            if col in out_events.columns:
                out_events["enter_ts"] = out_events[col]
                break
    if "enter_idx" not in out_events.columns and "start_idx" in out_events.columns:
        out_events["enter_idx"] = pd.to_numeric(out_events["start_idx"], errors="coerce")

    if "enter_ts" in out_events.columns:
        out_events["enter_ts"] = pd.to_datetime(out_events["enter_ts"], utc=True, errors="coerce")
    else:
        out_events["enter_ts"] = pd.NaT

    if "year" not in out_events.columns:
        out_events["year"] = out_events["enter_ts"].dt.year
    out_events["year"] = pd.to_numeric(out_events["year"], errors="coerce").fillna(0).astype(int)
    return out_events, out_controls


def _phase1_pass_status(summary_path: Path, require_phase1_pass: bool) -> Tuple[bool, bool, str]:
    phase1_pass = True
    phase1_structure_pass = True
    phase1_decision = "not_required"
    if not require_phase1_pass:
        return phase1_pass, phase1_structure_pass, phase1_decision

    if not summary_path.exists():
        return False, False, "missing_summary"

    try:
        phase1_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False, False, "invalid_summary"

    phase1_decision = str(phase1_payload.get("decision", "unknown"))
    gates = phase1_payload.get("gates", {}) if isinstance(phase1_payload, dict) else {}
    if "phase1_structure_pass" in phase1_payload:
        phase1_structure_pass = bool(phase1_payload.get("phase1_structure_pass"))
    elif isinstance(gates, dict) and "phase1_structure_pass" in gates:
        phase1_structure_pass = bool(gates.get("phase1_structure_pass"))
    elif phase1_decision.lower() in {"promote", "pass"}:
        phase1_structure_pass = True
    elif "event_count" in phase1_payload:
        phase1_structure_pass = bool(int(phase1_payload.get("event_count", 0)) > 0)
    elif "events" in phase1_payload:
        phase1_structure_pass = bool(int(phase1_payload.get("events", 0)) > 0)
    elif isinstance(phase1_payload.get("summaries"), list):
        total_events = 0
        for row in phase1_payload.get("summaries", []):
            if not isinstance(row, dict):
                continue
            total_events += int(row.get("event_count", 0) or 0)
        phase1_structure_pass = bool(total_events > 0)
    else:
        phase1_structure_pass = False
    phase1_pass = phase1_structure_pass
    return phase1_pass, phase1_structure_pass, phase1_decision


def _controls_coverage(events: pd.DataFrame, controls: pd.DataFrame) -> Tuple[float, int, int]:
    if events.empty or "event_id" not in events.columns:
        return 1.0, 0, 0
    event_ids = events["event_id"].dropna().astype(str).unique().tolist()
    total_events = int(len(event_ids))
    if total_events == 0:
        return 0.0, 0, 0
    if controls.empty or "event_id" not in controls.columns:
        return 0.0, total_events, 0

    control_ids = set(controls["event_id"].dropna().astype(str).tolist())
    matched_events = int(sum(1 for event_id in event_ids if event_id in control_ids))
    ratio = float(matched_events / total_events)
    return ratio, total_events, matched_events


def _table_text(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False)
    except ImportError:
        return df.to_string(index=False)


def _write_freeze_outputs(
    *,
    out_dir: Path,
    run_id: str,
    event_type: str,
    phase1_pass: bool,
    phase1_structure_pass: bool,
    phase1_decision: str,
    phase1_events_file_exists: bool,
    queue_exists: bool,
    matched_hypothesis_ids: List[str],
    reason: str,
    reason_message: str,
    max_conditions: int,
    max_actions: int,
    opportunity_horizon_bars: int,
    opportunity_tight_eps: float,
    min_regime_stable_splits: int,
    extra_promoted_fields: Dict[str, object] | None = None,
    extra_manifest_fields: Dict[str, object] | None = None,
) -> None:
    ensure_dir(out_dir)
    cand_path = out_dir / "phase2_candidates.csv"
    costed_path = out_dir / "candidates_costed.jsonl"
    prom_path = out_dir / "promoted_candidates.json"
    symbol_eval_path = out_dir / "phase2_symbol_evaluation.csv"
    manifest_path = out_dir / "phase2_manifests.json"
    summary_path = out_dir / "phase2_summary.md"

    pd.DataFrame(columns=PRIMARY_OUTPUT_COLUMNS).to_csv(cand_path, index=False)
    costed_path.write_text("", encoding="utf-8")
    pd.DataFrame(columns=SYMBOL_OUTPUT_COLUMNS).to_csv(symbol_eval_path, index=False)

    prom_payload: Dict[str, object] = {
        "run_id": run_id,
        "event_type": event_type,
        "decision": "freeze",
        "phase1_pass": bool(phase1_pass),
        "phase1_structure_pass": bool(phase1_structure_pass),
        "phase1_decision": phase1_decision,
        "promoted_count": 0,
        "deployment_mode": DEPLOYMENT_MODE_DIVERSIFY,
        "deployment_mode_rationale": reason_message,
        "deployment_dispersion_test": {
            "deployment_mode": DEPLOYMENT_MODE_DIVERSIFY,
            "rationale": reason_message,
            "dominance_detected": False,
            "top_symbols_considered": [],
            "dispersion_metrics": {
                "score_gap": 0.0,
                "relative_gap": 0.0,
                "confidence_overlap": True,
                "stability_adjusted_spread": 0.0,
            },
        },
        "candidates": [],
        "reason": str(reason),
        "phase1_events_file_exists": bool(phase1_events_file_exists),
        "phase1_hypothesis_queue_exists": bool(queue_exists),
        "matched_hypothesis_count": int(len(matched_hypothesis_ids)),
        "matched_hypothesis_ids": matched_hypothesis_ids,
        "selection_split_policy": {
            "canonical_labels": ["train", "val", "oos1"],
            "legacy_labels_retained": ["train", "validation", "test"],
            "selection_split": "validation",
            "test_usage": "read_only",
        },
    }
    if isinstance(extra_promoted_fields, dict):
        prom_payload.update(extra_promoted_fields)
    prom_path.write_text(json.dumps(prom_payload, indent=2), encoding="utf-8")

    manifest_payload: Dict[str, object] = {
        "run_id": run_id,
        "event_type": event_type,
        "phase1_pass": bool(phase1_pass),
        "phase1_structure_pass": bool(phase1_structure_pass),
        "phase1_decision": phase1_decision,
        "conditions_generated": 0,
        "actions_generated": 0,
        "conditions_evaluated": 0,
        "actions_evaluated": 0,
        "candidates_evaluated": 0,
        "caps": {
            "max_conditions": int(max_conditions),
            "max_actions": int(max_actions),
            "condition_cap_pass": True,
            "action_cap_pass": True,
            "simplicity_gate_pass": True,
        },
        "opportunity": {
            "horizon_bars": int(opportunity_horizon_bars),
            "tight_ci_eps": float(opportunity_tight_eps),
        },
        "gates": {
            "min_regime_stable_splits": int(min_regime_stable_splits),
        },
        "no_phase1_events": bool(reason == "no_phase1_events"),
        "phase1_events_file_exists": bool(phase1_events_file_exists),
        "phase1_hypothesis_queue_exists": bool(queue_exists),
        "matched_hypothesis_count": int(len(matched_hypothesis_ids)),
        "deployment_mode": DEPLOYMENT_MODE_DIVERSIFY,
        "deployment_mode_rationale": reason_message,
        "freeze_reason": str(reason),
        "selection_split_policy": {
            "canonical_labels": ["train", "val", "oos1"],
            "legacy_labels_retained": ["train", "validation", "test"],
            "selection_split": "validation",
            "test_usage": "read_only",
        },
    }
    if isinstance(extra_manifest_fields, dict):
        manifest_payload.update(extra_manifest_fields)
    manifest_path.write_text(json.dumps(manifest_payload, indent=2, encoding="utf-8")) # Added closing parenthesis
    summary_path.write_text(
        "\n".join(
            [
                "# Phase 2 Conditional Edge Hypothesis",
                "",
                f"Run ID: `{run_id}`",
                f"Event type: `{event_type}`",
                "Decision: **FREEZE**",
                f"Phase 1 status: `{phase1_decision}`",
                "",
                reason_message,
                f"Hypothesis queue present: {bool(queue_exists)}",
                f"Matched hypotheses for this event family: {int(len(matched_hypothesis_ids))}",
                f"Deployment mode: {DEPLOYMENT_MODE_DIVERSIFY}",
                f"Freeze reason: `{reason}`",
            ]
        ),
        encoding="utf-8",
    )


def _read_csv_allow_empty(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _parse_string_list(value: object) -> List[str]:
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(text)
            except Exception:
                continue
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
    if "|" in text:
        return [x.strip() for x in text.split("|") if x.strip()]
    return [text]


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    if not np.isfinite(out):
        return float(default)
    return out


def _parse_delay_grid(value: str) -> List[int]:
    parts = [part.strip() for part in str(value).split(",") if part.strip()]
    delays: List[int] = []
    seen = set()
    for part in parts:
        try:
            delay = int(part)
        except ValueError as exc:
            raise ValueError(f"Invalid delay grid entry: `{part}`") from exc
        if delay < 0:
            raise ValueError("Delay grid entries must be >= 0")
        if delay not in seen:
            delays.append(delay)
            seen.add(delay)
    if not delays:
        raise ValueError("Delay grid must include at least one integer entry")
    return sorted(delays)


def _parse_numeric_condition_expr(condition: str) -> Tuple[str, str, float] | None:
    match = NUMERIC_CONDITION_PATTERN.match(str(condition or "").strip())
    if not match:
        return None
    feature, operator, raw_value = match.groups()
    try:
        value = float(raw_value)
    except ValueError:
        return None
    return feature, operator, value


def _load_phase1_hypothesis_queue(run_id: str) -> Tuple[bool, pd.DataFrame]:
    queue_root = DATA_ROOT / "reports" / HYPOTHESIS_QUEUE_DIR / run_id
    csv_path = queue_root / "phase1_hypothesis_queue.csv"
    jsonl_path = queue_root / "phase1_hypothesis_queue.jsonl"
    queue_exists = csv_path.exists() or jsonl_path.exists()
    if not queue_exists:
        return False, pd.DataFrame(columns=["hypothesis_id", "priority_score", "target_phase2_event_types"])

    if csv_path.exists():
        queue = _read_csv_allow_empty(csv_path)
    else:
        rows: List[Dict[str, object]] = []
        with jsonl_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                raw = line.strip()
                if not raw:
                    continue
                payload = json.loads(raw)
                if isinstance(payload, dict):
                    rows.append(payload)
        queue = pd.DataFrame(rows)

    if queue.empty:
        return True, pd.DataFrame(columns=["hypothesis_id", "priority_score", "target_phase2_event_types"])

    if "hypothesis_id" not in queue.columns:
        queue["hypothesis_id"] = [f"H{i:04d}" for i in range(len(queue))]
    queue["hypothesis_id"] = queue["hypothesis_id"].astype(str).str.strip()
    queue = queue[queue["hypothesis_id"] != ""].copy()
    queue["priority_score"] = pd.to_numeric(queue.get("priority_score"), errors="coerce").fillna(0.0)
    if "target_phase2_event_types" not in queue.columns:
        queue["target_phase2_event_types"] = [[] for _ in range(len(queue))]
    queue["target_phase2_event_types"] = queue["target_phase2_event_types"].apply(_parse_string_list)
    queue = queue.sort_values(["priority_score", "hypothesis_id"], ascending=[False, True]).reset_index(drop=True)
    return True, queue


def _supporting_hypothesis_ids(queue: pd.DataFrame, event_type: str, max_ids: int = 10) -> List[str]:
    if queue.empty:
        return []
    event_key = str(event_type).strip()
    matched = queue[
        queue["target_phase2_event_types"].apply(
            lambda values: event_key in [str(x).strip() for x in values]
        )
    ]
    if matched.empty:
        return []
    return matched["hypothesis_id"].astype(str).dropna().tolist()[:max_ids]


@dataclass(frozen=True)
class ConditionSpec:
    name: str
    description: str
    mask_fn: Callable[[pd.DataFrame], pd.Series]


@dataclass(frozen=True)
class ActionSpec:
    name: str
    family: str
    params: Dict[str, object]


def _bootstrap_ci(values: np.ndarray, n_boot: int, seed: int, alpha: float = 0.05) -> Tuple[float, float, float]:
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    if len(arr) == 0:
        return np.nan, np.nan, np.nan
    rng = np.random.default_rng(seed)
    boots = []
    for _ in range(n_boot):
        sample = rng.choice(arr, size=len(arr), replace=True)
        boots.append(float(np.mean(sample)))
    return float(np.mean(arr)), float(np.quantile(boots, alpha / 2.0)), float(np.quantile(boots, 1.0 - alpha / 2.0))


def _build_conditions(events: pd.DataFrame) -> List[ConditionSpec]:
    conds: List[ConditionSpec] = [ConditionSpec("all", "all events", lambda d: pd.Series(True, index=d.index))]

    if "symbol" in events.columns:
        for symbol in sorted(events["symbol"].dropna().astype(str).unique().tolist()):
            conds.append(
                ConditionSpec(
                    name=f"symbol_{symbol}",
                    description=f"symbol == {symbol}",
                    mask_fn=lambda d, sym=symbol: d["symbol"].astype(str) == sym,
                )
            )

    if "bull_bear" in events.columns:
        for bb in sorted(events["bull_bear"].dropna().astype(str).unique().tolist()):
            conds.append(
                ConditionSpec(
                    name=f"bull_bear_{bb}",
                    description=f"bull_bear == {bb}",
                    mask_fn=lambda d, v=bb: d["bull_bear"].astype(str) == v,
                )
            )

    if "vol_regime" in events.columns:
        for vr in sorted(events["vol_regime"].dropna().astype(str).unique().tolist()):
            conds.append(
                ConditionSpec(
                    name=f"vol_regime_{vr}",
                    description=f"vol_regime == {vr}",
                    mask_fn=lambda d, v=vr: d["vol_regime"].astype(str) == v,
                )
            )

    if "tod_bucket" in events.columns:
        conds.extend(
            [
                ConditionSpec("session_asia", "enter hour in [0,7]", lambda d: d["tod_bucket"].between(0, 7, inclusive="both")),
                ConditionSpec("session_eu", "enter hour in [8,15]", lambda d: d["tod_bucket"].between(8, 15, inclusive="both")),
                ConditionSpec("session_us", "enter hour in [16,23]", lambda d: d["tod_bucket"].between(16, 23, inclusive="both")),
            ]
        )
    elif "anchor_hour" in events.columns:
        conds.extend(
            [
                ConditionSpec("session_asia", "enter hour in [0,7]", lambda d: d["anchor_hour"].between(0, 7, inclusive="both")),
                ConditionSpec("session_eu", "enter hour in [8,15]", lambda d: d["anchor_hour"].between(8, 15, inclusive="both")),
                ConditionSpec("session_us", "enter hour in [16,23]", lambda d: d["anchor_hour"].between(16, 23, inclusive="both")),
            ]
        )

    if "t_rv_peak" in events.columns:
        conds.extend(
            [
                ConditionSpec("age_bucket_0_8", "t_rv_peak in [0,8]", lambda d: d["t_rv_peak"].fillna(10**9).between(0, 8, inclusive="both")),
                ConditionSpec("age_bucket_9_30", "t_rv_peak in [9,30]", lambda d: d["t_rv_peak"].fillna(10**9).between(9, 30, inclusive="both")),
                ConditionSpec("age_bucket_31_96", "t_rv_peak in [31,96]", lambda d: d["t_rv_peak"].fillna(10**9).between(31, 96, inclusive="both")),
            ]
        )
    if "rv_decay_half_life" in events.columns:
        conds.append(
            ConditionSpec("near_half_life", "rv_decay_half_life <= 30", lambda d: d["rv_decay_half_life"].fillna(10**9) <= 30)
        )
    if {"t_rv_peak", "duration_bars"}.issubset(events.columns):
        conds.extend(
            [
                ConditionSpec(
                    "fractional_age_0_33",
                    "t_rv_peak / duration_bars <= 0.33",
                    lambda d: (d["t_rv_peak"].fillna(10**9) / d["duration_bars"].replace(0, np.nan)).fillna(10**9) <= 0.33,
                ),
                ConditionSpec(
                    "fractional_age_34_66",
                    "t_rv_peak / duration_bars in (0.33, 0.66]",
                    lambda d: (
                        (d["t_rv_peak"].fillna(10**9) / d["duration_bars"].replace(0, np.nan)).fillna(10**9) > 0.33
                    )
                    & ((d["t_rv_peak"].fillna(10**9) / d["duration_bars"].replace(0, np.nan)).fillna(10**9) <= 0.66),
                ),
                ConditionSpec(
                    "fractional_age_67_100",
                    "t_rv_peak / duration_bars > 0.66",
                    lambda d: (d["t_rv_peak"].fillna(10**9) / d["duration_bars"].replace(0, np.nan)).fillna(10**9) > 0.66,
                ),
            ]
        )

    # de-dup by name preserving order
    seen = set()
    out = []
    for c in conds:
        if c.name in seen:
            continue
        seen.add(c.name)
        out.append(c)
    return out


def _build_actions() -> List[ActionSpec]:
    return [
        ActionSpec("no_action", "baseline", {}),
        ActionSpec("entry_gate_skip", "entry_gating", {"k": 0.0}),
        ActionSpec("risk_throttle_0.5", "risk_throttle", {"k": 0.5}),
        ActionSpec("risk_throttle_0", "risk_throttle", {"k": 0.0}),
        ActionSpec("delay_0", "timing", {"delay_bars": 0}),
        ActionSpec("delay_8", "timing", {"delay_bars": 8}),
        ActionSpec("delay_30", "timing", {"delay_bars": 30}),
        ActionSpec("reenable_at_half_life", "timing", {"landmark": "rv_decay_half_life"}),
    ]


def _candidate_type_from_action(action_name: str) -> str:
    action = str(action_name or "").strip().lower()
    if action == "entry_gate_skip" or action.startswith("risk_throttle_"):
        return "overlay"
    if action == "no_action" or action.startswith("delay_") or action.name == "reenable_at_half_life":
        return "standalone"
    return "standalone"


def _assign_candidate_types_and_overlay_bases(candidates: pd.DataFrame, event_type: str) -> pd.DataFrame:
    if candidates.empty:
        return candidates
    out = candidates.copy()
    action_series = out["action"] if "action" in out.columns else pd.Series("", index=out.index, dtype=str)
    out["candidate_type"] = action_series.astype(str).map(_candidate_type_from_action)
    out["overlay_base_candidate_id"] = ""

    no_action_rows = out[action_series.astype(str) == "no_action"]
    base_by_condition: Dict[str, str] = {}
    for _, row in no_action_rows.iterrows():
        cond = str(row.get("condition", "")).strip()
        candidate_id = str(row.get("candidate_id", "")).strip()
        if cond and candidate_id and cond not in base_by_condition:
            base_by_condition[cond] = candidate_id
    fallback_base = f"BASE_TEMPLATE::{str(event_type).strip().lower()}"
    overlay_mask = out["candidate_type"].astype(str) == "overlay"
    for idx in out[overlay_mask].index:
        condition = str(out.at[idx, "condition"]).strip() if "condition" in out.columns else ""
        out.at[idx, "overlay_base_candidate_id"] = base_by_condition.get(condition, fallback_base)
    return out


def _attach_forward_opportunity(
    events: pd.DataFrame,
    controls: pd.DataFrame,
    run_id: str,
    symbols: List[str],
    timeframe: str,
    horizon_bars: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if events.empty:
        return events, controls

    out_events = events.copy()
    out_controls = controls.copy()
    if "enter_ts" not in out_events.columns:
        for col in ["anchor_ts", "timestamp"]:
            if col in out_events.columns:
                out_events["enter_ts"] = out_events[col]
                break
    if "enter_idx" not in out_events.columns and "start_idx" in out_events.columns:
        out_events["enter_idx"] = pd.to_numeric(out_events["start_idx"], errors="coerce")
    out_events["enter_ts"] = pd.to_datetime(out_events.get("enter_ts"), utc=True, errors="coerce")

    rows = []
    for symbol in symbols:
        bars_candidates = [
            run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", "perp", symbol, f"bars_{timeframe}"),
            DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / f"bars_{timeframe}",
        ]
        bars_dir = choose_partition_dir(bars_candidates)
        bars = read_parquet(list_parquet_files(bars_dir)) if bars_dir else pd.DataFrame()
        if bars.empty:
            continue
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True, errors="coerce")
        bars = bars.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        dupes = int(bars["timestamp"].duplicated(keep="last").sum())
        if dupes > 0:
            logging.warning(
                "Dropping %s duplicate bars for %s (%s) before forward opportunity join.",
                dupes,
                symbol,
                timeframe,
            )
            bars = bars.drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
        close = bars["close"].astype(float)
        fwd_abs_return = (close.shift(-horizon_bars) / close - 1.0).abs()
        rows.append(
            pd.DataFrame(
                {
                    "symbol": symbol,
                    "bar_idx": np.arange(len(bars), dtype=int),
                    "timestamp": bars["timestamp"],
                    "forward_abs_return_h": fwd_abs_return,
                }
            )
        )

    if not rows:
        out_events["forward_abs_return_h"] = np.nan
        out_events["forward_abs_return_h_ctrl"] = np.nan
        out_events["opportunity_value_excess"] = np.nan
        return out_events, out_controls

    fwd = pd.concat(rows, ignore_index=True)
    fwd_ts = fwd.rename(columns={"timestamp": "enter_ts"})

    out_events = out_events.merge(
        fwd_ts[["symbol", "enter_ts", "forward_abs_return_h"]],
        on=["symbol", "enter_ts"],
        how="left",
        validate="many_to_one",
    )
    if "enter_idx" in out_events.columns:
        out_events = out_events.merge(
            fwd[["symbol", "bar_idx", "forward_abs_return_h"]].rename(columns={"bar_idx": "enter_idx", "forward_abs_return_h": "forward_abs_return_h_idx"}),
            on=["symbol", "enter_idx"],
            how="left",
            validate="many_to_one",
        )
        out_events["forward_abs_return_h"] = out_events["forward_abs_return_h"].where(
            out_events["forward_abs_return_h"].notna(),
            out_events["forward_abs_return_h_idx"],
        )
        out_events = out_events.drop(columns=["forward_abs_return_h_idx"])

    if not out_controls.empty and "event_id" in out_controls.columns and "control_idx" in out_controls.columns:
        event_to_symbol = out_events[["event_id", "symbol"]].drop_duplicates()
        if "symbol" not in out_controls.columns:
            out_controls = out_controls.merge(event_to_symbol, on="event_id", how="left", validate="many_to_one")
        else:
            out_controls = out_controls.merge(
                event_to_symbol.rename(columns={"symbol": "event_symbol"}),
                on="event_id",
                how="left",
                validate="many_to_one",
            )
            out_controls["symbol"] = out_controls["symbol"].where(
                out_controls["symbol"].notna(),
                out_controls["event_symbol"],
            )
            out_controls = out_controls.drop(columns=["event_symbol"])

        if "symbol" in out_controls.columns:
            out_controls = out_controls.merge(
                fwd[["symbol", "bar_idx", "forward_abs_return_h"]].rename(
                    columns={"bar_idx": "control_idx", "forward_abs_return_h": "forward_abs_return_h_ctrl_row"}
                ),
                on=["symbol", "control_idx"],
                how="left",
                validate="many_to_one",
            )
            ctrl_mean = out_controls.groupby("event_id", as_index=False)["forward_abs_return_h_ctrl_row"].mean()
            out_events = out_events.merge(
                ctrl_mean.rename(columns={"forward_abs_return_h_ctrl_row": "forward_abs_return_h_ctrl"}),
                on="event_id",
                how="left",
            )
        else:
            out_events["forward_abs_return_h_ctrl"] = np.nan
    else:
        out_events["forward_abs_return_h_ctrl"] = np.nan

    out_events["opportunity_value_excess"] = out_events["forward_abs_return_h"] - out_events["forward_abs_return_h_ctrl"]
    out_events["opportunity_value_excess"] = out_events["opportunity_value_excess"].where(
        out_events["opportunity_value_excess"].notna(),
        out_events["forward_abs_return_h"],
    )
    return out_events, out_controls


def _attach_event_market_features(
    events: pd.DataFrame,
    run_id: str,
    symbols: List[str],
    timeframe: str = "5m",
) -> pd.DataFrame:
    if events.empty:
        return events

    out = events.copy()
    out["enter_ts"] = pd.to_datetime(out.get("enter_ts"), utc=True, errors="coerce")
    if out["enter_ts"].isna().all():
        return out

    context_rows: List[pd.DataFrame] = []
    for symbol in symbols:
        features_candidates = [
            run_scoped_lake_path(DATA_ROOT, run_id, "features", "perp", symbol, timeframe, "features_v1"),
            DATA_ROOT / "lake" / "features" / "perp" / symbol / timeframe / "features_v1",
        ]
        bars_candidates = [
            run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", "perp", symbol, f"bars_{timeframe}"),
            DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / f"bars_{timeframe}",
        ]

        features_src = choose_partition_dir(features_candidates)
        features = read_parquet(list_parquet_files(features_src)) if features_src else pd.DataFrame()
        bars_src = choose_partition_dir(bars_candidates)
        bars = read_parquet(list_parquet_files(bars_src)) if bars_src else pd.DataFrame()

        # Load market_state context
        context_candidates = [
            run_scoped_lake_path(DATA_ROOT, run_id, "context", "market_state", symbol, timeframe),
            DATA_ROOT / "lake" / "context" / "market_state" / symbol / timeframe,
        ]
        context_src = choose_partition_dir(context_candidates)
        market_state = read_parquet(list_parquet_files(context_src)) if context_src else pd.DataFrame()

        if features.empty and bars.empty and market_state.empty:
            continue

        if "timestamp" in features.columns:
            features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True, errors="coerce")
        else:
            features["timestamp"] = pd.NaT
        if "timestamp" in bars.columns:
            bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True, errors="coerce")
        else:
            bars["timestamp"] = pd.NaT

        feature_cols = ["timestamp", "spread_bps", "atr_14", "quote_volume", "funding_rate_scaled", "close", "high", "low"]
        feat = features[[col for col in feature_cols if col in features.columns]].copy()
        if feat.empty:
            feat = pd.DataFrame({"timestamp": pd.Series(dtype="datetime64[ns, UTC]")})
        if "timestamp" not in feat.columns:
            feat["timestamp"] = pd.NaT

        bar_cols = ["timestamp", "close", "high", "low", "quote_volume"]
        bar_view = bars[[col for col in bar_cols if col in bars.columns]].copy()
        if not bar_view.empty:
            feat = feat.merge(bar_view, on="timestamp", how="outer", suffixes=("", "_bar"))
            for col in ["close", "high", "low", "quote_volume"]:
                bar_col = f"{col}_bar"
                if bar_col in feat.columns:
                    if col not in feat.columns:
                        feat[col] = feat[bar_col]
                    else:
                        feat[col] = feat[col].where(feat[col].notna(), feat[bar_col])
                    feat = feat.drop(columns=[bar_col])

        # Merge market_state
        state_cols = ["timestamp", "vol_regime", "vol_regime_code", "carry_state", "carry_state_code"]
        state_view = market_state[[col for col in state_cols if col in market_state.columns]].copy()
        if not state_view.empty:
            feat = feat.merge(state_view, on="timestamp", how="outer")

        feat["symbol"] = str(symbol).upper()
        feat["enter_ts"] = pd.to_datetime(feat["timestamp"], utc=True, errors="coerce")
        feat = feat.dropna(subset=["enter_ts"]).drop_duplicates(subset=["symbol", "enter_ts"], keep="last")
        keep_cols = [
            "symbol",
            "enter_ts",
            "spread_bps",
            "atr_14",
            "quote_volume",
            "funding_rate_scaled",
            "close",
            "high",
            "low",
            "vol_regime",
            "vol_regime_code",
            "carry_state",
            "carry_state_code",
        ]
        feat = feat[[col for col in keep_cols if col in feat.columns]]
        context_rows.append(feat)

    if not context_rows:
        return out

    context = pd.concat(context_rows, ignore_index=True).drop_duplicates(subset=["symbol", "enter_ts"], keep="last")
    merged = out.merge(context, on=["symbol", "enter_ts"], how="left", suffixes=("", "_ctx"))
    for col in ["spread_bps", "atr_14", "quote_volume", "funding_rate_scaled", "close", "high", "low", "vol_regime", "vol_regime_code", "carry_state", "carry_state_code"]:
        ctx_col = f"{col}_ctx"
        if ctx_col in merged.columns:
            if col not in merged.columns:
                merged[col] = merged[ctx_col]
            else:
                merged[col] = merged[col].where(merged[col].notna(), merged[ctx_col])
            merged = merged.drop(columns=[ctx_col])
    return merged


def _turnover_proxy_for_action(action: ActionSpec, n: int) -> np.ndarray:
    if n <= 0:
        return np.array([], dtype=float)
    if action.name == "entry_gate_skip":
        return np.full(n, 0.0, dtype=float)
    if action.family == "risk_throttle" or action.name.startswith("risk_throttle_"):
        raw_scale = action.params.get("k")
        if raw_scale is None and action.name.startswith("risk_throttle_"):
            try:
                raw_scale = float(action.name.split("_")[-1])
            except ValueError:
                raw_scale = 1.0
        k = float(raw_scale if raw_scale is not None else 1.0)
        return np.full(n, max(0.0, min(1.0, k)), dtype=float)
    if action.family == "entry_gating":
        return np.full(n, 1.0, dtype=float)
    if action.name.startswith("delay_") or action.name == "reenable_at_half_life":
        return np.full(n, 1.0, dtype=float)
    if action.name == "no_action":
        return np.full(n, 1.0, dtype=float)
    return np.full(n, 1.0, dtype=float)


def _candidate_cost_fields(
    *,
    sub: pd.DataFrame,
    action: ActionSpec,
    expectancy_per_trade: float,
    execution_cost_config: Dict[str, float],
    stressed_cost_multiplier: float,
) -> Dict[str, float | bool]:
    n = int(len(sub))
    turnover = _turnover_proxy_for_action(action=action, n=n)
    if n <= 0 or turnover.size == 0:
        return {
            "after_cost_expectancy_per_trade": float(expectancy_per_trade),
            "stressed_after_cost_expectancy_per_trade": float(expectancy_per_trade),
            "turnover_proxy_mean": 0.0,
            "avg_dynamic_cost_bps": 0.0,
            "cost_input_coverage": 0.0,
            "cost_model_valid": False,
            "cost_ratio": 0.0,
            "gate_after_cost_positive": bool(expectancy_per_trade > 0.0),
            "gate_after_cost_stressed_positive": bool(expectancy_per_trade > 0.0),
            "gate_cost_model_valid": False,
            "gate_cost_ratio": True,
        }

    idx = sub.index
    spread = pd.to_numeric(sub.get("spread_bps", pd.Series(np.nan, index=idx)), errors="coerce")
    atr = pd.to_numeric(sub.get("atr_14", pd.Series(np.nan, index=idx)), errors="coerce")
    quote_volume = pd.to_numeric(sub.get("quote_volume", pd.Series(np.nan, index=idx)), errors="coerce")
    close = pd.to_numeric(sub.get("close", pd.Series(np.nan, index=idx)), errors="coerce")
    high = pd.to_numeric(sub.get("high", pd.Series(np.nan, index=idx)), errors="coerce")
    low = pd.to_numeric(sub.get("low", pd.Series(np.nan, index=idx)), errors="coerce")
    coverage_components = [spread, quote_volume, close, high, low]
    coverage_values = [float(comp.notna().mean()) for comp in coverage_components]
    cost_input_coverage = float(np.nanmean(coverage_values)) if coverage_values else 0.0

    frame = pd.DataFrame(
        {
            "spread_bps": spread.fillna(0.0),
            "atr_14": atr,
            "quote_volume": quote_volume,
            "close": close,
            "high": high,
            "low": low,
        },
        index=idx,
    )
    cost_bps_series = estimate_transaction_cost_bps(
        frame=frame,
        turnover=pd.Series(turnover, index=idx, dtype=float),
        config=dict(execution_cost_config),
    )
    cost_values = cost_bps_series.to_numpy(dtype=float)
    finite_cost = bool(cost_values.size > 0 and np.isfinite(cost_values).all())
    cost_model_valid = bool(cost_input_coverage >= COST_INPUT_COVERAGE_MIN and finite_cost)
    cost_per_trade = float(np.nanmean((cost_bps_series.to_numpy(dtype=float) * turnover) / 10_000.0))
    cost_per_trade = max(0.0, cost_per_trade)
    after_cost = float(expectancy_per_trade - cost_per_trade)
    stressed_after_cost = float(expectancy_per_trade - (float(stressed_cost_multiplier) * cost_per_trade))
    gross_proxy = max(1e-9, abs(float(expectancy_per_trade)) + cost_per_trade)
    cost_ratio = float(min(2.0, max(0.0, cost_per_trade / gross_proxy)))
    return {
        "after_cost_expectancy_per_trade": after_cost,
        "stressed_after_cost_expectancy_per_trade": stressed_after_cost,
        "turnover_proxy_mean": float(np.nanmean(turnover)),
        "avg_dynamic_cost_bps": float(np.nanmean(cost_bps_series.to_numpy(dtype=float))),
        "cost_input_coverage": cost_input_coverage,
        "cost_model_valid": cost_model_valid,
        "cost_ratio": cost_ratio,
        "gate_after_cost_positive": bool(after_cost > 0.0),
        "gate_after_cost_stressed_positive": bool(stressed_after_cost > 0.0),
        "gate_cost_model_valid": cost_model_valid,
        "gate_cost_ratio": bool(cost_ratio < 0.60),
    }


def _assert_registry_event_count_parity(run_id: str, event_type: str, symbols: List[str], events: pd.DataFrame) -> None:
    spec = EVENT_REGISTRY_SPECS.get(str(event_type))
    if spec is not None:
        phase1_count = int(len(normalize_phase1_events(events=events, spec=spec, run_id=run_id)))
    else:
        phase1_count = int(len(events))
    registry = load_registry_events(data_root=DATA_ROOT, run_id=run_id, event_type=event_type, symbols=symbols)
    registry_count = int(len(registry))
    if phase1_count != registry_count:
        raise ValueError(
            f"Event registry parity check failed for event_type={event_type}: "
            f"phase1_count={phase1_count} registry_count={registry_count}. "
            "Rebuild registry before running phase2."
        )


def _prepare_baseline(events: pd.DataFrame, controls: pd.DataFrame) -> pd.DataFrame:
    out = events.copy()
    out["baseline_mode"] = "event_proxy_only"

    adverse_binary_col = _first_existing(out, ["secondary_shock_within_h", "secondary_shock_within", "tail_move_within"])
    adverse_mag_col = _first_existing(out, ["range_pct_96", "range_expansion"])
    opportunity_col = _first_existing(out, ["relaxed_within_96", "forward_abs_return_h"])

    adverse_binary = _numeric_non_negative(out, adverse_binary_col, n=len(out))
    adverse_mag = _numeric_non_negative(out, adverse_mag_col, n=len(out))
    if adverse_binary_col and adverse_mag_col:
        out["adverse_proxy"] = 0.5 * adverse_binary + 0.5 * adverse_mag
    elif adverse_binary_col:
        out["adverse_proxy"] = adverse_binary
    elif adverse_mag_col:
        out["adverse_proxy"] = adverse_mag
    else:
        out["adverse_proxy"] = 0.0

    if opportunity_col:
        out["opportunity_proxy"] = _numeric_non_negative(out, opportunity_col, n=len(out))
    elif adverse_binary_col:
        out["opportunity_proxy"] = (1.0 - adverse_binary).clip(lower=0.0)
    else:
        out["opportunity_proxy"] = 0.0

    time_to_adverse_col = _first_existing(out, ["time_to_secondary_shock", "time_to_tail_move"])
    timing_landmark_col = _first_existing(out, ["rv_decay_half_life", "parent_time_to_relax", "time_to_relax"])
    out["time_to_adverse"] = _numeric_any(out, time_to_adverse_col, n=len(out), default=np.nan)
    out["timing_landmark"] = _numeric_any(out, timing_landmark_col, n=len(out), default=np.nan)

    if controls.empty or "event_id" not in controls.columns or "event_id" not in out.columns:
        out["adverse_proxy_ctrl"] = np.nan
        out["opportunity_proxy_ctrl"] = np.nan
        out["adverse_proxy_excess"] = out["adverse_proxy"]
        out["opportunity_proxy_excess"] = out["opportunity_proxy"]
        return out

    numeric_ctrl = controls.groupby("event_id", as_index=False).mean(numeric_only=True)
    ctrl_cols = [
        c
        for c in [
            "secondary_shock_within_h",
            "secondary_shock_within",
            "tail_move_within",
            "range_pct_96",
            "range_expansion",
            "relaxed_within_96",
            "forward_abs_return_h",
            "time_to_secondary_shock",
            "time_to_tail_move",
            "rv_decay_half_life",
            "parent_time_to_relax",
            "time_to_relax",
        ]
        if c in numeric_ctrl.columns
    ]
    if not ctrl_cols:
        out["adverse_proxy_ctrl"] = np.nan
        out["opportunity_proxy_ctrl"] = np.nan
        out["adverse_proxy_excess"] = out["adverse_proxy"]
        out["opportunity_proxy_excess"] = out["opportunity_proxy"]
        return out

    rename_map = {c: f"{c}_ctrl" for c in ctrl_cols}
    merged = out.merge(numeric_ctrl[["event_id"] + ctrl_cols].rename(columns=rename_map), on="event_id", how="left")

    adverse_binary_ctrl_col = _first_existing(merged, ["secondary_shock_within_h_ctrl", "secondary_shock_within_ctrl", "tail_move_within_ctrl"])
    adverse_mag_ctrl_col = _first_existing(merged, ["range_pct_96_ctrl", "range_expansion_ctrl"])
    opportunity_ctrl_col = _first_existing(merged, ["relaxed_within_96_ctrl", "forward_abs_return_h_ctrl"])

    adverse_binary_ctrl = _numeric_non_negative(merged, adverse_binary_ctrl_col, n=len(merged))
    adverse_mag_ctrl = _numeric_non_negative(merged, adverse_mag_ctrl_col, n=len(merged))
    if adverse_binary_ctrl_col and adverse_mag_ctrl_col:
        merged["adverse_proxy_ctrl"] = 0.5 * adverse_binary_ctrl + 0.5 * adverse_mag_ctrl
    elif adverse_binary_ctrl_col:
        merged["adverse_proxy_ctrl"] = adverse_binary_ctrl
    elif adverse_mag_ctrl_col:
        merged["adverse_proxy_ctrl"] = adverse_mag_ctrl
    else:
        merged["adverse_proxy_ctrl"] = np.nan

    if opportunity_ctrl_col:
        merged["opportunity_proxy_ctrl"] = _numeric_non_negative(merged, opportunity_ctrl_col, n=len(merged))
    else:
        merged["opportunity_proxy_ctrl"] = np.nan

    if "time_to_adverse" not in merged.columns:
        merged["time_to_adverse"] = _numeric_any(merged, _first_existing(merged, ["time_to_secondary_shock", "time_to_tail_move"]), n=len(merged), default=np.nan)
    if "timing_landmark" not in merged.columns:
        merged["timing_landmark"] = _numeric_any(merged, _first_existing(merged, ["rv_decay_half_life", "parent_time_to_relax", "time_to_relax"]), n=len(merged), default=np.nan)

    merged["adverse_proxy_excess"] = (merged["adverse_proxy"] - merged["adverse_proxy_ctrl"]).clip(lower=0.0)
    merged["opportunity_proxy_excess"] = (merged["opportunity_proxy"] - merged["opportunity_proxy_ctrl"]).clip(lower=0.0)
    merged["baseline_mode"] = "matched_controls_excess"
    return merged


def _apply_action_proxy(sub: pd.DataFrame, action: ActionSpec) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    # Returns per-event contributions for: adverse_delta, opportunity_delta, exposure_delta
    # Negative adverse_delta is improvement (risk reduction).
    if "adverse_proxy_excess" in sub.columns:
        adverse = sub["adverse_proxy_excess"].fillna(0).astype(float).to_numpy()
    else:
        sec = sub["secondary_shock_within_h"].fillna(0).astype(float).to_numpy()
        rng = sub["range_pct_96"].fillna(0).astype(float).to_numpy()
        adverse = 0.5 * sec + 0.5 * np.clip(rng, 0.0, None)

    if "opportunity_value_excess" in sub.columns:
        opp_value = sub["opportunity_value_excess"].fillna(0).astype(float).to_numpy()
    elif "opportunity_proxy_excess" in sub.columns:
        opp_value = sub["opportunity_proxy_excess"].fillna(0).astype(float).to_numpy()
    else:
        opp_value = sub["relaxed_within_96"].fillna(0).astype(float).to_numpy()

    if action.name in {"no_action", "delay_0"}:
        return np.zeros(len(sub), dtype=float), np.zeros(len(sub), dtype=float), np.zeros(len(sub), dtype=float)

    if action.family in {"entry_gating", "risk_throttle"}:
        k = float(action.params.get("k", 1.0))
        exposure_delta = np.full(len(sub), -(1.0 - k), dtype=float)
        adverse_delta = -(1.0 - k) * adverse
        opportunity_delta = exposure_delta * opp_value
        return adverse_delta, opportunity_delta, exposure_delta

    if action.name.startswith("delay_"):
        d = int(action.params.get("delay_bars", 0))
        t_adverse = _numeric_any(
            sub,
            _first_existing(sub, ["time_to_adverse", "time_to_secondary_shock", "time_to_tail_move"]),
            n=len(sub),
            default=10**9,
        ).fillna(10**9).to_numpy()
        adverse_delta = -(t_adverse <= d).astype(float) * adverse
        exposure_delta = -np.full(len(sub), min(1.0, d / 96.0), dtype=float)
        opportunity_delta = exposure_delta * opp_value
        return adverse_delta, opportunity_delta, exposure_delta

    if action.name == "reenable_at_half_life":
        t_landmark = _numeric_any(
            sub,
            _first_existing(sub, ["timing_landmark", "rv_decay_half_life", "parent_time_to_relax", "time_to_relax"]),
            n=len(sub),
            default=10**9,
        ).fillna(10**9).to_numpy()
        t_adverse = _numeric_any(
            sub,
            _first_existing(sub, ["time_to_adverse", "time_to_secondary_shock", "time_to_tail_move"]),
            n=len(sub),
            default=10**9,
        ).fillna(10**9).to_numpy()
        adverse_delta = -(t_adverse <= t_landmark).astype(float) * adverse
        exposure_delta = -np.clip(t_landmark / 96.0, 0.0, 1.0)
        opportunity_delta = exposure_delta * opp_value
        return adverse_delta, opportunity_delta, exposure_delta

    raise ValueError(f"Unsupported action: {action.name}")


def _expectancy_from_effect_vectors(adverse_delta_vec: np.ndarray, opp_delta_vec: np.ndarray) -> float:
    mean_adv = float(np.nanmean(adverse_delta_vec)) if len(adverse_delta_vec) else np.nan
    mean_opp = float(np.nanmean(opp_delta_vec)) if len(opp_delta_vec) else np.nan
    if not np.isfinite(mean_adv):
        return 0.0
    risk_reduction = float(-mean_adv)
    opportunity_cost = float(max(0.0, -mean_opp)) if np.isfinite(mean_opp) else 0.0
    net_benefit = float(risk_reduction - opportunity_cost)
    return float(net_benefit) if np.isfinite(net_benefit) else 0.0


def _expectancy_for_action(sub: pd.DataFrame, action: ActionSpec) -> float:
    if sub.empty:
        return 0.0
    adverse_delta_vec, opp_delta_vec, _ = _apply_action_proxy(sub, action)
    return _expectancy_from_effect_vectors(adverse_delta_vec, opp_delta_vec)


def _combine_with_delay_override(sub: pd.DataFrame, action: ActionSpec, delay_bars: int) -> float:
    if sub.empty:
        return 0.0
    base_adv, base_opp, _ = _apply_action_proxy(sub, action)
    if int(delay_bars) <= 0:
        return _expectancy_from_effect_vectors(base_adv, base_opp)
    delay_action = ActionSpec(
        name=f"delay_{int(delay_bars)}",
        family="timing",
        params={"delay_bars": int(delay_bars)},
    )
    delay_adv, delay_opp, _ = _apply_action_proxy(sub, delay_action)
    return _expectancy_from_effect_vectors(base_adv + delay_adv, base_opp + delay_opp)


def _condition_mask_for_numeric_expr(frame: pd.DataFrame, feature: str, operator: str, threshold: float) -> pd.Series:
    values = pd.to_numeric(frame.get(feature), errors="coerce")
    if operator == ">=":
        return values >= threshold
    if operator == "<=":
        return values <= threshold
    if operator == ">":
        return values > threshold
    if operator == "<":
        return values < threshold
    if operator == "==":
        return values == threshold
    return pd.Series(False, index=frame.index)


def _curvature_metrics(
    *,
    all_events: pd.DataFrame,
    condition_name: str,
    sub: pd.DataFrame,
    action: ActionSpec,
    parameter_curvature_max_penalty: float,
) -> Dict[str, object]:
    parsed = _parse_numeric_condition_expr(condition_name)
    center = _expectancy_for_action(sub, action)
    if parsed is None:
        return {
            "expectancy_left": center,
            "expectancy_center": center,
            "expectancy_right": center,
            "curvature_penalty": 0.0,
            "neighborhood_positive_count": 3,
            "gate_parameter_curvature": True,
        }

    feature, operator, threshold = parsed
    if feature not in all_events.columns:
        return {
            "expectancy_left": center,
            "expectancy_center": center,
            "expectancy_right": center,
            "curvature_penalty": 0.0,
            "neighborhood_positive_count": 3,
            "gate_parameter_curvature": True,
        }

    delta = max(abs(float(threshold)) * 0.10, 1e-6)
    left_mask = _condition_mask_for_numeric_expr(all_events, feature, operator, float(threshold - delta))
    right_mask = _condition_mask_for_numeric_expr(all_events, feature, operator, float(threshold + delta))
    left = _expectancy_for_action(all_events[left_mask.fillna(False)].copy(), action)
    right = _expectancy_for_action(all_events[right_mask.fillna(False)].copy(), action)

    denom = max(abs(center), 1e-9)
    curvature_penalty = float(max(0.0, center - min(left, right)) / denom)
    neighborhood_positive_count = int(sum(1 for x in [left, center, right] if x > 0.0))
    gate_parameter_curvature = bool(
        neighborhood_positive_count >= 2 and curvature_penalty <= float(parameter_curvature_max_penalty)
    )
    return {
        "expectancy_left": float(left),
        "expectancy_center": float(center),
        "expectancy_right": float(right),
        "curvature_penalty": curvature_penalty,
        "neighborhood_positive_count": neighborhood_positive_count,
        "gate_parameter_curvature": gate_parameter_curvature,
    }


def _delay_robustness_fields(
    delay_expectancies_adjusted: List[float],
    *,
    min_delay_positive_ratio: float,
    min_delay_robustness_score: float,
) -> Dict[str, object]:
    if not delay_expectancies_adjusted:
        return {
            "delay_positive_ratio": 0.0,
            "delay_dispersion": 0.0,
            "delay_robustness_score": 0.0,
            "gate_delay_robustness": False,
        }
    arr = np.asarray(delay_expectancies_adjusted, dtype=float)
    delay_positive_ratio = float(np.mean(arr > 0.0))
    delay_dispersion = float(np.std(arr))
    mean_delay_expectancy = float(np.mean(arr))
    stability_component = float(
        max(0.0, 1.0 - (delay_dispersion / max(abs(mean_delay_expectancy), 1e-9)))
    )
    delay_robustness_score = float((0.7 * delay_positive_ratio) + (0.3 * stability_component))
    gate_delay_robustness = bool(
        delay_positive_ratio >= float(min_delay_positive_ratio)
        and delay_robustness_score >= float(min_delay_robustness_score)
    )
    return {
        "delay_positive_ratio": delay_positive_ratio,
        "delay_dispersion": delay_dispersion,
        "delay_robustness_score": delay_robustness_score,
        "gate_delay_robustness": gate_delay_robustness,
    }


def _gate_year_stability(sub: pd.DataFrame, effect_col: str, min_ratio: float = 0.8) -> Tuple[bool, str]:
    if "year" not in sub.columns or sub.empty:
        return False, "insufficient_years"
    signs = []
    for _, g in sub.groupby("year", sort=True):
        x = g[effect_col].mean()
        signs.append(1 if x > 0 else -1 if x < 0 else 0)
    non_zero = [s for s in signs if s != 0]
    if not non_zero:
        return False, "all_zero"
    improvement_ratio = non_zero.count(-1) / len(non_zero)
    catastrophic_reversal = (non_zero.count(1) > 0) and (improvement_ratio < min_ratio)
    return bool(improvement_ratio >= min_ratio and not catastrophic_reversal), ",".join(str(s) for s in signs)


def _gate_regime_stability(
    sub: pd.DataFrame,
    effect_col: str,
    condition_name: str,
    min_stable_splits: int = 2,
) -> Tuple[bool, int, int]:
    # If condition itself is on split variable, skip that split.
    checks: List[pd.Series] = []
    if not condition_name.startswith("symbol_") and "symbol" in sub.columns:
        checks.append(sub.groupby("symbol")[effect_col].mean())
    if not condition_name.startswith("vol_regime_") and "vol_regime" in sub.columns:
        checks.append(sub.groupby("vol_regime")[effect_col].mean())
    if not condition_name.startswith("bull_bear_") and "bull_bear" in sub.columns:
        checks.append(sub.groupby("bull_bear")[effect_col].mean())

    # Majority-style regime gate by default (2/3 when all splits exist).
    stable_splits = 0
    for s in checks:
        nz = [v for v in s.tolist() if abs(v) > 1e-12]
        if not nz:
            continue
        # Improvement should stay non-positive (adverse reduction) across splits.
        if not any(v > 0 for v in nz):
            stable_splits += 1

    if not checks:
        return False, 0, 0

    required_splits = min(max(1, int(min_stable_splits)), len(checks))
    return stable_splits >= required_splits, stable_splits, required_splits


def _split_count(sub: pd.DataFrame, label: str) -> int:
    if "split_label" not in sub.columns:
        return 0
    return int((sub["split_label"] == label).sum())


def _split_mean(sub: pd.DataFrame, split_label: str, col: str) -> float:
    if "split_label" not in sub.columns or col not in sub.columns:
        return np.nan
    frame = sub[sub["split_label"] == split_label]
    if frame.empty:
        return np.nan
    values = pd.to_numeric(frame[col], errors="coerce").dropna()
    return float(values.mean()) if not values.empty else np.nan


def _split_t_stat_and_p_value(sub: pd.DataFrame, split_label: str, col: str) -> Tuple[float, float]:
    if "split_label" not in sub.columns or col not in sub.columns:
        return np.nan, np.nan
    frame = sub[sub["split_label"] == split_label]
    values = pd.to_numeric(frame[col], errors="coerce").dropna().to_numpy(dtype=float)
    if len(values) < 2:
        return np.nan, np.nan
    std = float(np.std(values, ddof=1))
    if std <= 0.0:
        return np.nan, np.nan
    t_stat = float(np.mean(values) / (std / np.sqrt(len(values))))
    return t_stat, float(_two_sided_p_from_t(t_stat))


def _capacity_proxy_from_frame(sub: pd.DataFrame) -> float:
    for col in ["quote_volume", "volume", "notional", "turnover", "liquidation_notional"]:
        if col not in sub.columns:
            continue
        vals = pd.to_numeric(sub[col], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        vals = vals[vals > 0]
        if vals.empty:
            continue
        return float(np.log1p(float(vals.median())))
    return np.nan


def _effective_sample_size(values: np.ndarray, max_lag: int) -> Tuple[float, int]:
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    n = int(len(arr))
    if n <= 1:
        return float(n), 0

    lag_cap = min(int(max_lag), n // 10)
    if lag_cap <= 0:
        return float(n), 0

    series = pd.Series(arr)
    rho_sum = 0.0
    for lag in range(1, lag_cap + 1):
        rho = series.autocorr(lag=lag)
        if np.isfinite(rho):
            rho_sum += float(rho)

    denom = max(1e-6, 1.0 + (2.0 * rho_sum))
    ess = float(n / denom)
    ess = float(np.clip(ess, 0.0, float(n)))
    return ess, lag_cap


def _multiplicity_penalty(*, multiplicity_k: float, num_tests_event_family: int, ess_effective: float) -> float:
    tests = max(2.0, float(num_tests_event_family))
    eff_n = max(1.0, float(ess_effective))
    return float(float(multiplicity_k) * np.sqrt(np.log(tests) / eff_n))


def _apply_multiplicity_adjustments(
    candidates: pd.DataFrame,
    *,
    multiplicity_k: float,
    min_delay_positive_ratio: float = 0.60,
    min_delay_robustness_score: float = 0.60,
) -> pd.DataFrame:
    if candidates.empty:
        return candidates

    out = candidates.copy()
    out["expectancy_per_trade"] = pd.to_numeric(out.get("expectancy_per_trade"), errors="coerce").fillna(0.0)
    out["ess_effective"] = pd.to_numeric(out.get("ess_effective"), errors="coerce").fillna(0.0)
    out["num_tests_event_family"] = pd.to_numeric(out.get("num_tests_event_family"), errors="coerce").fillna(0).astype(int)
    out["multiplicity_penalty"] = out.apply(
        lambda row: _multiplicity_penalty(
            multiplicity_k=float(multiplicity_k),
            num_tests_event_family=int(row.get("num_tests_event_family", 0)),
            ess_effective=float(row.get("ess_effective", 0.0)),
        ),
        axis=1,
    )
    out["expectancy_after_multiplicity"] = out["expectancy_per_trade"] - out["multiplicity_penalty"]
    out["gate_multiplicity_strict"] = out["gate_multiplicity"].astype(bool) & (out["expectancy_after_multiplicity"] > 0.0)

    delay_maps_adjusted: List[str] = []
    delay_positive_ratio: List[float] = []
    delay_dispersion: List[float] = []
    delay_scores: List[float] = []
    delay_gates: List[bool] = []
    for _, row in out.iterrows():
        raw_map_payload = row.get("delay_expectancy_map", "{}")
        try:
            raw_map = json.loads(str(raw_map_payload))
        except Exception:
            raw_map = {}
        if not isinstance(raw_map, dict):
            raw_map = {}
        adjusted_map = {
            str(key): float(_safe_float(val, 0.0) - _safe_float(row.get("multiplicity_penalty"), 0.0))
            for key, val in raw_map.items()
        }
        fields = _delay_robustness_fields(
            list(adjusted_map.values()),
            min_delay_positive_ratio=float(min_delay_positive_ratio),
            min_delay_robustness_score=float(min_delay_robustness_score),
        )
        delay_maps_adjusted.append(json.dumps(adjusted_map, sort_keys=True))
        delay_positive_ratio.append(float(fields["delay_positive_ratio"]))
        delay_dispersion.append(float(fields["delay_dispersion"]))
        delay_scores.append(float(fields["delay_robustness_score"]))
        delay_gates.append(bool(fields["gate_delay_robustness"]))

    out["delay_expectancy_map"] = delay_maps_adjusted
    out["delay_positive_ratio"] = delay_positive_ratio
    out["delay_dispersion"] = delay_dispersion
    out["delay_robustness_score"] = delay_scores
    out["gate_delay_robustness"] = delay_gates
    return out


def _evaluate_candidate(
    sub: pd.DataFrame,
    condition: ConditionSpec,
    action: ActionSpec,
    bootstrap_iters: int,
    seed: int,
    run_symbols: List[str],
    cost_floor: float,
    tail_material_threshold: float,
    opportunity_tight_eps: float,
    opportunity_near_zero_eps: float,
    net_benefit_floor: float,
    simplicity_gate: bool,
    min_regime_stable_splits: int = 2,
    min_oos_split_samples: int = 1,
    min_ess: float = 150.0,
    ess_max_lag: int = 24,
    parameter_curvature_max_penalty: float = 0.50,
    delay_grid_bars: List[int] | None = None,
    min_delay_positive_ratio: float = 0.60,
    min_delay_robustness_score: float = 0.60,
    total_event_count: int = 0,
    all_events: pd.DataFrame | None = None,
    execution_cost_config: Dict[str, float] | None = None,
    stressed_cost_multiplier: float = 1.5,
) -> Dict[str, object]:
    if all_events is None:
        all_events = sub
    adverse_delta_vec, opp_delta_vec, exposure_delta_vec = _apply_action_proxy(sub, action)

    mean_adv, ci_low_adv, ci_high_adv = _bootstrap_ci(adverse_delta_vec, bootstrap_iters, seed + 1)
    mean_opp, ci_low_opp, ci_high_opp = _bootstrap_ci(opp_delta_vec, bootstrap_iters, seed + 7)
    mean_exp, _, _ = _bootstrap_ci(exposure_delta_vec, bootstrap_iters, seed + 13)

    # primary improvement means adverse delta is negative
    gate_a = bool(np.isfinite(ci_high_adv) and ci_high_adv < 0)

    tmp = sub.copy()
    tmp["adverse_effect"] = adverse_delta_vec
    gate_b, year_signs = _gate_year_stability(tmp, "adverse_effect")
    gate_c, gate_c_stable_splits, gate_c_required_splits = _gate_regime_stability(
        tmp,
        "adverse_effect",
        condition_name=condition.name,
        min_stable_splits=min_regime_stable_splits,
    )

    risk_reduction = float(-mean_adv) if np.isfinite(mean_adv) else np.nan
    material_tail = bool(np.isfinite(risk_reduction) and risk_reduction >= tail_material_threshold)
    gate_d = bool(np.isfinite(risk_reduction) and risk_reduction >= cost_floor) or material_tail

    opportunity_forward = (
        float(np.nanmean(sub["forward_abs_return_h"].to_numpy(dtype=float)))
        if "forward_abs_return_h" in sub.columns and len(sub) > 0
        else np.nan
    )
    opportunity_tail = (
        float(np.nanmean(sub["opportunity_value_excess"].to_numpy(dtype=float)))
        if "opportunity_value_excess" in sub.columns and len(sub) > 0
        else np.nan
    )
    opportunity_composite = float(np.nanmean([opportunity_forward, opportunity_tail]))
    if not np.isfinite(opportunity_composite):
        opportunity_composite = 0.0

    opportunity_cost = float(max(0.0, -mean_opp)) if np.isfinite(mean_opp) else np.nan
    net_benefit = float(risk_reduction - opportunity_cost) if np.isfinite(risk_reduction) and np.isfinite(opportunity_cost) else np.nan
    expectancy_per_trade = float(net_benefit) if np.isfinite(net_benefit) else 0.0
    cost_fields = _candidate_cost_fields(
        sub=sub,
        action=action,
        expectancy_per_trade=expectancy_per_trade,
        execution_cost_config=dict(execution_cost_config or {}),
        stressed_cost_multiplier=float(stressed_cost_multiplier),
    )

    opportunity_ci_tight_overlap = bool(
        np.isfinite(ci_low_opp)
        and np.isfinite(ci_high_opp)
        and (ci_low_opp <= 0.0 <= ci_high_opp)
        and (max(abs(ci_low_opp), abs(ci_high_opp)) <= opportunity_tight_eps)
    )
    opportunity_near_zero = bool(
        (np.isfinite(opportunity_cost) and opportunity_cost <= opportunity_near_zero_eps)
        or opportunity_ci_tight_overlap
    )
    full_block_exposure = bool(np.isfinite(mean_exp) and mean_exp <= -0.9)
    gate_f = not (full_block_exposure and not opportunity_near_zero)
    gate_g = bool(np.isfinite(net_benefit) and net_benefit >= net_benefit_floor)
    gate_e = bool(simplicity_gate)

    gate_values = [
        gate_a,
        gate_b,
        gate_c,
        gate_d,
        gate_f,
        gate_g,
        bool(cost_fields["gate_after_cost_positive"]),
        bool(cost_fields["gate_after_cost_stressed_positive"]),
        bool(cost_fields["gate_cost_model_valid"]),
        bool(cost_fields["gate_cost_ratio"]),
    ]
    robustness_score = float(np.mean([1.0 if g else 0.0 for g in gate_values]))

    train_samples = _split_count(sub, "train")

    validation_samples = _split_count(sub, "validation")
    test_samples = _split_count(sub, "test")
    validation_delta_adverse_mean = _split_mean(tmp, "validation", "adverse_effect")
    test_delta_adverse_mean = _split_mean(tmp, "test", "adverse_effect")
    val_t_stat, val_p_value = _split_t_stat_and_p_value(tmp, "validation", "adverse_effect")
    oos1_t_stat, oos1_p_value = _split_t_stat_and_p_value(tmp, "test", "adverse_effect")
    gate_oos_min_samples = bool(validation_samples >= int(min_oos_split_samples))
    gate_oos_validation = bool(
        gate_oos_min_samples
        and np.isfinite(validation_delta_adverse_mean)
        and validation_delta_adverse_mean < 0.0
    )
    gate_oos_consistency_strict = bool(
        gate_oos_validation
        and validation_samples >= 120
        and test_samples >= 120
    )
    if gate_oos_validation:
        robustness_score = min(1.0, robustness_score + 0.1)

    sample_size = int(len(sub))
    event_frequency = float(sample_size / total_event_count) if total_event_count > 0 else 0.0
    capacity_proxy = _capacity_proxy_from_frame(sub)
    profit_density_score = float(expectancy_per_trade * robustness_score * event_frequency)
    ess_effective, ess_lag_used = _effective_sample_size(adverse_delta_vec, max_lag=int(ess_max_lag))
    gate_ess = bool(ess_effective >= float(min_ess))
    curvature_metrics = _curvature_metrics(
        all_events=all_events,
        condition_name=condition.name,
        sub=sub,
        action=action,
        parameter_curvature_max_penalty=float(parameter_curvature_max_penalty),
    )
    delay_grid = [0, 4, 8, 16, 30] if delay_grid_bars is None else list(delay_grid_bars)
    delay_expectancy_map_raw = {
        str(int(delay)): _combine_with_delay_override(sub, action, int(delay))
        for delay in delay_grid
    }

    fail_reasons = []
    if not gate_a:
        fail_reasons.append("gate_a_ci")
    if not gate_b:
        fail_reasons.append("gate_b_time")
    if not gate_c:
        fail_reasons.append("gate_c_regime")
    if not gate_d:
        fail_reasons.append("gate_d_friction")
    if not gate_f:
        fail_reasons.append("gate_f_exposure_guard")
    if not gate_g:
        fail_reasons.append("gate_g_net_benefit")
    if not gate_e:
        fail_reasons.append("gate_e_simplicity")
    if not gate_oos_min_samples:
        fail_reasons.append("gate_oos_min_samples")
    if not gate_oos_validation:
        fail_reasons.append("gate_oos_validation")
    if not gate_oos_consistency_strict:
        fail_reasons.append("gate_oos_consistency_strict")
    if not gate_ess:
        fail_reasons.append("gate_ess")

    # Gate H: executable contract (avoid promoting conditions/actions that cannot compile into DSL runtime)
    gate_h_condition = bool(is_executable_condition(condition.name, run_symbols=run_symbols))
    gate_h_action = bool(is_executable_action(action.name))
    if not gate_h_condition:
        fail_reasons.append("gate_h_executable_condition")
    if not gate_h_action:
        fail_reasons.append("gate_h_executable_action")
    if not bool(cost_fields["gate_after_cost_positive"]):
        fail_reasons.append("gate_after_cost_positive")
    if not bool(cost_fields["gate_after_cost_stressed_positive"]):
        fail_reasons.append("gate_after_cost_stressed_positive")
    if not bool(cost_fields["gate_cost_model_valid"]):
        fail_reasons.append("gate_cost_model_valid")
    if not bool(cost_fields["gate_cost_ratio"]):
        fail_reasons.append("gate_cost_ratio")
    if not bool(curvature_metrics.get("gate_parameter_curvature", True)):
        fail_reasons.append("gate_parameter_curvature")

    return {
        "candidate_id": f"{condition.name}__{action.name}",
        "condition": condition.name,
        "condition_desc": condition.description,
        "action": action.name,
        "action_family": action.family,
        "candidate_type": _candidate_type_from_action(action.name),
        "overlay_base_candidate_id": "",
        "sample_size": int(len(sub)),
        "train_samples": train_samples,
        "validation_samples": validation_samples,
        "test_samples": test_samples,
        "baseline_mode": str(sub["baseline_mode"].iloc[0]) if "baseline_mode" in sub.columns and not sub.empty else "event_proxy_only",
        "delta_adverse_mean": mean_adv,
        "delta_adverse_ci_low": ci_low_adv,
        "delta_adverse_ci_high": ci_high_adv,
        "delta_opportunity_mean": mean_opp,
        "delta_opportunity_ci_low": ci_low_opp,
        "delta_opportunity_ci_high": ci_high_opp,
        "delta_exposure_mean": mean_exp,
        "gate_a_ci_separated": gate_a,
        "gate_b_time_stable": gate_b,
        "gate_b_year_signs": year_signs,
        "gate_c_regime_stable": gate_c,
        "gate_c_stable_splits": gate_c_stable_splits,
        "gate_c_required_splits": gate_c_required_splits,
        "gate_d_friction_floor": gate_d,
        "opportunity_forward_mean": opportunity_forward,
        "opportunity_tail_mean": opportunity_tail,
        "opportunity_composite_mean": opportunity_composite,
        "opportunity_cost_mean": opportunity_cost,
        "net_benefit_mean": net_benefit,
        "expectancy_per_trade": expectancy_per_trade,
        "robustness_score": robustness_score,
        "event_frequency": event_frequency,
        "capacity_proxy": capacity_proxy,
        "profit_density_score": profit_density_score,
        "gate_f_exposure_guard": gate_f,
        "gate_g_net_benefit": gate_g,
        "gate_e_simplicity": gate_e,
        "gate_h_executable_condition": gate_h_condition,
        "gate_h_executable_action": gate_h_action,
        "validation_delta_adverse_mean": validation_delta_adverse_mean,
        "test_delta_adverse_mean": test_delta_adverse_mean,
        "val_delta_adverse_mean": validation_delta_adverse_mean,
        "oos1_delta_adverse_mean": test_delta_adverse_mean,
        "val_t_stat": val_t_stat,
        "val_p_value": val_p_value,
        "val_p_value_adj_bh": np.nan,
        "oos1_t_stat": oos1_t_stat,
        "oos1_p_value": oos1_p_value,
        "test_t_stat": oos1_t_stat,
        "test_p_value": oos1_p_value,
        "test_p_value_adj_bh": np.nan,
        "num_tests_event_family": 0,
        "ess_effective": ess_effective,
        "ess_lag_used": int(ess_lag_used),
        "multiplicity_penalty": 0.0,
        "expectancy_after_multiplicity": expectancy_per_trade,
        "expectancy_left": float(curvature_metrics["expectancy_left"]),
        "expectancy_center": float(curvature_metrics["expectancy_center"]),
        "expectancy_right": float(curvature_metrics["expectancy_right"]),
        "curvature_penalty": float(curvature_metrics["curvature_penalty"]),
        "neighborhood_positive_count": int(curvature_metrics["neighborhood_positive_count"]),
        "gate_parameter_curvature": bool(curvature_metrics["gate_parameter_curvature"]),
        "delay_expectancy_map": json.dumps(delay_expectancy_map_raw, sort_keys=True),
        "delay_positive_ratio": 0.0,
        "delay_dispersion": 0.0,
        "delay_robustness_score": 0.0,
        "gate_delay_robustness": False,
        "gate_oos_min_samples": gate_oos_min_samples,
        "gate_oos_validation": gate_oos_validation,
        "gate_oos_validation_test": gate_oos_validation,
        "gate_oos_consistency_strict": gate_oos_consistency_strict,
        "gate_multiplicity": False,
        "gate_multiplicity_strict": False,
        "gate_ess": gate_ess,
        "after_cost_expectancy_per_trade": float(cost_fields["after_cost_expectancy_per_trade"]),
        "stressed_after_cost_expectancy_per_trade": float(cost_fields["stressed_after_cost_expectancy_per_trade"]),
        "turnover_proxy_mean": float(cost_fields["turnover_proxy_mean"]),
        "avg_dynamic_cost_bps": float(cost_fields["avg_dynamic_cost_bps"]),
        "cost_input_coverage": float(cost_fields["cost_input_coverage"]),
        "cost_model_valid": bool(cost_fields["cost_model_valid"]),
        "cost_ratio": float(cost_fields["cost_ratio"]),
        "gate_after_cost_positive": bool(cost_fields["gate_after_cost_positive"]),
        "gate_after_cost_stressed_positive": bool(cost_fields["gate_after_cost_stressed_positive"]),
        "gate_cost_model_valid": bool(cost_fields["gate_cost_model_valid"]),
        "gate_cost_ratio": bool(cost_fields["gate_cost_ratio"]),
        "bridge_eval_status": "pending",
        "bridge_train_after_cost_bps": np.nan,
        "bridge_validation_after_cost_bps": np.nan,
        "bridge_validation_stressed_after_cost_bps": np.nan,
        "bridge_validation_trades": 0,
        "bridge_effective_cost_bps_per_trade": np.nan,
        "bridge_gross_edge_bps_per_trade": np.nan,
        "gate_bridge_has_trades_validation": False,
        "gate_bridge_after_cost_positive_validation": False,
        "gate_bridge_after_cost_stressed_positive_validation": False,
        "gate_bridge_edge_cost_ratio": False,
        "gate_bridge_turnover_controls": False,
        "gate_bridge_tradable": False,
        "selection_score_executed": np.nan,
        "gate_pass": bool(
            gate_a
            and gate_b
            and gate_c
            and gate_d
            and gate_f
            and gate_g
            and gate_e
            and gate_oos_validation
            and gate_ess
            and bool(cost_fields["gate_after_cost_positive"])
            and bool(cost_fields["gate_after_cost_stressed_positive"])
            and bool(cost_fields["gate_cost_model_valid"])
            and bool(cost_fields["gate_cost_ratio"])
            and bool(curvature_metrics.get("gate_parameter_curvature", True))
        ),
        "gate_all_research": False,
        "gate_all": False,
        "fail_reasons": ",".join(fail_reasons) if fail_reasons else "",
    }


def _compute_drawdown_profile(pnl: np.ndarray) -> float:
    arr = np.asarray(pnl, dtype=float)
    arr = arr[np.isfinite(arr)]
    if len(arr) == 0:
        return 0.0
    cum = np.cumsum(arr)
    running_peak = np.maximum.accumulate(cum)
    drawdown = cum - running_peak
    return float(np.min(drawdown)) if len(drawdown) else 0.0


def _build_symbol_evaluation_table(
    events: pd.DataFrame,
    candidates: pd.DataFrame,
    conditions: List[ConditionSpec],
    actions: List[ActionSpec],
    event_type: str,
    min_ev: float,
    min_stability_score: float,
    min_sample_size: int,
) -> pd.DataFrame:
    if events.empty or candidates.empty or "symbol" not in events.columns:
        return pd.DataFrame(columns=SYMBOL_OUTPUT_COLUMNS)

    condition_map = {c.name: c for c in conditions}
    action_map = {a.name: a for a in actions}
    rows: List[Dict[str, object]] = []

    for _, cand in candidates.iterrows():
        condition_name = str(cand.get("condition", ""))
        action_name = str(cand.get("action", ""))
        cond = condition_map.get(condition_name)
        action = action_map.get(action_name)
        if cond is None or action is None:
            continue

        sub = events[cond.mask_fn(events)].copy()
        if sub.empty or "symbol" not in sub.columns:
            continue

        adverse_delta_vec, opp_delta_vec, _ = _apply_action_proxy(sub, action)
        sub["net_benefit_effect"] = (-adverse_delta_vec) - np.maximum(0.0, -opp_delta_vec)

        for symbol, sym_frame in sub.groupby("symbol", sort=True):
            sym_values = pd.to_numeric(sym_frame["net_benefit_effect"], errors="coerce").dropna().to_numpy(dtype=float)
            if len(sym_values) == 0:
                continue
            ev = float(np.mean(sym_values))
            variance = float(np.var(sym_values, ddof=1)) if len(sym_values) > 1 else 0.0
            std = float(np.sqrt(max(variance, 0.0)))
            sharpe_like = float(ev / std) if std > 1e-12 else 0.0

            splits = []
            if "split_label" in sym_frame.columns:
                for _, g in sym_frame.groupby("split_label", sort=True):
                    vals = pd.to_numeric(g["net_benefit_effect"], errors="coerce").dropna().to_numpy(dtype=float)
                    if len(vals):
                        splits.append(float(np.mean(vals)))
            if not splits:
                splits = [ev]

            overall_sign = 1 if ev > 0 else -1 if ev < 0 else 0
            non_zero_split_signs = [1 if x > 0 else -1 if x < 0 else 0 for x in splits if abs(x) > 1e-12]
            window_consistency = (
                float(sum(1 for s in non_zero_split_signs if s == overall_sign) / len(non_zero_split_signs))
                if non_zero_split_signs
                else 0.0
            )
# Delegated module split for testable components.
from pipelines.research import phase2_event_analyzer as _phase2_event_analyzer
from pipelines.research import phase2_statistical_gates as _phase2_statistical_gates
from pipelines.research import phase2_cost_integration as _phase2_cost_integration

ConditionSpec = _phase2_event_analyzer.ConditionSpec
ActionSpec = _phase2_event_analyzer.ActionSpec
_build_conditions = _phase2_event_analyzer.build_conditions
_build_actions = _phase2_event_analyzer.build_actions
_candidate_type_from_action = _phase2_event_analyzer.candidate_type_from_action
_assign_candidate_types_and_overlay_bases = _phase2_event_analyzer.assign_candidate_types_and_overlay_bases
_attach_forward_opportunity = _phase2_event_analyzer.attach_forward_opportunity
_attach_event_market_features = _phase2_event_analyzer.attach_event_market_features
_prepare_baseline = _phase2_event_analyzer.prepare_baseline
_apply_action_proxy = _phase2_event_analyzer.apply_action_proxy
_expectancy_from_effect_vectors = _phase2_event_analyzer.expectancy_from_effect_vectors
_expectancy_for_action = _phase2_event_analyzer.expectancy_for_action
_combine_with_delay_override = _phase2_event_analyzer.combine_with_delay_override
_compute_drawdown_profile = _phase2_event_analyzer.compute_drawdown_profile

_turnover_proxy_for_action = _phase2_cost_integration.turnover_proxy_for_action
_candidate_cost_fields = _phase2_cost_integration.candidate_cost_fields

_condition_mask_for_numeric_expr = _phase2_statistical_gates.condition_mask_for_numeric_expr
_curvature_metrics = _phase2_statistical_gates.curvature_metrics
_delay_robustness_fields = _phase2_statistical_gates.delay_robustness_fields
_gate_year_stability = _phase2_statistical_gates.gate_year_stability
_gate_regime_stability = _phase2_statistical_gates.gate_regime_stability
_split_count = _phase2_statistical_gates.split_count
_split_mean = _phase2_statistical_gates.split_mean
_split_t_stat_and_p_value = _phase2_statistical_gates.split_t_stat_and_p_value
_capacity_proxy_from_frame = _phase2_statistical_gates.capacity_proxy_from_frame
_effective_sample_size = _phase2_statistical_gates.effective_sample_size
_multiplicity_penalty = _phase2_statistical_gates.multiplicity_penalty
_apply_multiplicity_adjustments = _phase2_statistical_gates.apply_multiplicity_adjustments
