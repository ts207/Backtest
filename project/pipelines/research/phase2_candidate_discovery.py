from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import hashlib
from collections import Counter
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional

import numpy as np
import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = PROJECT_ROOT.parent
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", REPO_ROOT / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import (
    ensure_dir,
    read_parquet,
    list_parquet_files,
    run_scoped_lake_path,
    choose_partition_dir,
)
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.validation import ts_ns_utc
from pipelines._lib.sanity import assert_monotonic_utc_timestamp
from research.candidate_schema import ensure_candidate_schema
from pipelines._lib.ontology_contract import (
    bool_field,
    compare_hash_fields,
    load_run_manifest_hashes,
    ontology_component_hash_fields,
    ontology_component_hashes,
    ontology_spec_paths,
    ontology_spec_hash,
    parse_list_field,
    state_id_to_context_column,
)
from pipelines._lib.bh_fdr_grouping import canonical_bh_group_key
from pipelines._lib.execution_costs import resolve_execution_costs
from pipelines._lib.spec_loader import load_global_defaults
from pipelines._lib.timeframe_constants import HORIZON_BARS_BY_TIMEFRAME
from events.registry import EVENT_REGISTRY_SPECS
from events.registry import load_registry_events
from pipelines.research.cost_calibration import ToBRegimeCostCalibrator
from pipelines._lib.spec_utils import get_spec_hashes
from pipelines.research.condition_key_contract import (
    format_available_key_sample,
    load_symbol_joined_condition_keys,
    missing_condition_keys,
)
from pipelines.research.analyze_conditional_expectancy import (
    _bh_adjust,
    _distribution_stats,
    _two_sided_p_from_t,
)

PRIMARY_OUTPUT_COLUMNS = [
    "runtime_event_type", "canonical_event_type", "canonical_family", "state_id", "state_provenance",
    "state_activation", "state_activation_hash",
    "hypothesis_id", "hypothesis_version", "hypothesis_spec_path", "hypothesis_spec_hash",
    "hypothesis_metric", "hypothesis_output_schema",
    "hypothesis_candidate_id",
    "candidate_hash_inputs",
    "template_id", "template_verb", "operator_id", "operator_version",
    "candidate_id", "condition", "condition_desc", "condition_source", "action", "action_family", "candidate_type",
    "condition_signature", "horizon_bars", "entry_lag_bars", "direction_rule",
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
    "effect_raw", "effect_shrunk_family", "effect_shrunk_event", "effect_shrunk_state",
    "shrinkage_weight_family", "shrinkage_weight_event", "shrinkage_weight_state",
    "p_value_raw", "p_value_shrunk", "p_value_for_fdr", "std_return", "gate_state_information",
    "effective_sample_size", "time_weight_sum", "mean_weight_age_days", "time_decay_tau_days", "time_decay_learning_rate",
    "time_decay_tau_up_days", "time_decay_tau_down_days", "time_decay_directional_ratio", "time_decay_directional_up_share",
    "lambda_family", "lambda_event", "lambda_state",
    "lambda_family_status", "lambda_event_status", "lambda_state_status",
    "discovery_start", "discovery_end",
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

_TAU_BY_FAMILY_DAYS: Dict[str, float] = {
    "LIQUIDITY_DISLOCATION": 15.0,
    "VOLATILITY_TRANSITION": 30.0,
    "POSITIONING_EXTREMES": 60.0,
    "FORCED_FLOW_AND_EXHAUSTION": 90.0,
    "FLOW_EXHAUSTION": 90.0,
    "TREND_STRUCTURE": 90.0,
    "STATISTICAL_DISLOCATION": 30.0,
    "REGIME_TRANSITION": 180.0,
    "INFORMATION_DESYNC": 60.0,
    "CROSS_SIGNAL": 60.0,
    "TEMPORAL_STRUCTURE": 30.0,
    "EXECUTION_FRICTION": 20.0,
}

_VOL_REGIME_MULTIPLIER: Dict[str, float] = {
    "LOW": 1.4,
    "MID": 1.0,
    "HIGH": 0.7,
    "SHOCK": 0.4,
}

_LIQUIDITY_STATE_MULTIPLIER: Dict[str, float] = {
    "LOW": 0.6,
    "NORMAL": 1.0,
    "RECOVERY": 1.2,
}

_DIRECTIONAL_ASYMMETRY_BY_FAMILY: Dict[str, Tuple[float, float]] = {
    "LIQUIDITY_DISLOCATION": (1.1, 0.5),
    "VOLATILITY_TRANSITION": (1.0, 0.9),
    "POSITIONING_EXTREMES": (1.4, 0.7),
    "FORCED_FLOW_AND_EXHAUSTION": (1.2, 0.8),
    "FLOW_EXHAUSTION": (1.2, 0.8),
    "TREND_STRUCTURE": (1.6, 0.8),
    "REGIME_TRANSITION": (1.2, 0.8),
}

_EVENT_DIRECTION_NUMERIC_COLS: Tuple[str, ...] = (
    "evt_event_direction",
    "evt_direction",
    "evt_signal_direction",
    "evt_flow_direction",
    "evt_breakout_direction",
    "evt_shock_direction",
    "evt_move_direction",
    "evt_leader_direction",
    "evt_return_1",
    "evt_return_sign",
    "evt_sign",
    "evt_polarity",
    "evt_funding_z",
    "evt_basis_z",
)

_EVENT_DIRECTION_TEXT_COLS: Tuple[str, ...] = (
    "evt_side",
    "evt_trade_side",
    "evt_signal_side",
    "evt_direction_label",
)


def _resolve_tau_days(canonical_family: str, override_days: Optional[float]) -> float:
    if override_days is not None and float(override_days) > 0.0:
        return float(override_days)
    key = str(canonical_family or "").strip().upper()
    return float(_TAU_BY_FAMILY_DAYS.get(key, 60.0))


def _time_decay_weights(
    event_ts: pd.Series,
    *,
    ref_ts: pd.Timestamp,
    tau_seconds: float,
    floor_weight: float,
) -> pd.Series:
    if event_ts.empty:
        return pd.Series(dtype=float)
    if float(tau_seconds) <= 0.0:
        return pd.Series(1.0, index=event_ts.index, dtype=float)
    delta = (ref_ts - pd.to_datetime(event_ts, utc=True, errors="coerce")).dt.total_seconds().fillna(0.0).clip(lower=0.0)
    w = np.exp(-delta / float(tau_seconds))
    floor = max(0.0, min(1.0, float(floor_weight)))
    return pd.Series(np.maximum(w, floor), index=event_ts.index, dtype=float)


def _effective_sample_size(weights: pd.Series) -> float:
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).clip(lower=0.0)
    s1 = float(w.sum())
    s2 = float((w * w).sum())
    if s1 <= 0.0 or s2 <= 0.0:
        return 0.0
    return float((s1 * s1) / s2)


def _normalize_vol_regime(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "MID"
    if "shock" in text:
        return "SHOCK"
    if "high" in text:
        return "HIGH"
    if "low" in text:
        return "LOW"
    if "mid" in text or "normal" in text:
        return "MID"
    return "MID"


def _normalize_liquidity_state(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "NORMAL"
    if "recovery" in text:
        return "RECOVERY"
    if any(token in text for token in ("low", "absence", "illiquid", "collapse", "vacuum")):
        return "LOW"
    return "NORMAL"


def _regime_conditioned_tau_days(
    *,
    canonical_family: str,
    vol_regime: Any,
    liquidity_state: Any,
    base_tau_days_override: Optional[float],
) -> float:
    tau = _resolve_tau_days(canonical_family, base_tau_days_override)
    vol_key = _normalize_vol_regime(vol_regime)
    liq_key = _normalize_liquidity_state(liquidity_state)
    tau *= float(_VOL_REGIME_MULTIPLIER.get(vol_key, 1.0))
    tau *= float(_LIQUIDITY_STATE_MULTIPLIER.get(liq_key, 1.0))
    return float(tau)


def _direction_sign(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        num = float(value)
        if np.isfinite(num) and num != 0.0:
            return 1 if num > 0.0 else -1
    except (TypeError, ValueError):
        pass

    token = str(value).strip().lower()
    if not token:
        return None
    if token in {"1", "+1", "up", "long", "buy", "bull", "positive", "pos"}:
        return 1
    if token in {"-1", "down", "short", "sell", "bear", "negative", "neg"}:
        return -1
    return None


def _optional_token(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    token = str(value).strip()
    if not token:
        return None
    if token.lower() in {"none", "null", "nan", "na"}:
        return None
    return token


def _event_direction_from_joined_row(
    row: pd.Series,
    *,
    canonical_family: str,
    fallback_direction: int,
) -> int:
    family_key = str(canonical_family or "").strip().upper()
    for col in _EVENT_DIRECTION_NUMERIC_COLS:
        if col not in row.index:
            continue
        sign = _direction_sign(row.get(col))
        if sign is None:
            continue
        if family_key == "POSITIONING_EXTREMES" and col == "evt_funding_z":
            return -sign
        return sign
    for col in _EVENT_DIRECTION_TEXT_COLS:
        if col not in row.index:
            continue
        sign = _direction_sign(row.get(col))
        if sign is not None:
            return sign
    return 1 if int(fallback_direction) >= 0 else -1


def _asymmetric_tau_days(
    *,
    base_tau_days: float,
    canonical_family: str,
    direction: int,
    default_up_mult: float,
    default_down_mult: float,
    min_ratio: float,
    max_ratio: float,
) -> Tuple[float, float, float, float]:
    family_key = str(canonical_family or "").strip().upper()
    up_mult, down_mult = _DIRECTIONAL_ASYMMETRY_BY_FAMILY.get(
        family_key,
        (float(default_up_mult), float(default_down_mult)),
    )
    up_mult = float(up_mult if up_mult > 0.0 else max(default_up_mult, 1e-6))
    down_mult = float(down_mult if down_mult > 0.0 else max(default_down_mult, 1e-6))
    ratio = up_mult / max(down_mult, 1e-9)
    min_r = max(1.0, float(min_ratio))
    max_r = max(min_r, float(max_ratio))
    if ratio < min_r:
        down_mult = up_mult / min_r
    elif ratio > max_r:
        down_mult = up_mult / max_r
    ratio = up_mult / max(down_mult, 1e-9)

    tau_up = float(base_tau_days) * up_mult
    tau_down = float(base_tau_days) * down_mult
    tau_eff = tau_up if int(direction) >= 0 else tau_down
    return float(tau_eff), float(tau_up), float(tau_down), float(ratio)


def _make_family_id(
    symbol: str,
    event_type: str,
    rule: str,
    horizon: str,
    cond_label: str,
    *,
    canonical_family: Optional[str] = None,
    state_id: Optional[str] = None,
) -> str:
    """BH family key based on ontology axes, stratified by symbol.

    Conditioning buckets are intentionally excluded to avoid multiplicity leakage.
    """
    base = canonical_bh_group_key(
        canonical_family=str(canonical_family or event_type),
        canonical_event_type=str(event_type),
        template_verb=str(rule),
        horizon=str(horizon),
        state_id=(str(state_id).strip() if state_id else None),
        symbol=None,
        include_symbol=False,
        direction_bucket=None,
    )
    return f"{str(symbol).strip().upper()}_{base}"


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


def _resolve_state_context_column(columns: pd.Index, state_id: Optional[str]) -> Optional[str]:
    state = str(state_id or "").strip()
    if not state:
        return None
    by_id = state_id_to_context_column(state)
    candidates = [
        by_id,
        state,
        state.upper(),
        state.lower(),
    ]
    for candidate in candidates:
        if candidate and candidate in columns:
            return str(candidate)
    return None


def _bool_mask_from_series(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series.fillna(False)
    values = pd.to_numeric(series, errors="coerce")
    if values.notna().any():
        return values.fillna(0.0) > 0.0
    text = series.astype(str).str.strip().str.lower()
    return text.isin({"1", "true", "t", "yes", "y", "on", "active"})


def _apply_multiplicity_controls(
    raw_df: pd.DataFrame,
    max_q: float,
    *,
    mode: str = "production",
    min_sample_size: int = 0,
) -> pd.DataFrame:
    """Apply BH correction per-family, then a global BH over family-adjusted q-values.

    In research mode, rows with sample_size below min_sample_size are retained for
    diagnostics but excluded from the multiplicity pool.
    """
    if raw_df.empty:
        out = raw_df.copy()
        out["q_value_family"] = pd.Series(dtype=float)
        out["is_discovery_family"] = pd.Series(dtype=bool)
        out["q_value"] = pd.Series(dtype=float)
        out["is_discovery"] = pd.Series(dtype=bool)
        return out

    out = raw_df.copy()
    out["q_value_family"] = 1.0
    out["is_discovery_family"] = False
    out["q_value"] = 1.0
    out["is_discovery"] = False

    eligible_mask = pd.Series(True, index=out.index)
    if str(mode) == "research" and int(min_sample_size) > 0 and "sample_size" in out.columns:
        sample = pd.to_numeric(out["sample_size"], errors="coerce").fillna(0.0)
        eligible_mask = sample >= float(int(min_sample_size))

    eligible = out[eligible_mask].copy()
    if eligible.empty:
        return out

    p_col = "p_value_for_fdr" if "p_value_for_fdr" in eligible.columns else "p_value"
    family_frames: List[pd.DataFrame] = []
    for _, family_df in eligible.groupby("family_id"):
        fam = family_df.copy()
        fam["q_value_family"] = _bh_adjust(fam[p_col])
        fam["is_discovery_family"] = fam["q_value_family"] <= float(max_q)
        family_frames.append(fam)

    eligible_scored = pd.concat(family_frames, axis=0)
    eligible_scored["q_value"] = _bh_adjust(eligible_scored["q_value_family"])
    eligible_scored["is_discovery"] = eligible_scored["q_value"] <= float(max_q)

    for col in ("q_value_family", "is_discovery_family", "q_value", "is_discovery"):
        out.loc[eligible_scored.index, col] = eligible_scored[col]
    return out


def _aggregate_effect_units(
    df: pd.DataFrame,
    *,
    unit_cols: List[str],
    n_col: str,
    mean_col: str,
    var_col: str,
    prefix: str,
) -> pd.DataFrame:
    cols = unit_cols + [n_col, mean_col, var_col]
    if df.empty:
        return pd.DataFrame(columns=unit_cols + [f"n_{prefix}", f"mean_{prefix}", f"var_{prefix}"])

    work = df[cols].copy()
    work["_n"] = pd.to_numeric(work[n_col], errors="coerce").fillna(0.0).clip(lower=0.0)
    work["_mean"] = pd.to_numeric(work[mean_col], errors="coerce").fillna(0.0)
    work["_var"] = pd.to_numeric(work[var_col], errors="coerce").fillna(0.0).clip(lower=0.0)

    rows: List[Dict[str, Any]] = []
    for keys, g in work.groupby(unit_cols, dropna=False):
        g = g.copy()
        total_n = float(g["_n"].sum())
        if total_n <= 0.0:
            mean_u = 0.0
            var_u = 0.0
        else:
            mean_u = float((g["_n"] * g["_mean"]).sum() / total_n)
            within = float(((g["_n"] - 1.0).clip(lower=0.0) * g["_var"]).sum())
            between = float((g["_n"] * (g["_mean"] - mean_u) ** 2).sum())
            denom = max(total_n - 1.0, 1.0)
            var_u = float(max(0.0, (within + between) / denom))

        if not isinstance(keys, tuple):
            keys = (keys,)
        row = {col: val for col, val in zip(unit_cols, keys)}
        row[f"n_{prefix}"] = total_n
        row[f"mean_{prefix}"] = mean_u
        row[f"var_{prefix}"] = var_u
        rows.append(row)
    return pd.DataFrame(rows)


def _estimate_adaptive_lambda(
    units_df: pd.DataFrame,
    *,
    parent_cols: List[str],
    child_col: str,
    n_col: str,
    mean_col: str,
    var_col: str,
    lambda_name: str,
    fixed_lambda: float,
    adaptive: bool,
    lambda_min: float,
    lambda_max: float,
    eps: float,
    min_total_samples: int,
    previous_lambda_by_parent: Optional[Dict[Tuple[Any, ...], float]] = None,
    lambda_smoothing_alpha: float = 0.1,
    lambda_shock_cap_pct: float = 0.5,
) -> pd.DataFrame:
    if units_df.empty:
        return pd.DataFrame(
            columns=parent_cols
            + [
                lambda_name,
                f"{lambda_name}_status",
                f"{lambda_name}_sigma_within",
                f"{lambda_name}_sigma_between",
                f"{lambda_name}_total_n",
                f"{lambda_name}_child_count",
                f"{lambda_name}_raw",
                f"{lambda_name}_prev",
            ]
        )

    if not adaptive:
        base = units_df[parent_cols].drop_duplicates().copy()
        base[lambda_name] = float(fixed_lambda)
        base[f"{lambda_name}_status"] = "fixed"
        base[f"{lambda_name}_sigma_within"] = np.nan
        base[f"{lambda_name}_sigma_between"] = np.nan
        base[f"{lambda_name}_total_n"] = np.nan
        base[f"{lambda_name}_child_count"] = np.nan
        base[f"{lambda_name}_raw"] = np.nan
        base[f"{lambda_name}_prev"] = np.nan
        return base

    rows: List[Dict[str, Any]] = []
    for keys, g in units_df.groupby(parent_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        parent_payload = {col: val for col, val in zip(parent_cols, keys)}

        n = pd.to_numeric(g[n_col], errors="coerce").fillna(0.0).clip(lower=0.0)
        mean = pd.to_numeric(g[mean_col], errors="coerce").fillna(0.0)
        var = pd.to_numeric(g[var_col], errors="coerce").fillna(0.0).clip(lower=0.0)
        child_count = int((n > 0.0).sum())
        total_n = float(n.sum())

        status = "adaptive"
        sigma_within = np.nan
        sigma_between = np.nan

        if total_n < float(max(0, int(min_total_samples))):
            # Explicit skip: no pooling when support is too low.
            lam = 0.0
            status = "insufficient_data"
        elif child_count <= 1:
            lam = float(lambda_max)
            status = "single_child"
            sigma_within = float(((n - 1.0).clip(lower=0.0) * var).sum() / max(float((n - 1.0).clip(lower=0.0).sum()), 1.0))
            sigma_between = 0.0
        else:
            mean_global = float((n * mean).sum() / max(total_n, 1.0))
            denom_within = float((n - 1.0).clip(lower=0.0).sum())
            sigma_within = float(((n - 1.0).clip(lower=0.0) * var).sum() / max(denom_within, 1.0))
            sigma_between = float((n * (mean - mean_global) ** 2).sum() / max(total_n, 1.0))
            lam_raw = float(sigma_within / max(sigma_between, float(eps)))
            lam = float(np.clip(lam_raw, float(lambda_min), float(lambda_max)))

        lam_raw = float(lam)
        lam_prev = np.nan
        if status == "adaptive" and previous_lambda_by_parent:
            key_tuple = tuple(parent_payload[c] for c in parent_cols)
            prev_val = previous_lambda_by_parent.get(key_tuple)
            if prev_val is not None and float(prev_val) > 0.0:
                lam_prev = float(prev_val)
                alpha = max(0.0, min(1.0, float(lambda_smoothing_alpha)))
                smoothed = ((1.0 - alpha) * lam_prev) + (alpha * lam_raw)
                cap = max(0.0, float(lambda_shock_cap_pct))
                lo = lam_prev * (1.0 - cap)
                hi = lam_prev * (1.0 + cap)
                lam = float(np.clip(smoothed, lo, hi))
                status = "adaptive_smoothed"

        row = dict(parent_payload)
        row[lambda_name] = float(lam)
        row[f"{lambda_name}_raw"] = float(lam_raw)
        row[f"{lambda_name}_prev"] = float(lam_prev) if np.isfinite(lam_prev) else np.nan
        row[f"{lambda_name}_status"] = status
        row[f"{lambda_name}_sigma_within"] = float(sigma_within) if np.isfinite(sigma_within) else np.nan
        row[f"{lambda_name}_sigma_between"] = float(sigma_between) if np.isfinite(sigma_between) else np.nan
        row[f"{lambda_name}_total_n"] = total_n
        row[f"{lambda_name}_child_count"] = child_count
        rows.append(row)
    return pd.DataFrame(rows)


def _apply_hierarchical_shrinkage(
    raw_df: pd.DataFrame,
    *,
    lambda_state: float = 100.0,
    lambda_event: float = 300.0,
    lambda_family: float = 1000.0,
    adaptive_lambda: bool = True,
    adaptive_lambda_min: float = 5.0,
    adaptive_lambda_max: float = 5000.0,
    adaptive_lambda_eps: float = 1e-8,
    adaptive_lambda_min_total_samples: int = 200,
    previous_lambda_maps: Optional[Dict[str, Dict[Tuple[Any, ...], float]]] = None,
    lambda_smoothing_alpha: float = 0.1,
    lambda_shock_cap_pct: float = 0.5,
) -> pd.DataFrame:
    """Empirical-Bayes partial pooling across family -> event -> state."""
    if raw_df.empty:
        out = raw_df.copy()
        for col in (
            "effect_raw",
            "effect_shrunk_family",
            "effect_shrunk_event",
            "effect_shrunk_state",
            "shrinkage_weight_family",
            "shrinkage_weight_event",
            "shrinkage_weight_state",
            "p_value_raw",
            "p_value_shrunk",
            "p_value_for_fdr",
        ):
            out[col] = pd.Series(dtype=float)
        return out

    out = raw_df.copy()
    out["effect_raw"] = pd.to_numeric(out.get("expectancy", 0.0), errors="coerce").fillna(0.0)
    out["p_value_raw"] = pd.to_numeric(out.get("p_value", 1.0), errors="coerce").fillna(1.0).clip(0.0, 1.0)
    out["_n"] = pd.to_numeric(
        out.get("effective_sample_size", out.get("n_events", out.get("sample_size", 0))),
        errors="coerce",
    ).fillna(0.0).clip(lower=0.0)
    if "std_return" not in out.columns:
        out["std_return"] = np.nan
    out["std_return"] = pd.to_numeric(out["std_return"], errors="coerce")

    family_col = out["canonical_family"] if "canonical_family" in out.columns else out.get("event_type", pd.Series("", index=out.index))
    event_col = out["canonical_event_type"] if "canonical_event_type" in out.columns else out.get("event_type", pd.Series("", index=out.index))
    verb_col = out["template_verb"] if "template_verb" in out.columns else out.get("rule_template", pd.Series("", index=out.index))
    horizon_col = out["horizon"] if "horizon" in out.columns else pd.Series("", index=out.index)
    symbol_col = out["symbol"] if "symbol" in out.columns else pd.Series("", index=out.index)
    state_col = out["state_id"] if "state_id" in out.columns else pd.Series("", index=out.index)

    out["_family"] = family_col.astype(str).str.strip().str.upper()
    out["_event"] = event_col.astype(str).str.strip().str.upper()
    out["_verb"] = verb_col.astype(str).str.strip()
    out["_horizon"] = horizon_col.astype(str).str.strip()
    out["_symbol"] = symbol_col.astype(str).str.strip().str.upper()
    out["_state"] = state_col.fillna("").astype(str).str.strip().str.upper()

    out["_var"] = (out["std_return"] ** 2).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    # Preserve condition-level candidates while shrinking along ontology levels.
    # Adaptive lambdas are estimated per (family,event,verb,horizon), not per symbol.
    global_cols = ["_verb", "_horizon"]
    family_cols = global_cols + ["_family"]
    event_cols = family_cols + ["_event"]
    state_cols = event_cols + ["_state"]

    global_stats = _aggregate_effect_units(
        out,
        unit_cols=global_cols,
        n_col="_n",
        mean_col="effect_raw",
        var_col="_var",
        prefix="global",
    )
    out = out.merge(global_stats[global_cols + ["mean_global", "n_global", "var_global"]], on=global_cols, how="left")

    family_stats = _aggregate_effect_units(
        out,
        unit_cols=family_cols,
        n_col="_n",
        mean_col="effect_raw",
        var_col="_var",
        prefix="family",
    )
    out = out.merge(family_stats[family_cols + ["mean_family", "n_family", "var_family"]], on=family_cols, how="left")

    lambda_family_df = _estimate_adaptive_lambda(
        family_stats,
        parent_cols=global_cols,
        child_col="_family",
        n_col="n_family",
        mean_col="mean_family",
        var_col="var_family",
        lambda_name="lambda_family",
        fixed_lambda=float(lambda_family),
        adaptive=bool(adaptive_lambda),
        lambda_min=float(adaptive_lambda_min),
        lambda_max=float(adaptive_lambda_max),
        eps=float(adaptive_lambda_eps),
        min_total_samples=int(adaptive_lambda_min_total_samples),
        previous_lambda_by_parent=(previous_lambda_maps or {}).get("family"),
        lambda_smoothing_alpha=float(lambda_smoothing_alpha),
        lambda_shock_cap_pct=float(lambda_shock_cap_pct),
    )
    out = out.merge(
        lambda_family_df[
            global_cols
            + [
                "lambda_family",
                "lambda_family_status",
                "lambda_family_sigma_within",
                "lambda_family_sigma_between",
                "lambda_family_total_n",
                "lambda_family_child_count",
                "lambda_family_raw",
                "lambda_family_prev",
            ]
        ],
        on=global_cols,
        how="left",
    )

    out["shrinkage_weight_family"] = np.where(
        out["lambda_family_status"] == "insufficient_data",
        1.0,
        out["n_family"] / (out["n_family"] + pd.to_numeric(out["lambda_family"], errors="coerce").fillna(float(lambda_family))),
    )
    out["shrinkage_weight_family"] = out["shrinkage_weight_family"].replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(0.0, 1.0)
    out["effect_shrunk_family"] = (
        out["shrinkage_weight_family"] * out["mean_family"]
        + (1.0 - out["shrinkage_weight_family"]) * out["mean_global"]
    )

    event_stats = _aggregate_effect_units(
        out,
        unit_cols=event_cols,
        n_col="_n",
        mean_col="effect_raw",
        var_col="_var",
        prefix="event",
    )
    out = out.merge(event_stats[event_cols + ["mean_event", "n_event", "var_event"]], on=event_cols, how="left")

    lambda_event_df = _estimate_adaptive_lambda(
        event_stats,
        parent_cols=family_cols,
        child_col="_event",
        n_col="n_event",
        mean_col="mean_event",
        var_col="var_event",
        lambda_name="lambda_event",
        fixed_lambda=float(lambda_event),
        adaptive=bool(adaptive_lambda),
        lambda_min=float(adaptive_lambda_min),
        lambda_max=float(adaptive_lambda_max),
        eps=float(adaptive_lambda_eps),
        min_total_samples=int(adaptive_lambda_min_total_samples),
        previous_lambda_by_parent=(previous_lambda_maps or {}).get("event"),
        lambda_smoothing_alpha=float(lambda_smoothing_alpha),
        lambda_shock_cap_pct=float(lambda_shock_cap_pct),
    )
    out = out.merge(
        lambda_event_df[
            family_cols
            + [
                "lambda_event",
                "lambda_event_status",
                "lambda_event_sigma_within",
                "lambda_event_sigma_between",
                "lambda_event_total_n",
                "lambda_event_child_count",
                "lambda_event_raw",
                "lambda_event_prev",
            ]
        ],
        on=family_cols,
        how="left",
    )

    out["shrinkage_weight_event"] = np.where(
        out["lambda_event_status"] == "insufficient_data",
        1.0,
        out["n_event"] / (out["n_event"] + pd.to_numeric(out["lambda_event"], errors="coerce").fillna(float(lambda_event))),
    )
    out["shrinkage_weight_event"] = out["shrinkage_weight_event"].replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(0.0, 1.0)
    out["effect_shrunk_event"] = (
        out["shrinkage_weight_event"] * out["mean_event"]
        + (1.0 - out["shrinkage_weight_event"]) * out["effect_shrunk_family"]
    )

    state_mask = out["_state"] != ""
    state_stats = _aggregate_effect_units(
        out[state_mask],
        unit_cols=state_cols,
        n_col="_n",
        mean_col="effect_raw",
        var_col="_var",
        prefix="state",
    )
    out = out.merge(state_stats[state_cols + ["mean_state", "n_state", "var_state"]], on=state_cols, how="left")

    lambda_state_df = _estimate_adaptive_lambda(
        state_stats,
        parent_cols=event_cols,
        child_col="_state",
        n_col="n_state",
        mean_col="mean_state",
        var_col="var_state",
        lambda_name="lambda_state",
        fixed_lambda=float(lambda_state),
        adaptive=bool(adaptive_lambda),
        lambda_min=float(adaptive_lambda_min),
        lambda_max=float(adaptive_lambda_max),
        eps=float(adaptive_lambda_eps),
        min_total_samples=int(adaptive_lambda_min_total_samples),
        previous_lambda_by_parent=(previous_lambda_maps or {}).get("state"),
        lambda_smoothing_alpha=float(lambda_smoothing_alpha),
        lambda_shock_cap_pct=float(lambda_shock_cap_pct),
    )
    out = out.merge(
        lambda_state_df[
            event_cols
            + [
                "lambda_state",
                "lambda_state_status",
                "lambda_state_sigma_within",
                "lambda_state_sigma_between",
                "lambda_state_total_n",
                "lambda_state_child_count",
                "lambda_state_raw",
                "lambda_state_prev",
            ]
        ],
        on=event_cols,
        how="left",
    )

    out["shrinkage_weight_state"] = np.where(
        out["lambda_state_status"] == "insufficient_data",
        1.0,
        out["_n"] / (out["_n"] + pd.to_numeric(out["lambda_state"], errors="coerce").fillna(float(lambda_state))),
    )
    out["shrinkage_weight_state"] = out["shrinkage_weight_state"].replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(0.0, 1.0)
    out.loc[~state_mask, "lambda_state_status"] = "no_state"
    out.loc[~state_mask, "lambda_state"] = 0.0
    out.loc[~state_mask, "shrinkage_weight_state"] = 1.0

    out["effect_shrunk_state"] = np.where(
        state_mask,
        out["shrinkage_weight_state"] * out["mean_state"].fillna(out["effect_raw"])
        + (1.0 - out["shrinkage_weight_state"]) * out["effect_shrunk_event"],
        out["effect_shrunk_event"],
    )

    # Build shrunken p-values from state-shrunk effect and raw standard error.
    se = out["std_return"] / np.sqrt(np.maximum(out["_n"], 1.0))
    valid_se = np.isfinite(se) & (se > 0.0) & (out["_n"] > 1.0)
    p_shrunk = out["p_value_raw"].astype(float).copy()
    t_shrunk = pd.Series(0.0, index=out.index, dtype=float)
    t_shrunk.loc[valid_se] = out.loc[valid_se, "effect_shrunk_state"] / se.loc[valid_se]
    p_shrunk.loc[valid_se] = t_shrunk.loc[valid_se].apply(_two_sided_p_from_t)
    out["p_value_shrunk"] = pd.to_numeric(p_shrunk, errors="coerce").fillna(out["p_value_raw"]).clip(0.0, 1.0)
    out["p_value_for_fdr"] = out["p_value_shrunk"]

    drop_cols = [
        "_n",
        "_family",
        "_event",
        "_verb",
        "_horizon",
        "_symbol",
        "_state",
        "mean_global",
        "n_global",
        "mean_family",
        "n_family",
        "mean_event",
        "n_event",
        "var_event",
        "mean_state",
        "n_state",
        "var_state",
        "mean_global",
        "n_global",
        "var_global",
        "mean_family",
        "n_family",
        "var_family",
    ]
    return out.drop(columns=[c for c in drop_cols if c in out.columns], errors="ignore")


def _refresh_phase2_metrics_after_shrinkage(
    df: pd.DataFrame,
    *,
    min_after_cost: float,
    conservative_cost_multiplier: float,
    min_sample_size_gate: int,
    require_sign_stability: bool,
    quality_floor_fallback: float,
    min_events_fallback: int,
    min_information_weight_state: float,
) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        return out

    known_gate_prefixes = (
        "ECONOMIC_GATE",
        "ECONOMIC_CONSERVATIVE",
        "STABILITY_GATE",
        "MIN_SAMPLE_SIZE_GATE",
        "STATE_INFORMATION_WEIGHT",
    )

    out["expectancy"] = pd.to_numeric(out["effect_shrunk_state"], errors="coerce").fillna(
        pd.to_numeric(out.get("expectancy", 0.0), errors="coerce").fillna(0.0)
    )

    resolved_cost = pd.to_numeric(out.get("cost_bps_resolved", 0.0), errors="coerce").fillna(0.0) / 10000.0
    out["after_cost_expectancy"] = out["expectancy"] - resolved_cost
    out["after_cost_expectancy_per_trade"] = out["after_cost_expectancy"]
    out["stressed_after_cost_expectancy_per_trade"] = out["expectancy"] - (resolved_cost * float(conservative_cost_multiplier))

    out["p_value"] = pd.to_numeric(out.get("p_value_shrunk", out.get("p_value", 1.0)), errors="coerce").fillna(1.0).clip(0.0, 1.0)
    out["p_value_for_fdr"] = pd.to_numeric(out.get("p_value_for_fdr", out["p_value"]), errors="coerce").fillna(out["p_value"]).clip(0.0, 1.0)

    out["gate_economic"] = out["after_cost_expectancy"] >= float(min_after_cost)
    out["gate_economic_conservative"] = out["stressed_after_cost_expectancy_per_trade"] >= float(min_after_cost)
    out["gate_after_cost_positive"] = out["gate_economic"]
    out["gate_after_cost_stressed_positive"] = out["gate_economic_conservative"]
    if "gate_stability" in out.columns:
        gate_stability_col = out["gate_stability"]
    else:
        gate_stability_col = pd.Series(False, index=out.index)
    out["gate_stability"] = gate_stability_col.astype(bool)
    out["sample_size"] = pd.to_numeric(out.get("sample_size", out.get("n_events", 0)), errors="coerce").fillna(0).astype(int)
    out["n_events"] = pd.to_numeric(out.get("n_events", 0), errors="coerce").fillna(0).astype(int)

    sample_gate_pass = (
        out["sample_size"] >= int(min_sample_size_gate)
        if int(min_sample_size_gate) > 0
        else pd.Series(True, index=out.index)
    )
    if "state_id" in out.columns:
        state_id_col = out["state_id"]
    else:
        state_id_col = pd.Series("", index=out.index)
    if "shrinkage_weight_state" in out.columns:
        state_weight_col = out["shrinkage_weight_state"]
    else:
        state_weight_col = pd.Series(1.0, index=out.index)
    out["gate_state_information"] = (
        state_id_col.fillna("").astype(str).str.strip() == ""
    ) | (pd.to_numeric(state_weight_col, errors="coerce").fillna(0.0) >= float(min_information_weight_state))

    stability_ok = out["gate_stability"] if require_sign_stability else pd.Series(True, index=out.index)
    out["gate_phase2_research"] = out["gate_economic_conservative"] & sample_gate_pass & stability_ok & out["gate_state_information"]
    out["gate_phase2_final"] = out["gate_phase2_research"]

    out["robustness_score"] = (
        out["gate_economic"].astype(float)
        + out["gate_economic_conservative"].astype(float)
        + out["gate_stability"].astype(float)
        + out["gate_state_information"].astype(float)
    ) / 4.0
    out["phase2_quality_score"] = out["robustness_score"]
    out["phase2_quality_components"] = out.apply(
        lambda r: json.dumps(
            {
                "econ": int(bool(r["gate_economic"])),
                "econ_cons": int(bool(r["gate_economic_conservative"])),
                "stability": int(bool(r["gate_stability"])),
                "state_info": int(bool(r["gate_state_information"])),
            },
            sort_keys=True,
        ),
        axis=1,
    )
    out["compile_eligible_phase2_fallback"] = (
        (out["phase2_quality_score"] >= float(quality_floor_fallback))
        & (out["n_events"] >= int(min_events_fallback))
    )

    def _refresh_fail_reasons(row: pd.Series) -> str:
        prior = [s.strip() for s in str(row.get("fail_reasons", "")).split(",") if s.strip()]
        keep = [s for s in prior if not s.startswith(known_gate_prefixes)]
        if not bool(row.get("gate_economic", False)):
            keep.append("ECONOMIC_GATE")
        if not bool(row.get("gate_economic_conservative", False)):
            keep.append("ECONOMIC_CONSERVATIVE")
        if require_sign_stability and not bool(row.get("gate_stability", False)):
            keep.append("STABILITY_GATE")
        if int(min_sample_size_gate) > 0 and int(row.get("sample_size", 0)) < int(min_sample_size_gate):
            keep.append("MIN_SAMPLE_SIZE_GATE")
        if not bool(row.get("gate_state_information", True)):
            keep.append("STATE_INFORMATION_WEIGHT")
        return ",".join(dict.fromkeys(keep))

    out["fail_reasons"] = out.apply(_refresh_fail_reasons, axis=1)
    out["promotion_track"] = np.where(out["gate_phase2_final"], "standard", "fallback_only")
    return out


def _load_gates_spec() -> Dict[str, Any]:
    path = REPO_ROOT / "spec" / "gates.yaml"
    if not path.exists():
        return {}
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _select_phase2_gate_spec(
    gates_spec: Dict[str, Any],
    *,
    mode: str,
    gate_profile: str,
) -> Dict[str, Any]:
    """Resolve phase2 gate settings by profile with backward compatibility.

    Selection:
    - gate_profile=auto => discovery when mode=research, else promotion
    - explicit gate_profile in {discovery,promotion} wins.
    """
    base_cfg = gates_spec.get("gate_v1_phase2", {}) if isinstance(gates_spec, dict) else {}
    if not isinstance(base_cfg, dict):
        base_cfg = {}

    profile_choice = str(gate_profile or "auto").strip().lower()
    mode_choice = str(mode or "production").strip().lower()
    if profile_choice == "auto":
        resolved_profile = "discovery" if mode_choice == "research" else "promotion"
    else:
        resolved_profile = profile_choice

    profiles = gates_spec.get("gate_v1_phase2_profiles", {}) if isinstance(gates_spec, dict) else {}
    profile_cfg = profiles.get(resolved_profile, {}) if isinstance(profiles, dict) else {}
    if not isinstance(profile_cfg, dict):
        profile_cfg = {}

    merged = dict(base_cfg)
    for key, value in profile_cfg.items():
        if key == "event_overrides" and isinstance(value, dict):
            merged_overrides = merged.get("event_overrides", {})
            if not isinstance(merged_overrides, dict):
                merged_overrides = {}
            merged_overrides = dict(merged_overrides)
            for event_type, override_cfg in value.items():
                if isinstance(override_cfg, dict):
                    prior = merged_overrides.get(event_type, {})
                    if not isinstance(prior, dict):
                        prior = {}
                    merged_overrides[event_type] = {**prior, **override_cfg}
                else:
                    merged_overrides[event_type] = override_cfg
            merged["event_overrides"] = merged_overrides
        else:
            merged[key] = value
    merged["_resolved_profile"] = resolved_profile
    return merged


def _load_family_spec() -> Dict[str, Any]:
    path = REPO_ROOT / "spec" / "multiplicity" / "families.yaml"
    if not path.exists():
        return {}
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _load_global_defaults() -> Dict[str, Any]:
    return load_global_defaults(project_root=PROJECT_ROOT)


def _load_template_verb_lexicon() -> Dict[str, Any]:
    path = ontology_spec_paths(REPO_ROOT).get("template_verb_lexicon")
    if not path or not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    return payload if isinstance(payload, dict) else {}


def _operator_registry(verb_lexicon: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    operators = verb_lexicon.get("operators", {})
    if not isinstance(operators, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for key, value in operators.items():
        if not isinstance(value, dict):
            continue
        verb = str(key).strip()
        if not verb:
            continue
        out[verb] = dict(value)
    return out


def _validate_operator_for_event(
    *,
    template_verb: str,
    canonical_family: str,
    operator_registry: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    op = operator_registry.get(str(template_verb).strip(), {})
    if not op:
        raise ValueError(f"Missing operator definition for template verb: {template_verb}")
    compatible = op.get("compatible_families", [])
    if isinstance(compatible, list) and compatible:
        allowed = {str(x).strip().upper() for x in compatible if str(x).strip()}
        if str(canonical_family).strip().upper() not in allowed:
            raise ValueError(
                f"Template verb {template_verb} is incompatible with family {canonical_family}; "
                f"allowed={sorted(allowed)}"
            )
    return op


def _resolve_phase2_gate_params(gate_v1_phase2: Dict[str, Any], event_type: str) -> Dict[str, Any]:
    event_overrides = gate_v1_phase2.get("event_overrides", {}) if isinstance(gate_v1_phase2, dict) else {}
    per_event = event_overrides.get(event_type, {}) if isinstance(event_overrides, dict) else {}
    if not isinstance(per_event, dict):
        per_event = {}

    def _pick(key: str, default: Any) -> Any:
        if key in per_event:
            return per_event[key]
        if isinstance(gate_v1_phase2, dict) and key in gate_v1_phase2:
            return gate_v1_phase2[key]
        return default

    conservative_mult = float(_pick("conservative_cost_multiplier", 1.5))
    conservative_mult = max(1.0, conservative_mult)
    return {
        "max_q_value": float(_pick("max_q_value", 0.05)),
        "min_after_cost_expectancy_bps": float(_pick("min_after_cost_expectancy_bps", 0.1)),
        "min_sample_size": int(_pick("min_sample_size", 0) or 0),
        "require_sign_stability": bool(_pick("require_sign_stability", True)),
        "quality_floor_fallback": float(_pick("quality_floor_fallback", 0.66)),
        "min_events_fallback": int(_pick("min_events_fallback", 100)),
        "conservative_cost_multiplier": conservative_mult,
    }


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
    extra_evt_cols = []
    for col in (
        "vol_regime",
        "liquidity_state",
        "market_liquidity_state",
        "depth_state",
        "event_direction",
        "direction",
        "signal_direction",
        "flow_direction",
        "breakout_direction",
        "shock_direction",
        "move_direction",
        "leader_direction",
        "return_1",
        "return_sign",
        "sign",
        "polarity",
        "funding_z",
        "basis_z",
        "side",
        "trade_side",
        "signal_side",
        "direction_label",
    ):
        if col in evt.columns:
            extra_evt_cols.append(col)
    evt_cols = ["_event_ts"] + extra_evt_cols
    evt_for_join = evt[evt_cols].rename(
        columns={"_event_ts": "timestamp", **{c: f"evt_{c}" for c in extra_evt_cols}}
    )
    merged = pd.merge_asof(
        evt_for_join,
        feat,
        on="timestamp",
        direction="backward",
    )
    return merged


def calculate_expectancy_stats(
    sym_events: pd.DataFrame,
    features_df: pd.DataFrame,
    rule: str,
    horizon: str,
    canonical_family: str = "",
    shift_labels_k: int = 0,
    entry_lag_bars: int = 1,
    min_samples: int = 30,
    time_decay_enabled: bool = False,
    time_decay_tau_seconds: Optional[float] = None,
    time_decay_floor_weight: float = 0.02,
    regime_conditioned_decay: bool = False,
    regime_tau_smoothing_alpha: float = 0.15,
    regime_tau_min_days: float = 3.0,
    regime_tau_max_days: float = 365.0,
    directional_asymmetry_decay: bool = False,
    directional_tau_smoothing_alpha: float = 0.15,
    directional_tau_min_ratio: float = 1.5,
    directional_tau_max_ratio: float = 3.0,
    directional_tau_default_up_mult: float = 1.25,
    directional_tau_default_down_mult: float = 0.65,
) -> Dict[str, Any]:
    """
    Real PIT expectancy calculation.

    Args:
        shift_labels_k: If > 0, shift the forward label by an additional k bars
            (future_pos = pos + entry_lag_bars + horizon_bars + shift_labels_k).  This implements
            a *true* misalignment canary: deterministic, and tests specifically
            whether the pipeline is sensitive to label-offset errors.

    Returns a stats payload with mean_return, p_value, n_events, stability_pass,
    std_return, and t_stat.
    """
    if sym_events.empty or features_df.empty:
        return {
            "mean_return": 0.0,
            "p_value": 1.0,
            "n_events": 0.0,
            "n_effective": 0.0,
            "stability_pass": False,
            "std_return": 0.0,
            "t_stat": 0.0,
            "time_weight_sum": 0.0,
            "mean_weight_age_days": 0.0,
            "mean_tau_days": 0.0,
            "learning_rate_mean": 0.0,
            "mean_tau_up_days": 0.0,
            "mean_tau_down_days": 0.0,
            "tau_directional_ratio": 0.0,
            "directional_up_share": 0.0,
        }

    if int(entry_lag_bars) < 1:
        raise ValueError("entry_lag_bars must be >= 1 to prevent same-bar entry leakage")

    horizon_bars = _horizon_to_bars(horizon)
    direction = _TEMPLATE_DIRECTION.get(rule, 1)

    # Join events to features at t0
    merged = _join_events_to_features(sym_events, features_df)
    if merged.empty or "close" not in merged.columns:
        return {
            "mean_return": 0.0,
            "p_value": 1.0,
            "n_events": 0.0,
            "n_effective": 0.0,
            "stability_pass": False,
            "std_return": 0.0,
            "t_stat": 0.0,
            "time_weight_sum": 0.0,
            "mean_weight_age_days": 0.0,
            "mean_tau_days": 0.0,
            "learning_rate_mean": 0.0,
            "mean_tau_up_days": 0.0,
            "mean_tau_down_days": 0.0,
            "tau_directional_ratio": 0.0,
            "directional_up_share": 0.0,
        }

    # Compute forward return at t0 using aligned feature rows.
    # We use the features_df index to look up close horizon_bars later.
    feat_close = features_df["close"].astype(float).values
    # Use DatetimeIndex for timezone-safe searchsorted (handles UTC-aware ts).
    feat_ts_idx = pd.to_datetime(features_df["timestamp"], utc=True)

    event_returns: List[float] = []
    event_ts_list: List[pd.Timestamp] = []
    event_vol_list: List[Any] = []
    event_liq_list: List[Any] = []
    event_dir_list: List[int] = []
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
        event_ts_list.append(pd.to_datetime(ts, utc=True, errors="coerce"))
        event_vol_list.append(row.get("evt_vol_regime", ""))
        event_liq_list.append(row.get("evt_liquidity_state", row.get("evt_market_liquidity_state", row.get("evt_depth_state", ""))))
        event_dir_list.append(
            _event_direction_from_joined_row(
                row,
                canonical_family=canonical_family,
                fallback_direction=direction,
            )
        )

    if len(event_returns) < min_samples:
        std_ret = float(pd.Series(event_returns, dtype=float).std()) if len(event_returns) > 1 else 0.0
        n_eff = float(len(event_returns))
        return {
            "mean_return": 0.0,
            "p_value": 1.0,
            "n_events": float(len(event_returns)),
            "n_effective": n_eff,
            "stability_pass": False,
            "std_return": std_ret,
            "t_stat": 0.0,
            "time_weight_sum": float(len(event_returns)),
            "mean_weight_age_days": 0.0,
            "mean_tau_days": 0.0,
            "learning_rate_mean": 0.0,
            "mean_tau_up_days": 0.0,
            "mean_tau_down_days": 0.0,
            "tau_directional_ratio": 0.0,
            "directional_up_share": 0.0,
        }

    returns_series = pd.Series(event_returns, dtype=float)
    ts_series = pd.to_datetime(pd.Series(event_ts_list), utc=True, errors="coerce")
    tau_days_list: List[float] = []
    direction_used: List[int] = []
    asymmetry_ratio_used: List[float] = []
    if bool(time_decay_enabled):
        ref_ts = ts_series.max()
        tau_seconds_default = float(time_decay_tau_seconds or (60.0 * 60.0 * 24.0 * 60.0))
        if bool(regime_conditioned_decay) or bool(directional_asymmetry_decay):
            prev_tau_days: Optional[float] = None
            alpha_src = regime_tau_smoothing_alpha if bool(regime_conditioned_decay) else directional_tau_smoothing_alpha
            alpha = max(0.0, min(1.0, float(alpha_src)))
            for vol_regime, liq_state, evt_dir in zip(event_vol_list, event_liq_list, event_dir_list):
                if bool(regime_conditioned_decay):
                    base_tau_days = _regime_conditioned_tau_days(
                        canonical_family=canonical_family,
                        vol_regime=vol_regime,
                        liquidity_state=liq_state,
                        base_tau_days_override=(tau_seconds_default / 86400.0),
                    )
                    base_tau_days = float(np.clip(base_tau_days, float(regime_tau_min_days), float(regime_tau_max_days)))
                else:
                    base_tau_days = float(tau_seconds_default / 86400.0)

                if bool(directional_asymmetry_decay):
                    raw_tau_days, _, _, asym_ratio = _asymmetric_tau_days(
                        base_tau_days=base_tau_days,
                        canonical_family=canonical_family,
                        direction=int(evt_dir),
                        default_up_mult=float(directional_tau_default_up_mult),
                        default_down_mult=float(directional_tau_default_down_mult),
                        min_ratio=float(directional_tau_min_ratio),
                        max_ratio=float(directional_tau_max_ratio),
                    )
                    asymmetry_ratio_used.append(float(asym_ratio))
                else:
                    raw_tau_days = float(base_tau_days)

                raw_tau_days = float(np.clip(raw_tau_days, float(regime_tau_min_days), float(regime_tau_max_days)))
                if prev_tau_days is None:
                    smoothed_tau_days = raw_tau_days
                else:
                    smoothed_tau_days = ((1.0 - alpha) * prev_tau_days) + (alpha * raw_tau_days)
                tau_days_list.append(float(smoothed_tau_days))
                direction_used.append(1 if int(evt_dir) >= 0 else -1)
                prev_tau_days = float(smoothed_tau_days)
            tau_seconds_arr = np.array(tau_days_list, dtype=float) * 86400.0
            age_seconds = (ref_ts - ts_series).dt.total_seconds().fillna(0.0).clip(lower=0.0).values
            raw_w = np.exp(-age_seconds / np.maximum(tau_seconds_arr, 1e-9))
            weights = pd.Series(np.maximum(raw_w, float(time_decay_floor_weight)), index=returns_series.index, dtype=float)
        else:
            weights = _time_decay_weights(
                ts_series,
                ref_ts=ref_ts,
                tau_seconds=tau_seconds_default,
                floor_weight=float(time_decay_floor_weight),
            )
    else:
        weights = pd.Series(1.0, index=returns_series.index, dtype=float)

    n_eff = _effective_sample_size(weights)
    w_sum = float(weights.sum())
    if w_sum <= 0.0:
        mean_ret = float(returns_series.mean())
        var_ret = float(returns_series.var(ddof=0))
    else:
        mean_ret = float((returns_series * weights).sum() / w_sum)
        var_ret = float(((weights * (returns_series - mean_ret) ** 2).sum()) / w_sum)

    std_ret = float(np.sqrt(max(var_ret, 0.0)))
    if std_ret > 0.0 and n_eff > 1.0:
        t_stat = float(mean_ret / (std_ret / np.sqrt(max(n_eff, 1.0))))
    else:
        t_stat = 0.0
    p_value = _two_sided_p_from_t(t_stat)

    # Sign stability: first vs second time half
    n = len(event_returns)
    mid = n // 2
    first_half = pd.Series(event_returns[:mid], dtype=float)
    second_half = pd.Series(event_returns[mid:], dtype=float)
    first_w = weights.iloc[:mid] if len(weights) >= mid else pd.Series(1.0, index=first_half.index, dtype=float)
    second_w = weights.iloc[mid:] if len(weights) >= n else pd.Series(1.0, index=second_half.index, dtype=float)
    first_sign = float((first_half * first_w).sum() / max(float(first_w.sum()), 1e-12)) if not first_half.empty else 0.0
    second_sign = float((second_half * second_w).sum() / max(float(second_w.sum()), 1e-12)) if not second_half.empty else 0.0
    stability_pass = (np.sign(first_sign) == np.sign(second_sign)) and (first_sign != 0) and (second_sign != 0)

    if bool(time_decay_enabled) and not ts_series.empty:
        ref_ts = ts_series.max()
        age_days = (ref_ts - ts_series).dt.total_seconds().fillna(0.0).clip(lower=0.0) / 86400.0
        mean_weight_age_days = float((age_days * weights).sum() / max(float(weights.sum()), 1e-12))
        if bool(regime_conditioned_decay) or bool(directional_asymmetry_decay):
            tau_days_vals = np.maximum(np.array(tau_days_list, dtype=float), 1e-9)
            mean_tau_days = float(np.mean(tau_days_vals))
            learning_rate_mean = float(np.mean(1.0 / tau_days_vals))
        else:
            tau_days_const = float(time_decay_tau_seconds or (60.0 * 60.0 * 24.0 * 60.0)) / 86400.0
            mean_tau_days = tau_days_const
            learning_rate_mean = 1.0 / max(tau_days_const, 1e-9)
    else:
        mean_weight_age_days = 0.0
        mean_tau_days = 0.0
        learning_rate_mean = 0.0

    if direction_used and tau_days_list:
        tau_arr = np.array(tau_days_list, dtype=float)
        dir_arr = np.array(direction_used, dtype=int)
        up_vals = tau_arr[dir_arr >= 0]
        down_vals = tau_arr[dir_arr < 0]
        mean_tau_up_days = float(np.mean(up_vals)) if up_vals.size else 0.0
        mean_tau_down_days = float(np.mean(down_vals)) if down_vals.size else 0.0
        tau_directional_ratio = (
            float(np.mean(asymmetry_ratio_used))
            if asymmetry_ratio_used
            else (
                float(mean_tau_up_days / max(mean_tau_down_days, 1e-9))
                if (mean_tau_up_days > 0.0 and mean_tau_down_days > 0.0)
                else 0.0
            )
        )
        directional_up_share = float((dir_arr >= 0).mean()) if dir_arr.size else 0.0
    else:
        mean_tau_up_days = 0.0
        mean_tau_down_days = 0.0
        tau_directional_ratio = 0.0
        directional_up_share = 0.0

    return {
        "mean_return": mean_ret,
        "p_value": p_value,
        "n_events": float(n),
        "n_effective": float(n_eff if n_eff > 0.0 else n),
        "stability_pass": bool(stability_pass),
        "std_return": std_ret,
        "t_stat": float(t_stat),
        "time_weight_sum": w_sum,
        "mean_weight_age_days": mean_weight_age_days,
        "mean_tau_days": mean_tau_days,
        "learning_rate_mean": learning_rate_mean,
        "mean_tau_up_days": mean_tau_up_days,
        "mean_tau_down_days": mean_tau_down_days,
        "tau_directional_ratio": tau_directional_ratio,
        "directional_up_share": directional_up_share,
    }


def calculate_expectancy(
    sym_events: pd.DataFrame,
    features_df: pd.DataFrame,
    rule: str,
    horizon: str,
    shift_labels_k: int = 0,
    entry_lag_bars: int = 1,
    min_samples: int = 30,
) -> Tuple[float, float, float, bool]:
    stats = calculate_expectancy_stats(
        sym_events=sym_events,
        features_df=features_df,
        rule=rule,
        horizon=horizon,
        shift_labels_k=shift_labels_k,
        entry_lag_bars=entry_lag_bars,
        min_samples=min_samples,
    )
    return (
        float(stats["mean_return"]),
        float(stats["p_value"]),
        float(stats["n_events"]),
        bool(stats["stability_pass"]),
    )


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
    parser.add_argument(
        "--cost_calibration_mode",
        choices=["static", "tob_regime"],
        default="static",
        help="Candidate cost calibration mode. static uses fees.yaml only; tob_regime calibrates slippage from ToB regimes.",
    )
    parser.add_argument(
        "--cost_min_tob_coverage",
        type=float,
        default=0.60,
        help="Minimum event-to-ToB alignment coverage required for tob_regime calibration before falling back to static costs.",
    )
    parser.add_argument(
        "--cost_tob_tolerance_minutes",
        type=int,
        default=10,
        help="Max backward asof tolerance (minutes) when aligning events to tob_5m_agg rows for cost calibration.",
    )
    parser.add_argument("--config", action="append", default=[],
                        help="Path to execution cost config (fees.yaml).")
    parser.add_argument("--shift_labels_k", type=int, default=0,
                        help="If > 0, shift labels by k bars (true misalignment canary).")
    parser.add_argument("--entry_lag_bars", type=int, default=1,
                        help="Entry lag in bars after event timestamp. Must be >= 1 (default: 1).")
    parser.add_argument(
        "--mode",
        choices=["research", "production", "certification"],
        default="production",
        help="Run mode. Research mode enables diagnostics-first candidate handling.",
    )
    parser.add_argument(
        "--gate_profile",
        choices=["auto", "discovery", "promotion"],
        default="auto",
        help="Phase2 gate profile. auto => discovery for research mode, promotion otherwise.",
    )
    parser.add_argument("--candidate_plan", default=None,
                        help="Path to Atlas candidate plan (jsonl). If provided, drives discovery.")
    parser.add_argument("--atlas_mode", type=int, default=0,
                        help="If 1, require --candidate_plan and fail if missing.")
    parser.add_argument(
        "--assert_condition_keys",
        type=int,
        default=1,
        help="If 1, fail-closed when plan conditioning keys are not in joined key contract for the symbol.",
    )
    parser.add_argument("--min_samples", type=int, default=30,
                        help="Minimum number of events required to compute expectancy.")
    parser.add_argument(
        "--allow_ontology_hash_mismatch",
        type=int,
        default=0,
        help="If 1, allow ontology hash drift between candidate plan, run manifest, and current specs.",
    )
    parser.add_argument(
        "--enable_hierarchical_shrinkage",
        type=int,
        default=1,
        help="If 1, apply empirical-Bayes family->event->state shrinkage before BH-FDR.",
    )
    parser.add_argument(
        "--adaptive_shrinkage_lambda",
        type=int,
        default=1,
        help="If 1, estimate lambda_state/lambda_event/lambda_family via variance decomposition.",
    )
    parser.add_argument("--lambda_state", type=float, default=100.0, help="Shrinkage strength for state -> event pooling.")
    parser.add_argument("--lambda_event", type=float, default=300.0, help="Shrinkage strength for event -> family pooling.")
    parser.add_argument("--lambda_family", type=float, default=1000.0, help="Shrinkage strength for family -> global pooling.")
    parser.add_argument("--adaptive_lambda_min", type=float, default=5.0, help="Lower clamp for adaptive lambda.")
    parser.add_argument("--adaptive_lambda_max", type=float, default=5000.0, help="Upper clamp for adaptive lambda.")
    parser.add_argument("--adaptive_lambda_eps", type=float, default=1e-8, help="Stabilizer for adaptive lambda denominator.")
    parser.add_argument(
        "--adaptive_lambda_min_total_samples",
        type=int,
        default=200,
        help="Minimum total samples required per parent group before adaptive lambda is estimated.",
    )
    parser.add_argument(
        "--enable_time_decay",
        type=int,
        default=1,
        help="If 1, compute expectancy statistics with exponential time decay.",
    )
    parser.add_argument(
        "--time_decay_half_life_days",
        type=float,
        default=None,
        help="Optional global half-life override in days for time-decayed weighting.",
    )
    parser.add_argument(
        "--time_decay_floor_weight",
        type=float,
        default=0.02,
        help="Minimum historical anchor weight for time-decayed weighting.",
    )
    parser.add_argument(
        "--enable_regime_conditioned_decay",
        type=int,
        default=1,
        help="If 1, adjust decay half-life by vol_regime and liquidity_state.",
    )
    parser.add_argument(
        "--regime_tau_smoothing_alpha",
        type=float,
        default=0.15,
        help="EMA smoothing alpha for regime-conditioned tau transitions.",
    )
    parser.add_argument(
        "--regime_tau_min_days",
        type=float,
        default=3.0,
        help="Minimum tau (days) after regime multipliers.",
    )
    parser.add_argument(
        "--regime_tau_max_days",
        type=float,
        default=365.0,
        help="Maximum tau (days) after regime multipliers.",
    )
    parser.add_argument(
        "--enable_directional_asymmetry_decay",
        type=int,
        default=1,
        help="If 1, apply directional asymmetric decay (tau_up != tau_down).",
    )
    parser.add_argument(
        "--directional_tau_smoothing_alpha",
        type=float,
        default=0.15,
        help="EMA smoothing alpha for directional asymmetric tau transitions.",
    )
    parser.add_argument(
        "--directional_tau_min_ratio",
        type=float,
        default=1.5,
        help="Minimum allowed tau_up / tau_down ratio.",
    )
    parser.add_argument(
        "--directional_tau_max_ratio",
        type=float,
        default=3.0,
        help="Maximum allowed tau_up / tau_down ratio.",
    )
    parser.add_argument(
        "--directional_tau_default_up_mult",
        type=float,
        default=1.25,
        help="Default directional multiplier for upside memory.",
    )
    parser.add_argument(
        "--directional_tau_default_down_mult",
        type=float,
        default=0.65,
        help="Default directional multiplier for downside memory.",
    )
    parser.add_argument(
        "--lambda_smoothing_alpha",
        type=float,
        default=0.1,
        help="EMA alpha for run-to-run adaptive lambda smoothing.",
    )
    parser.add_argument(
        "--lambda_shock_cap_pct",
        type=float,
        default=0.5,
        help="Maximum run-to-run fractional lambda change cap (e.g. 0.5 = +/-50%).",
    )
    parser.add_argument(
        "--min_information_weight_state",
        type=float,
        default=0.2,
        help="Minimum state shrinkage information weight required for state-conditioned promotion gates.",
    )
    return parser


def _validate_candidate_plan_ontology(
    plan_rows: List[Dict[str, Any]],
    *,
    run_manifest_ontology_hash: Optional[str],
    current_ontology_hash: str,
    allow_hash_mismatch: bool,
) -> List[str]:
    errors: List[str] = []

    if not run_manifest_ontology_hash and not allow_hash_mismatch:
        errors.append("run_manifest missing ontology_spec_hash while candidate_plan is provided")

    if run_manifest_ontology_hash:
        hash_mismatches = compare_hash_fields(
            run_manifest_ontology_hash,
            [("current_specs", current_ontology_hash)],
        )
        if hash_mismatches and not allow_hash_mismatch:
            errors.extend([f"ontology hash mismatch: {msg}" for msg in hash_mismatches])

    for idx, row in enumerate(plan_rows):
        row_prefix = f"plan_row[{idx}]"
        object_type = str(row.get("object_type", "event")).strip().lower()
        row_hash = str(row.get("ontology_spec_hash", "")).strip()
        if not row_hash:
            errors.append(f"{row_prefix}: missing ontology_spec_hash")
        if run_manifest_ontology_hash and row_hash and row_hash != run_manifest_ontology_hash:
            errors.append(
                f"{row_prefix}: ontology_spec_hash {row_hash} != run_manifest ontology_spec_hash {run_manifest_ontology_hash}"
            )
        if row_hash and row_hash != current_ontology_hash and not allow_hash_mismatch:
            errors.append(
                f"{row_prefix}: ontology_spec_hash {row_hash} != current ontology_spec_hash {current_ontology_hash}"
            )

        if object_type == "event":
            in_canonical = bool_field(row.get("ontology_in_canonical_registry", False))
            unknown_templates = parse_list_field(row.get("ontology_unknown_templates", []))
            canonical_event = str(row.get("canonical_event_type", "")).strip()
            runtime_event = str(row.get("runtime_event_type", "")).strip()
            event_type = str(row.get("event_type", "")).strip()

            if not in_canonical:
                errors.append(f"{row_prefix}: ontology_in_canonical_registry must be true for event rows")
            if unknown_templates:
                errors.append(f"{row_prefix}: ontology_unknown_templates must be empty for event rows")
            if not canonical_event:
                errors.append(f"{row_prefix}: canonical_event_type missing for event row")
            if event_type and canonical_event and event_type != canonical_event and event_type != runtime_event:
                errors.append(
                    f"{row_prefix}: event_type {event_type} is neither canonical_event_type {canonical_event} "
                    f"nor runtime_event_type {runtime_event or '<missing>'}"
                )

    return errors


def _validate_candidate_plan_condition_keys(
    plan_rows: List[Dict[str, Any]],
    *,
    data_root: Path,
    run_id: str,
    timeframe: str = "5m",
) -> List[str]:
    errors: List[str] = []
    available_cache: Dict[str, set[str]] = {}

    for idx, row in enumerate(plan_rows):
        conditioning = row.get("conditioning", {})
        if not isinstance(conditioning, dict) or not conditioning:
            continue
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol:
            errors.append(f"plan_row[{idx}]: missing symbol for condition-key validation")
            continue
        if symbol not in available_cache:
            available_cache[symbol] = load_symbol_joined_condition_keys(
                data_root=data_root,
                run_id=run_id,
                symbol=symbol,
                timeframe=timeframe,
                include_soft_defaults=False,
            )
        required = [str(key).strip() for key in conditioning.keys() if str(key).strip()]
        missing = sorted(missing_condition_keys(required, available_cache[symbol]))
        if missing:
            errors.append(
                f"plan_row[{idx}] ({symbol}) missing condition keys {missing}. "
                f"Available: {format_available_key_sample(available_cache[symbol])}"
            )
    return errors


def _load_previous_lambda_maps(
    *,
    data_root: Path,
    event_type: str,
    current_run_id: str,
) -> Tuple[Dict[str, Dict[Tuple[Any, ...], float]], Optional[Path]]:
    phase2_root = data_root / "reports" / "phase2"
    pattern = f"*/{event_type}/phase2_lambda_snapshot.parquet"
    candidates = []
    for path in phase2_root.glob(pattern):
        try:
            run_id = path.parts[-3]
        except Exception:
            continue
        if str(run_id) == str(current_run_id):
            continue
        candidates.append(path)
    if not candidates:
        return {}, None

    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    source = candidates[0]
    try:
        df = pd.read_parquet(source)
    except Exception:
        return {}, None

    out: Dict[str, Dict[Tuple[Any, ...], float]] = {"family": {}, "event": {}, "state": {}}
    if df.empty or "level" not in df.columns or "lambda_value" not in df.columns:
        return out, source

    def _safe_float(x: Any) -> Optional[float]:
        try:
            v = float(x)
        except (TypeError, ValueError):
            return None
        if not np.isfinite(v) or v <= 0.0:
            return None
        return v

    for _, row in df.iterrows():
        level = str(row.get("level", "")).strip().lower()
        lam = _safe_float(row.get("lambda_value"))
        if lam is None or level not in out:
            continue
        verb = str(row.get("template_verb", "")).strip()
        horizon = str(row.get("horizon", "")).strip()
        family = str(row.get("canonical_family", "")).strip().upper()
        event = str(row.get("canonical_event_type", "")).strip().upper()
        if level == "family":
            key = (verb, horizon)
        elif level == "event":
            key = (verb, horizon, family)
        else:
            key = (verb, horizon, family, event)
        out[level][key] = lam
    return out, source


def _build_lambda_snapshot(fdr_df: pd.DataFrame) -> pd.DataFrame:
    if fdr_df.empty:
        return pd.DataFrame(
            columns=[
                "level",
                "template_verb",
                "horizon",
                "canonical_family",
                "canonical_event_type",
                "lambda_value",
                "lambda_status",
            ]
        )

    rows: List[Dict[str, Any]] = []

    fam_cols = ["template_verb", "horizon", "lambda_family", "lambda_family_status"]
    for _, r in fdr_df[[c for c in fam_cols if c in fdr_df.columns]].drop_duplicates().iterrows():
        rows.append(
            {
                "level": "family",
                "template_verb": str(r.get("template_verb", "")),
                "horizon": str(r.get("horizon", "")),
                "canonical_family": "",
                "canonical_event_type": "",
                "lambda_value": float(pd.to_numeric(r.get("lambda_family", 0.0), errors="coerce") or 0.0),
                "lambda_status": str(r.get("lambda_family_status", "")),
            }
        )

    evt_cols = ["template_verb", "horizon", "canonical_family", "lambda_event", "lambda_event_status"]
    for _, r in fdr_df[[c for c in evt_cols if c in fdr_df.columns]].drop_duplicates().iterrows():
        rows.append(
            {
                "level": "event",
                "template_verb": str(r.get("template_verb", "")),
                "horizon": str(r.get("horizon", "")),
                "canonical_family": str(r.get("canonical_family", "")).upper(),
                "canonical_event_type": "",
                "lambda_value": float(pd.to_numeric(r.get("lambda_event", 0.0), errors="coerce") or 0.0),
                "lambda_status": str(r.get("lambda_event_status", "")),
            }
        )

    st_cols = ["template_verb", "horizon", "canonical_family", "canonical_event_type", "lambda_state", "lambda_state_status"]
    for _, r in fdr_df[[c for c in st_cols if c in fdr_df.columns]].drop_duplicates().iterrows():
        rows.append(
            {
                "level": "state",
                "template_verb": str(r.get("template_verb", "")),
                "horizon": str(r.get("horizon", "")),
                "canonical_family": str(r.get("canonical_family", "")).upper(),
                "canonical_event_type": str(r.get("canonical_event_type", "")).upper(),
                "lambda_value": float(pd.to_numeric(r.get("lambda_state", 0.0), errors="coerce") or 0.0),
                "lambda_status": str(r.get("lambda_state_status", "")),
            }
        )

    return pd.DataFrame(rows)


def _populate_fail_reasons(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    
    gate_cols = [
        "gate_economic",
        "gate_economic_conservative",
        "gate_stability",
        "gate_state_information",
        "gate_cost_model_valid",
        "gate_cost_ratio"
    ]
    
    df["fail_gate_primary"] = ""
    df["fail_reason_primary"] = ""
    
    for idx, row in df.iterrows():
        if not bool(row["gate_phase2_final"]):
            for gate in gate_cols:
                if gate in df.columns and not bool(row[gate]):
                    df.at[idx, "fail_gate_primary"] = gate
                    df.at[idx, "fail_reason_primary"] = f"failed_{gate}"
                    break
    return df

def _write_gate_summary(df: pd.DataFrame, out_path: Path):
    if df.empty:
        return
    
    gate_cols = [c for c in df.columns if c.startswith("gate_")]
    summary = {
        "candidates_total": int(len(df)),
        "pass_all_gates": int(df["gate_phase2_final"].sum()),
        "per_gate_pass_count": {c: int(df[c].sum()) for c in gate_cols if df[c].dtype == bool or df[c].dtype == int},
        "per_gate_fail_count": {c: int((~df[c].astype(bool)).sum()) for c in gate_cols if df[c].dtype == bool or df[c].dtype == int},
    }
    
    if "fail_gate_primary" in df.columns:
        top_fails = df[df["fail_gate_primary"] != ""]["fail_gate_primary"].value_counts().head(5).to_dict()
        summary["top_5_primary_fail_gates"] = {str(k): int(v) for k, v in top_fails.items()}
    
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=2)

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
    spec_hashes = get_spec_hashes(REPO_ROOT)
    current_ontology_hash = ontology_spec_hash(REPO_ROOT)
    current_ontology_components = ontology_component_hash_fields(ontology_component_hashes(REPO_ROOT))
    gates_spec = _load_gates_spec()
    gates = _select_phase2_gate_spec(
        gates_spec,
        mode=str(args.mode),
        gate_profile=str(args.gate_profile),
    )
    gate_cfg = _resolve_phase2_gate_params(gates, args.event_type)
    families_spec = _load_family_spec()
    verb_lexicon = _load_template_verb_lexicon()
    operator_registry = _operator_registry(verb_lexicon)
    operator_registry_version = str(verb_lexicon.get("version", "")).strip() or "unknown"

    # Resolve execution costs from spec configs — not a CLI float.
    cost_bps, cost_coordinate = _resolve_phase2_costs(args, PROJECT_ROOT)
    cost_calibrator = ToBRegimeCostCalibrator(
        run_id=args.run_id,
        data_root=DATA_ROOT,
        base_fee_bps=float(cost_coordinate["fee_bps_per_side"]),
        base_slippage_bps=float(cost_coordinate["slippage_bps_per_fill"]),
        static_cost_bps=float(cost_coordinate["cost_bps"]),
        mode=str(args.cost_calibration_mode),
        min_tob_coverage=float(args.cost_min_tob_coverage),
        tob_tolerance_minutes=int(args.cost_tob_tolerance_minutes),
    )
    cost_coordinate["calibration_mode"] = str(args.cost_calibration_mode)
    cost_coordinate["cost_min_tob_coverage"] = float(args.cost_min_tob_coverage)
    cost_coordinate["cost_tob_tolerance_minutes"] = int(args.cost_tob_tolerance_minutes)

    max_q = float(gate_cfg["max_q_value"])
    min_after_cost = float(gate_cfg["min_after_cost_expectancy_bps"]) / 10000.0  # bps → decimal
    min_sample_size_gate = int(gate_cfg["min_sample_size"])
    require_sign_stability = bool(gate_cfg["require_sign_stability"])
    quality_floor_fallback = float(gate_cfg["quality_floor_fallback"])
    min_events_fallback = int(gate_cfg["min_events_fallback"])
    conservative_cost_multiplier = float(gate_cfg["conservative_cost_multiplier"])

    # 2. Minimal Template Set & Horizons
    global_defaults = _load_global_defaults()
    fam_config = families_spec.get("families", {}).get(args.event_type, {})
    canonical_family_for_event = str(fam_config.get("canonical_family", args.event_type)).strip().upper() or str(args.event_type).strip().upper()
    templates = fam_config.get("templates", global_defaults.get("rule_templates", ["mean_reversion", "continuation", "carry"]))
    horizons = fam_config.get("horizons", global_defaults.get("horizons", ["5m", "15m", "60m"]))
    max_cands = fam_config.get("max_candidates_per_run", 1000)

    symbols = [s.strip() for s in args.symbols.split(",")]
    if len(symbols) * len(templates) * len(horizons) > max_cands:
        log.error("Search budget exceeded for %s. Limit: %d", args.event_type, max_cands)
        finalize_manifest(manifest, "failed", error="Search budget exceeded", stats={"limit": max_cands, "requested": len(symbols) * len(templates) * len(horizons)})
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
            symbol_set = {str(s).strip().upper() for s in symbols if str(s).strip()}
            if symbol_set and not events_df.empty and "symbol" in events_df.columns:
                events_df = events_df[events_df["symbol"].astype(str).str.upper().isin(symbol_set)].copy()
        except pd.errors.EmptyDataError:
            events_df = pd.DataFrame()

    # Load and merge market_state to support conditioning
    if not events_df.empty and "symbol" in events_df.columns:
        if "enter_ts" not in events_df.columns:
             for col in ["timestamp", "anchor_ts", "event_ts"]:
                 if col in events_df.columns:
                     events_df["enter_ts"] = events_df[col]
                     break
             if "enter_ts" not in events_df.columns:
                 log.warning("No enter_ts (or fallback timestamp, anchor_ts, event_ts) column found in events. Market state conditioning will be silently skipped, defaulting to unconditional.")
        
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
                            if "vol_regime_code" in sym_events.columns and sym_events["vol_regime_code"].isna().any():
                                log.warning(f"Market state merge for {sym} produced gaps > 1h (NaNs present in context columns).")
                    except Exception as e:
                        log.warning(f"Failed to load/merge market_state for {sym}: {e}")
                
                merged_dfs.append(sym_events)
            
            if merged_dfs:
                events_df = pd.concat(merged_dfs, ignore_index=True)

    # 3. Candidate Generation (Family = event_type × rule × horizon × condition)
    results = []
    skip_reason_counts: Counter[str] = Counter()
    missing_condition_columns: set[str] = set()
    
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

    run_manifest_hashes = load_run_manifest_hashes(DATA_ROOT, args.run_id)
    run_manifest_ontology_hash = run_manifest_hashes.get("ontology_spec_hash")
    allow_hash_mismatch = bool(int(args.allow_ontology_hash_mismatch))
    if plan_rows:
        ontology_errors = _validate_candidate_plan_ontology(
            plan_rows,
            run_manifest_ontology_hash=run_manifest_ontology_hash,
            current_ontology_hash=current_ontology_hash,
            allow_hash_mismatch=allow_hash_mismatch,
        )
        if ontology_errors:
            for msg in ontology_errors[:25]:
                log.error("Ontology contract violation: %s", msg)
            if len(ontology_errors) > 25:
                log.error("Ontology contract violation: ... %d additional errors omitted", len(ontology_errors) - 25)
            sys.exit(1)
        if bool(int(args.assert_condition_keys)):
            condition_key_errors = _validate_candidate_plan_condition_keys(
                plan_rows,
                data_root=DATA_ROOT,
                run_id=str(args.run_id),
                timeframe="5m",
            )
            if condition_key_errors:
                for msg in condition_key_errors[:25]:
                    log.error("Condition-key contract violation: %s", msg)
                if len(condition_key_errors) > 25:
                    log.error(
                        "Condition-key contract violation: ... %d additional errors omitted",
                        len(condition_key_errors) - 25,
                    )
                sys.exit(1)

    if plan_rows:
        # Atlas-driven discovery
        for plan_row in plan_rows:
            symbol = plan_row["symbol"]
            rule = plan_row["rule_template"]
            horizon = plan_row["horizon"]
            event_type = plan_row["event_type"]
            runtime_event_type = str(plan_row.get("runtime_event_type", event_type)).strip() or str(event_type)
            canonical_event_type = str(plan_row.get("canonical_event_type", event_type)).strip() or str(event_type)
            canonical_family = str(plan_row.get("canonical_family", canonical_family_for_event)).strip().upper() or canonical_family_for_event
            state_id = _optional_token(plan_row.get("state_id"))
            state_provenance = str(plan_row.get("state_provenance", "none")).strip() or "none"
            state_activation = _optional_token(plan_row.get("state_activation"))
            state_activation_hash = _optional_token(plan_row.get("state_activation_hash"))
            hypothesis_id = str(plan_row.get("hypothesis_id", "")).strip()
            hypothesis_version = int(plan_row.get("hypothesis_version", 0) or 0)
            hypothesis_spec_path = str(plan_row.get("hypothesis_spec_path", "")).strip()
            hypothesis_spec_hash = str(plan_row.get("hypothesis_spec_hash", "")).strip()
            hypothesis_metric = str(plan_row.get("hypothesis_metric", "")).strip()
            hypothesis_output_schema = plan_row.get("hypothesis_output_schema", [])
            hypothesis_candidate_id = str(plan_row.get("candidate_id", "")).strip()
            candidate_hash_inputs = str(plan_row.get("candidate_hash_inputs", "")).strip()
            template_id = str(plan_row.get("template_id", rule)).strip() or str(rule)
            horizon_bars = int(plan_row.get("horizon_bars", _horizon_to_bars(horizon)) or _horizon_to_bars(horizon))
            entry_lag_bars = int(plan_row.get("entry_lag_bars", args.entry_lag_bars) or args.entry_lag_bars)
            direction_rule = str(plan_row.get("direction_rule", "")).strip() or "both"
            condition_signature = str(plan_row.get("condition_signature", "")).strip() or "all"
            
            if event_type != args.event_type:
                skip_reason_counts["plan_event_type_mismatch"] += 1
                continue
            operator_def = _validate_operator_for_event(
                template_verb=str(rule),
                canonical_family=canonical_family,
                operator_registry=operator_registry,
            )
            operator_version = str(operator_def.get("operator_version", operator_registry_version)).strip() or operator_registry_version
                
            conditioning_map = plan_row.get("conditioning", {})
            min_samples = plan_row.get("min_events", args.min_samples)
            tau_days = _resolve_tau_days(canonical_family, args.time_decay_half_life_days)
            tau_seconds = float(tau_days) * 86400.0
            
            # Filter events for this symbol
            sym_events = events_df[events_df["symbol"] == symbol] if "symbol" in events_df.columns else events_df
            if sym_events.empty:
                skip_reason_counts["symbol_events_empty"] += 1
                continue
                
            # Apply conditioning if any
            bucket_events = sym_events
            cond_label = "all"
            if conditioning_map:
                missing_cols = [col for col in conditioning_map.keys() if col not in bucket_events.columns]
                if missing_cols:
                    skip_reason_counts["conditioning_columns_missing_in_events"] += 1
                    missing_condition_columns.update(str(col) for col in missing_cols if str(col).strip())
                for col, val in conditioning_map.items():
                    if col in bucket_events.columns:
                        bucket_events = bucket_events[bucket_events[col] == val]
                        cond_label = f"{col}_{val}"

            if state_id:
                state_col = _resolve_state_context_column(bucket_events.columns, state_id)
                if not state_col:
                    skip_reason_counts["state_column_missing"] += 1
                    continue
                state_mask = _bool_mask_from_series(bucket_events[state_col])
                bucket_events = bucket_events[state_mask]
                if bucket_events.empty:
                    skip_reason_counts["state_filter_empty"] += 1
                    continue
            
            if len(bucket_events) < min_samples:
                skip_reason_counts["below_min_samples_after_filters"] += 1
                continue
                
            features_df = _load_features(args.run_id, symbol)
            if features_df.empty:
                skip_reason_counts["features_empty"] += 1
                continue
                
            exp_stats = calculate_expectancy_stats(
                bucket_events, features_df, rule, horizon, canonical_family=canonical_family,
                shift_labels_k=args.shift_labels_k,
                entry_lag_bars=args.entry_lag_bars,
                min_samples=min_samples,
                time_decay_enabled=bool(int(args.enable_time_decay)),
                time_decay_tau_seconds=tau_seconds,
                time_decay_floor_weight=float(args.time_decay_floor_weight),
                regime_conditioned_decay=bool(int(args.enable_regime_conditioned_decay)),
                regime_tau_smoothing_alpha=float(args.regime_tau_smoothing_alpha),
                regime_tau_min_days=float(args.regime_tau_min_days),
                regime_tau_max_days=float(args.regime_tau_max_days),
                directional_asymmetry_decay=bool(int(args.enable_directional_asymmetry_decay)),
                directional_tau_smoothing_alpha=float(args.directional_tau_smoothing_alpha),
                directional_tau_min_ratio=float(args.directional_tau_min_ratio),
                directional_tau_max_ratio=float(args.directional_tau_max_ratio),
                directional_tau_default_up_mult=float(args.directional_tau_default_up_mult),
                directional_tau_default_down_mult=float(args.directional_tau_default_down_mult),
            )
            effect = float(exp_stats["mean_return"])
            pval = float(exp_stats["p_value"])
            n_joined = float(exp_stats["n_events"])
            n_effective = float(exp_stats.get("n_effective", n_joined))
            stability_pass = bool(exp_stats["stability_pass"])
            std_return = float(exp_stats.get("std_return", 0.0))
            mean_weight_age_days = float(exp_stats.get("mean_weight_age_days", 0.0))
            time_weight_sum = float(exp_stats.get("time_weight_sum", n_joined))
            mean_tau_days = float(exp_stats.get("mean_tau_days", tau_days))
            learning_rate_mean = float(exp_stats.get("learning_rate_mean", 0.0))
            mean_tau_up_days = float(exp_stats.get("mean_tau_up_days", 0.0))
            mean_tau_down_days = float(exp_stats.get("mean_tau_down_days", 0.0))
            tau_directional_ratio = float(exp_stats.get("tau_directional_ratio", 0.0))
            directional_up_share = float(exp_stats.get("directional_up_share", 0.0))
            
            # Helper to add results with Atlas lineage
            cost_estimate = cost_calibrator.estimate(symbol=symbol, events_df=bucket_events)
            candidate_cost_bps = float(cost_estimate.cost_bps)
            cost = candidate_cost_bps / 10000.0
            conservative_cost = cost * conservative_cost_multiplier
            after_cost = effect - cost
            after_cost_conservative = effect - conservative_cost
            econ_pass = after_cost >= min_after_cost
            econ_pass_conservative = after_cost_conservative >= min_after_cost
            fail_reasons = []
            if not econ_pass: fail_reasons.append("ECONOMIC_GATE")
            if not econ_pass_conservative: fail_reasons.append("ECONOMIC_CONSERVATIVE")
            if require_sign_stability and not stability_pass:
                fail_reasons.append("STABILITY_GATE")
            sample_size = _resolved_sample_size(n_joined, len(sym_events))
            sample_gate_pass = int(sample_size) >= int(min_sample_size_gate) if int(min_sample_size_gate) > 0 else True
            if not sample_gate_pass:
                fail_reasons.append("MIN_SAMPLE_SIZE_GATE")
            gate_phase2_research = econ_pass_conservative and sample_gate_pass and (
                stability_pass if require_sign_stability else True
            )
            gate_phase2_final = gate_phase2_research

            # Compute condition routing before building result
            _cond_str, _cond_source = _condition_routing(cond_label)
            _compile_eligible = _cond_source != "blocked"
            if not _compile_eligible:
                fail_reasons.append("NON_EXECUTABLE_CONDITION")

            _p2_quality_score = (float(econ_pass) + float(econ_pass_conservative) + float(stability_pass)) / 3.0
            _p2_quality_components = json.dumps({"econ": int(econ_pass), "econ_cons": int(econ_pass_conservative), "stability": int(stability_pass)}, sort_keys=True)
            _compile_eligible_fallback = _p2_quality_score >= quality_floor_fallback and int(n_joined) >= min_events_fallback
            _promotion_track = "standard" if gate_phase2_final else "fallback_only"
            state_token = state_id or "NO_STATE"
            results.append({
                "candidate_id": f"{event_type}_{rule}_{horizon}_{state_token}_{symbol}_{cond_label}",
                "family_id": _make_family_id(
                    symbol, event_type, rule, horizon, cond_label,
                    canonical_family=canonical_family,
                    state_id=state_id,
                ),
                "runtime_event_type": runtime_event_type,
                "canonical_event_type": canonical_event_type,
                "canonical_family": canonical_family,
                "hypothesis_id": hypothesis_id,
                "hypothesis_version": hypothesis_version,
                "hypothesis_spec_path": hypothesis_spec_path,
                "hypothesis_spec_hash": hypothesis_spec_hash,
                "hypothesis_metric": hypothesis_metric,
                "hypothesis_output_schema": hypothesis_output_schema,
                "hypothesis_candidate_id": hypothesis_candidate_id,
                "candidate_hash_inputs": candidate_hash_inputs,
                "event_type": event_type,
                "rule_template": rule,
                "template_id": template_id,
                "template_verb": rule,
                "operator_id": str(operator_def.get("operator_id", rule)).strip() or rule,
                "operator_version": operator_version,
                "horizon": horizon,
                "horizon_bars": horizon_bars,
                "entry_lag_bars": entry_lag_bars,
                "direction_rule": direction_rule,
                "symbol": symbol,
                "state_id": state_id,
                "state_provenance": state_provenance,
                "state_activation": state_activation,
                "state_activation_hash": state_activation_hash,
                "conditioning": cond_label,
                "condition_signature": condition_signature,
                "expectancy": effect,
                "after_cost_expectancy": after_cost,
                "after_cost_expectancy_per_trade": after_cost,
                "stressed_after_cost_expectancy_per_trade": after_cost_conservative,
                "turnover_proxy_mean": float(cost_estimate.turnover_proxy_mean),
                "avg_dynamic_cost_bps": float(cost_estimate.avg_dynamic_cost_bps),
                "cost_input_coverage": float(cost_estimate.cost_input_coverage),
                "cost_model_valid": bool(cost_estimate.cost_model_valid),
                "cost_model_source": str(cost_estimate.cost_model_source),
                "cost_regime_multiplier": float(cost_estimate.regime_multiplier),
                "cost_ratio": cost / max(abs(effect), 1e-9),
                "p_value": pval,
                "sample_size": _resolved_sample_size(n_joined, len(sym_events)),
                "n_events": int(n_joined),
                "effective_sample_size": n_effective,
                "time_weight_sum": time_weight_sum,
                "mean_weight_age_days": mean_weight_age_days,
                "time_decay_tau_days": mean_tau_days if bool(int(args.enable_time_decay)) else 0.0,
                "time_decay_learning_rate": learning_rate_mean if bool(int(args.enable_time_decay)) else 0.0,
                "time_decay_tau_up_days": mean_tau_up_days if bool(int(args.enable_time_decay)) else 0.0,
                "time_decay_tau_down_days": mean_tau_down_days if bool(int(args.enable_time_decay)) else 0.0,
                "time_decay_directional_ratio": tau_directional_ratio if bool(int(args.enable_time_decay)) else 0.0,
                "time_decay_directional_up_share": directional_up_share if bool(int(args.enable_time_decay)) else 0.0,
                "std_return": std_return,
                "sign": 1 if effect > 0 else -1,
                "gate_economic": econ_pass,
                "gate_economic_conservative": econ_pass_conservative,
                "gate_after_cost_positive": bool(econ_pass),
                "gate_after_cost_stressed_positive": bool(econ_pass_conservative),
                "gate_cost_model_valid": bool(cost_estimate.cost_model_valid),
                "gate_cost_ratio": bool((cost / max(abs(effect), 1e-9)) <= 1.0),
                "gate_stability": stability_pass,
                "gate_phase2_research": gate_phase2_research,
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
                "cost_bps_resolved": candidate_cost_bps,
                "fee_bps_resolved": float(cost_estimate.fee_bps_per_side),
                "slippage_bps_resolved": float(cost_estimate.slippage_bps_per_fill),
                # Atlas Lineage
                "plan_row_id": plan_row.get("plan_row_id", ""),
                "source_claim_ids": ",".join(plan_row.get("source_claim_ids", [])),
                "source_concept_ids": ",".join(plan_row.get("source_concept_ids", [])),
                "candidate_plan_hash": candidate_plan_hash,
                "effective_lag_bars": int(args.entry_lag_bars),
                "run_id": str(args.run_id),
            })
    else:
        # Default fallback discovery (original loop)
        # Pre-define allowed conditioning columns to avoid explosion.
        # Family-level override allows narrower discovery pools when needed.
        CONDITIONING_COLS = fam_config.get(
            "conditioning_cols",
            global_defaults.get("conditioning_cols", ["severity_bucket", "vol_regime"]),
        )

        for symbol in symbols:
            sym_all_events = events_df[events_df["symbol"] == symbol] if "symbol" in events_df.columns else events_df
            if sym_all_events.empty:
                log.info("No events available for %s/%s; skipping candidate expansion for this symbol.", args.event_type, symbol)
                continue
            features_df = _load_features(args.run_id, symbol)

            if features_df.empty:
                log.warning("No features found for %s / %s — skipping symbol.", args.event_type, symbol)
                continue

            for rule in templates:
                for horizon in horizons:
                    tau_days = _resolve_tau_days(canonical_family_for_event, args.time_decay_half_life_days)
                    tau_seconds = float(tau_days) * 86400.0
                    operator_def = _validate_operator_for_event(
                        template_verb=str(rule),
                        canonical_family=canonical_family_for_event,
                        operator_registry=operator_registry,
                    )
                    operator_version = str(operator_def.get("operator_version", operator_registry_version)).strip() or operator_registry_version
                    # 3a. Base candidate (all events for this symbol)
                    exp_stats = calculate_expectancy_stats(
                        sym_all_events, features_df, rule, horizon, canonical_family=canonical_family_for_event,
                        shift_labels_k=args.shift_labels_k,
                        entry_lag_bars=args.entry_lag_bars,
                        min_samples=args.min_samples,
                        time_decay_enabled=bool(int(args.enable_time_decay)),
                        time_decay_tau_seconds=tau_seconds,
                        time_decay_floor_weight=float(args.time_decay_floor_weight),
                        regime_conditioned_decay=bool(int(args.enable_regime_conditioned_decay)),
                        regime_tau_smoothing_alpha=float(args.regime_tau_smoothing_alpha),
                        regime_tau_min_days=float(args.regime_tau_min_days),
                        regime_tau_max_days=float(args.regime_tau_max_days),
                        directional_asymmetry_decay=bool(int(args.enable_directional_asymmetry_decay)),
                        directional_tau_smoothing_alpha=float(args.directional_tau_smoothing_alpha),
                        directional_tau_min_ratio=float(args.directional_tau_min_ratio),
                        directional_tau_max_ratio=float(args.directional_tau_max_ratio),
                        directional_tau_default_up_mult=float(args.directional_tau_default_up_mult),
                        directional_tau_default_down_mult=float(args.directional_tau_default_down_mult),
                    )
                    effect = float(exp_stats["mean_return"])
                    pval = float(exp_stats["p_value"])
                    n_joined = float(exp_stats["n_events"])
                    n_effective = float(exp_stats.get("n_effective", n_joined))
                    stability_pass = bool(exp_stats["stability_pass"])
                    std_return = float(exp_stats.get("std_return", 0.0))
                    mean_weight_age_days = float(exp_stats.get("mean_weight_age_days", 0.0))
                    time_weight_sum = float(exp_stats.get("time_weight_sum", n_joined))
                    mean_tau_days = float(exp_stats.get("mean_tau_days", tau_days))
                    learning_rate_mean = float(exp_stats.get("learning_rate_mean", 0.0))
                    mean_tau_up_days = float(exp_stats.get("mean_tau_up_days", 0.0))
                    mean_tau_down_days = float(exp_stats.get("mean_tau_down_days", 0.0))
                    tau_directional_ratio = float(exp_stats.get("tau_directional_ratio", 0.0))
                    directional_up_share = float(exp_stats.get("directional_up_share", 0.0))
                    
                    def _add_res(
                        eff,
                        pv,
                        n,
                        n_eff,
                        stab,
                        std_ret,
                        mean_age_days,
                        weight_sum,
                        tau_days_local,
                        learning_rate_local,
                        tau_up_days_local,
                        tau_down_days_local,
                        tau_directional_ratio_local,
                        directional_up_share_local,
                        bucket_events_for_cost,
                        cond_name="all",
                    ):
                        # Economic Gate with candidate-level cost calibration.
                        cost_estimate = cost_calibrator.estimate(symbol=symbol, events_df=bucket_events_for_cost)
                        candidate_cost_bps = float(cost_estimate.cost_bps)
                        cost = candidate_cost_bps / 10000.0
                        conservative_cost = cost * conservative_cost_multiplier

                        aft_cost = eff - cost
                        aft_cost_conservative = eff - conservative_cost

                        ec_pass = aft_cost >= min_after_cost
                        ec_pass_conservative = aft_cost_conservative >= min_after_cost

                        f_reasons = []
                        if not ec_pass:
                            f_reasons.append(f"ECONOMIC_GATE ({aft_cost:.6f} < {min_after_cost:.6f})")
                        if not ec_pass_conservative:
                            f_reasons.append("ECONOMIC_CONSERVATIVE")
                        if require_sign_stability and not stab:
                            f_reasons.append("STABILITY_GATE")
                        resolved_sample_size = _resolved_sample_size(n, len(sym_all_events))
                        sample_gate_pass = int(resolved_sample_size) >= int(min_sample_size_gate) if int(min_sample_size_gate) > 0 else True
                        if not sample_gate_pass:
                            f_reasons.append("MIN_SAMPLE_SIZE_GATE")

                        # Composite Phase 2 gate (economic + stability).
                        g_phase2_research = ec_pass_conservative and sample_gate_pass and (
                            stab if require_sign_stability else True
                        )
                        g_phase2_final = g_phase2_research

                        # Condition routing
                        _cond_str, _cond_source = _condition_routing(cond_name)
                        _compile_eligible = _cond_source != "blocked"
                        if not _compile_eligible:
                            f_reasons.append("NON_EXECUTABLE_CONDITION")

                        _p2_qs = (float(ec_pass) + float(ec_pass_conservative) + float(stab)) / 3.0
                        _p2_qc = json.dumps({"econ": int(ec_pass), "econ_cons": int(ec_pass_conservative), "stability": int(stab)}, sort_keys=True)
                        _fb_eligible = _p2_qs >= quality_floor_fallback and int(n) >= min_events_fallback
                        _p_track = "standard" if g_phase2_final else "fallback_only"
                        state_id = None
                        results.append({
                            "candidate_id": f"{args.event_type}_{rule}_{horizon}_NO_STATE_{symbol}_{cond_name}",
                            "family_id": _make_family_id(
                                symbol, args.event_type, rule, horizon, cond_name,
                                canonical_family=canonical_family_for_event,
                                state_id=state_id,
                            ),
                            "runtime_event_type": args.event_type,
                            "canonical_event_type": args.event_type,
                            "canonical_family": canonical_family_for_event,
                            "hypothesis_id": "",
                            "hypothesis_version": 0,
                            "hypothesis_spec_path": "",
                            "hypothesis_spec_hash": "",
                            "hypothesis_metric": "",
                            "hypothesis_output_schema": [],
                            "hypothesis_candidate_id": "",
                            "candidate_hash_inputs": "",
                            "event_type": args.event_type,
                            "rule_template": rule,
                            "template_id": rule,
                            "template_verb": rule,
                            "operator_id": str(operator_def.get("operator_id", rule)).strip() or rule,
                            "operator_version": operator_version,
                            "horizon": horizon,
                            "horizon_bars": _horizon_to_bars(horizon),
                            "entry_lag_bars": int(args.entry_lag_bars),
                            "direction_rule": str(operator_def.get("side_policy", "both")).strip() or "both",
                            "symbol": symbol,
                            "state_id": state_id,
                            "state_provenance": "none",
                            "state_activation": None,
                            "state_activation_hash": None,
                            "conditioning": cond_name,
                            "condition_signature": str(cond_name or "all"),
                            "expectancy": eff,
                            "after_cost_expectancy": aft_cost,
                            "after_cost_expectancy_per_trade": aft_cost,
                            "stressed_after_cost_expectancy_per_trade": aft_cost_conservative,
                            "turnover_proxy_mean": float(cost_estimate.turnover_proxy_mean),
                            "avg_dynamic_cost_bps": float(cost_estimate.avg_dynamic_cost_bps),
                            "cost_input_coverage": float(cost_estimate.cost_input_coverage),
                            "cost_model_valid": bool(cost_estimate.cost_model_valid),
                            "cost_model_source": str(cost_estimate.cost_model_source),
                            "cost_regime_multiplier": float(cost_estimate.regime_multiplier),
                            "cost_ratio": cost / max(abs(eff), 1e-9),
                            "p_value": pv,
                            "sample_size": _resolved_sample_size(n, len(sym_all_events)),
                            "n_events": int(n),
                            "effective_sample_size": float(n_eff),
                            "time_weight_sum": float(weight_sum),
                            "mean_weight_age_days": float(mean_age_days),
                            "time_decay_tau_days": float(tau_days_local) if bool(int(args.enable_time_decay)) else 0.0,
                            "time_decay_learning_rate": float(learning_rate_local) if bool(int(args.enable_time_decay)) else 0.0,
                            "time_decay_tau_up_days": float(tau_up_days_local) if bool(int(args.enable_time_decay)) else 0.0,
                            "time_decay_tau_down_days": float(tau_down_days_local) if bool(int(args.enable_time_decay)) else 0.0,
                            "time_decay_directional_ratio": float(tau_directional_ratio_local) if bool(int(args.enable_time_decay)) else 0.0,
                            "time_decay_directional_up_share": float(directional_up_share_local) if bool(int(args.enable_time_decay)) else 0.0,
                            "std_return": float(std_ret),
                            "sign": 1 if eff > 0 else -1,
                            "gate_economic": ec_pass,
                            "gate_economic_conservative": ec_pass_conservative,
                            "gate_after_cost_positive": bool(ec_pass),
                            "gate_after_cost_stressed_positive": bool(ec_pass_conservative),
                            "gate_cost_model_valid": bool(cost_estimate.cost_model_valid),
                            "gate_cost_ratio": bool((cost / max(abs(eff), 1e-9)) <= 1.0),
                            "gate_stability": stab,
                            "gate_phase2_research": g_phase2_research,
                            "gate_phase2_final": g_phase2_final,
                            "gate_a_ci_separated": False,
                            "gate_b_time_stable": stab,
                            "gate_b_year_signs": False,
                            "gate_c_regime_stable": False,
                            "gate_c_stable_splits": 0,
                            "gate_c_required_splits": 2,
                            "gate_d_friction_floor": bool(ec_pass),
                            "gate_e_simplicity": True,
                            "gate_f_exposure_guard": True,
                            "gate_g_net_benefit": bool(ec_pass),
                            "gate_h_executable_condition": _compile_eligible,
                            "gate_h_executable_action": True,
                            "gate_pass": g_phase2_final,
                            "gate_all_research": g_phase2_research,
                            "gate_all": g_phase2_final,
                            "gate_multiplicity": False,
                            "gate_multiplicity_strict": False,
                            "gate_ess": False,
                            "gate_delay_robustness": False,
                            "gate_oos_min_samples": False,
                            "gate_oos_validation": False,
                            "gate_oos_validation_test": False,
                            "gate_oos_consistency_strict": False,
                            "bridge_eval_status": "pending",
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
                            "cost_bps_resolved": candidate_cost_bps,
                            "fee_bps_resolved": float(cost_estimate.fee_bps_per_side),
                            "slippage_bps_resolved": float(cost_estimate.slippage_bps_per_fill),
                            "candidate_plan_hash": "",
                            "effective_lag_bars": int(args.entry_lag_bars),
                            "run_id": str(args.run_id),
                        })

                    _add_res(
                        effect,
                        pval,
                        n_joined,
                        n_effective,
                        stability_pass,
                        std_return,
                        mean_weight_age_days,
                        time_weight_sum,
                        mean_tau_days,
                        learning_rate_mean,
                        mean_tau_up_days,
                        mean_tau_down_days,
                        tau_directional_ratio,
                        directional_up_share,
                        sym_all_events,
                        "all",
                    )

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
                                
                            exp_stats_bucket = calculate_expectancy_stats(
                                bucket_events, features_df, rule, horizon, canonical_family=canonical_family_for_event,
                                shift_labels_k=args.shift_labels_k,
                                entry_lag_bars=args.entry_lag_bars,
                                min_samples=effective_min_samples,
                                time_decay_enabled=bool(int(args.enable_time_decay)),
                                time_decay_tau_seconds=tau_seconds,
                                time_decay_floor_weight=float(args.time_decay_floor_weight),
                                regime_conditioned_decay=bool(int(args.enable_regime_conditioned_decay)),
                                regime_tau_smoothing_alpha=float(args.regime_tau_smoothing_alpha),
                                regime_tau_min_days=float(args.regime_tau_min_days),
                                regime_tau_max_days=float(args.regime_tau_max_days),
                                directional_asymmetry_decay=bool(int(args.enable_directional_asymmetry_decay)),
                                directional_tau_smoothing_alpha=float(args.directional_tau_smoothing_alpha),
                                directional_tau_min_ratio=float(args.directional_tau_min_ratio),
                                directional_tau_max_ratio=float(args.directional_tau_max_ratio),
                                directional_tau_default_up_mult=float(args.directional_tau_default_up_mult),
                                directional_tau_default_down_mult=float(args.directional_tau_default_down_mult),
                            )
                            eff_b = float(exp_stats_bucket["mean_return"])
                            pv_b = float(exp_stats_bucket["p_value"])
                            n_b = float(exp_stats_bucket["n_events"])
                            n_eff_b = float(exp_stats_bucket.get("n_effective", n_b))
                            stab_b = bool(exp_stats_bucket["stability_pass"])
                            std_b = float(exp_stats_bucket.get("std_return", 0.0))
                            mean_age_b = float(exp_stats_bucket.get("mean_weight_age_days", 0.0))
                            weight_sum_b = float(exp_stats_bucket.get("time_weight_sum", n_b))
                            mean_tau_b = float(exp_stats_bucket.get("mean_tau_days", tau_days))
                            learning_rate_b = float(exp_stats_bucket.get("learning_rate_mean", 0.0))
                            mean_tau_up_b = float(exp_stats_bucket.get("mean_tau_up_days", 0.0))
                            mean_tau_down_b = float(exp_stats_bucket.get("mean_tau_down_days", 0.0))
                            tau_ratio_b = float(exp_stats_bucket.get("tau_directional_ratio", 0.0))
                            up_share_b = float(exp_stats_bucket.get("directional_up_share", 0.0))
                            _add_res(
                                eff_b,
                                pv_b,
                                n_b,
                                n_eff_b,
                                stab_b,
                                std_b,
                                mean_age_b,
                                weight_sum_b,
                                mean_tau_b,
                                learning_rate_b,
                                mean_tau_up_b,
                                mean_tau_down_b,
                                tau_ratio_b,
                                up_share_b,
                                bucket_events,
                                f"{col}_{val}",
                            )

    generation_diagnostics = {
        "run_id": str(args.run_id),
        "event_type": str(args.event_type),
        "mode": "atlas" if bool(plan_rows) else "fallback",
        "atlas_mode": bool(int(args.atlas_mode)),
        "candidate_plan_provided": bool(args.candidate_plan),
        "candidate_plan_rows_total": int(len(plan_rows)),
        "results_count": int(len(results)),
        "skip_reason_counts": {str(k): int(v) for k, v in skip_reason_counts.items()},
        "missing_condition_columns_in_events": sorted(missing_condition_columns),
    }

    if plan_rows:
        generation_diagnostics["candidate_plan_rows_matching_event"] = int(
            sum(
                1
                for row in plan_rows
                if str(row.get("event_type", "")).strip() == str(args.event_type).strip()
            )
        )

    if not results:
        log.warning("No results produced — check features/events availability for %s. Continuing.", args.event_type)
        # Emit empty artifacts to satisfy pipeline expectations
        ensure_dir(reports_root)
        with open(reports_root / "phase2_generation_diagnostics.json", "w", encoding="utf-8") as f:
            json.dump(generation_diagnostics, f, indent=2, sort_keys=True)
        pd.DataFrame(columns=PRIMARY_OUTPUT_COLUMNS).to_parquet(reports_root / "phase2_candidates_raw.parquet", index=False)
        pd.DataFrame(columns=PRIMARY_OUTPUT_COLUMNS).to_csv(reports_root / "phase2_candidates.csv", index=False)
        finalize_manifest(
            manifest,
            "success",
            stats={
                "total_tested": 0,
                "discoveries_statistical": 0,
                "survivors_phase2": 0,
                "no_results": True,
                "skip_reason_counts": generation_diagnostics["skip_reason_counts"],
                "ontology_spec_hash": run_manifest_ontology_hash or current_ontology_hash,
                "taxonomy_hash": current_ontology_components.get("taxonomy_hash"),
                "canonical_event_registry_hash": current_ontology_components.get("canonical_event_registry_hash"),
                "state_registry_hash": current_ontology_components.get("state_registry_hash"),
                "verb_lexicon_hash": current_ontology_components.get("verb_lexicon_hash"),
            },
        )
        return

    raw_df = pd.DataFrame(results)

    discovery_start_iso = ""
    discovery_end_iso = ""
    if not events_df.empty and "enter_ts" in events_df.columns and "split_label" in events_df.columns:
        discovery_mask = events_df["split_label"].isin(["train", "validation"])
        disc_events = events_df[discovery_mask]
        if not disc_events.empty:
            discovery_start_iso = str(disc_events["enter_ts"].min().isoformat())
            discovery_end_iso = str(disc_events["enter_ts"].max().isoformat())
    raw_df["discovery_start"] = discovery_start_iso
    raw_df["discovery_end"] = discovery_end_iso

    prev_lambda_maps, prev_lambda_source = _load_previous_lambda_maps(
        data_root=DATA_ROOT,
        event_type=str(args.event_type),
        current_run_id=str(args.run_id),
    )

    if bool(int(args.enable_hierarchical_shrinkage)):
        raw_df = _apply_hierarchical_shrinkage(
            raw_df,
            lambda_state=float(args.lambda_state),
            lambda_event=float(args.lambda_event),
            lambda_family=float(args.lambda_family),
            adaptive_lambda=bool(int(args.adaptive_shrinkage_lambda)),
            adaptive_lambda_min=float(args.adaptive_lambda_min),
            adaptive_lambda_max=float(args.adaptive_lambda_max),
            adaptive_lambda_eps=float(args.adaptive_lambda_eps),
            adaptive_lambda_min_total_samples=int(args.adaptive_lambda_min_total_samples),
            previous_lambda_maps=prev_lambda_maps,
            lambda_smoothing_alpha=float(args.lambda_smoothing_alpha),
            lambda_shock_cap_pct=float(args.lambda_shock_cap_pct),
        )
    else:
        raw_df["effect_raw"] = pd.to_numeric(raw_df.get("expectancy", 0.0), errors="coerce").fillna(0.0)
        raw_df["effect_shrunk_family"] = raw_df["effect_raw"]
        raw_df["effect_shrunk_event"] = raw_df["effect_raw"]
        raw_df["effect_shrunk_state"] = raw_df["effect_raw"]
        raw_df["shrinkage_weight_family"] = 1.0
        raw_df["shrinkage_weight_event"] = 1.0
        raw_df["shrinkage_weight_state"] = 1.0
        raw_df["p_value_raw"] = pd.to_numeric(raw_df.get("p_value", 1.0), errors="coerce").fillna(1.0).clip(0.0, 1.0)
        raw_df["p_value_shrunk"] = raw_df["p_value_raw"]
        raw_df["p_value_for_fdr"] = raw_df["p_value_raw"]
        raw_df["lambda_family"] = 0.0
        raw_df["lambda_event"] = 0.0
        raw_df["lambda_state"] = 0.0
        raw_df["lambda_family_status"] = "disabled"
        raw_df["lambda_event_status"] = "disabled"
        raw_df["lambda_state_status"] = "disabled"

    raw_df = _refresh_phase2_metrics_after_shrinkage(
        raw_df,
        min_after_cost=min_after_cost,
        conservative_cost_multiplier=conservative_cost_multiplier,
        min_sample_size_gate=min_sample_size_gate,
        require_sign_stability=require_sign_stability,
        quality_floor_fallback=quality_floor_fallback,
        min_events_fallback=min_events_fallback,
        min_information_weight_state=float(args.min_information_weight_state),
    )

    # 4. Multiplicity Control: family BH + global BH over family-adjusted q-values.
    fdr_df = _apply_multiplicity_controls(
        raw_df=raw_df,
        max_q=max_q,
        mode=str(args.mode),
        min_sample_size=min_sample_size_gate,
    )

    # After multiplicity control: refresh promotion_track and
    # compile_eligible_phase2_fallback from final discovery status.

    # Invariant: Phase 2 Final Pass REQUIRES discovery (multiplicity pass)
    if str(args.mode) == "research":
        if "gate_phase2_research" not in fdr_df.columns:
            fdr_df["gate_phase2_research"] = fdr_df["gate_phase2_final"].astype(bool)
        fdr_df["gate_phase2_final"] = fdr_df["gate_phase2_research"].astype(bool) & fdr_df["is_discovery"].astype(bool)
    else:
        # Preserve current strict behavior for production/certification.
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

    if "effect_raw" in fdr_df.columns and "effect_shrunk_state" in fdr_df.columns:
        fdr_df["shrinkage_adjustment_abs"] = (
            pd.to_numeric(fdr_df["effect_raw"], errors="coerce").fillna(0.0)
            - pd.to_numeric(fdr_df["effect_shrunk_state"], errors="coerce").fillna(0.0)
        ).abs()
    else:
        fdr_df["shrinkage_adjustment_abs"] = 0.0

    # Ensure lineage fields exist in fdr_df before writing
    lineage_cols = ["plan_row_id", "source_claim_ids", "source_concept_ids", "candidate_plan_hash"]
    for col in lineage_cols:
        if col not in fdr_df.columns:
            fdr_df[col] = ""

    # Populate explainability fields
    fdr_df = _populate_fail_reasons(fdr_df)
    
    # Write gate summary
    _write_gate_summary(fdr_df, reports_root / "gate_summary.json")

    # Canonical outputs
    fdr_df.to_parquet(reports_root / "phase2_candidates_raw.parquet", index=False)
    
    canonical_df = ensure_candidate_schema(fdr_df)
    canonical_df.to_parquet(reports_root / "phase2_candidates.parquet", index=False)
    canonical_df.to_csv(reports_root / "phase2_candidates.csv", index=False)

    fdr_df[["candidate_id", "family_id", "p_value", "sign"]].to_parquet(
        reports_root / "phase2_pvals.parquet", index=False
    )

    fdr_df[["candidate_id", "q_value", "is_discovery"]].to_parquet(
        reports_root / "phase2_fdr.parquet", index=False
    )

    diagnostics_cols = [
        "candidate_id",
        "symbol",
        "canonical_family",
        "canonical_event_type",
        "template_verb",
        "horizon",
        "state_id",
        "n_events",
        "effective_sample_size",
        "mean_weight_age_days",
        "time_decay_tau_days",
        "time_decay_learning_rate",
        "time_decay_tau_up_days",
        "time_decay_tau_down_days",
        "time_decay_directional_ratio",
        "time_decay_directional_up_share",
        "lambda_state",
        "lambda_state_status",
        "shrinkage_weight_state",
        "effect_raw",
        "effect_shrunk_state",
        "shrinkage_adjustment_abs",
    ]
    diag_df = fdr_df[[c for c in diagnostics_cols if c in fdr_df.columns]].copy()
    diag_df.to_parquet(reports_root / "phase2_shrinkage_diagnostics.parquet", index=False)
    diag_df.to_csv(reports_root / "phase2_shrinkage_diagnostics.csv", index=False)

    lambda_snapshot_df = _build_lambda_snapshot(fdr_df)
    lambda_snapshot_df.to_parquet(reports_root / "phase2_lambda_snapshot.parquet", index=False)
    lambda_snapshot_df.to_csv(reports_root / "phase2_lambda_snapshot.csv", index=False)

    report = {
        "spec_hashes": spec_hashes,
        "inputs": {
            "candidate_plan_hash": candidate_plan_hash,
            "ontology_spec_hash": run_manifest_ontology_hash or current_ontology_hash,
        },
        "ontology": {
            "allow_hash_mismatch": bool(allow_hash_mismatch),
            "run_manifest_ontology_spec_hash": run_manifest_ontology_hash,
            "current_ontology_spec_hash": current_ontology_hash,
            "operator_registry_version": operator_registry_version,
            "previous_lambda_source": str(prev_lambda_source) if prev_lambda_source else "",
            **current_ontology_components,
        },
        "family_definition": "Option B (symbol, event_type, rule_template, horizon) — F-3 fix",
        "cost_coordinate": cost_coordinate,
        "thresholds": {
            "gate_profile": str(gates.get("_resolved_profile", str(args.gate_profile))),
            "max_q_value": max_q,
            "min_after_cost_expectancy_bps": float(gate_cfg["min_after_cost_expectancy_bps"]),
            "conservative_cost_multiplier": conservative_cost_multiplier,
            "require_sign_stability": require_sign_stability,
            "entry_lag_bars": int(args.entry_lag_bars),
            "enable_hierarchical_shrinkage": bool(int(args.enable_hierarchical_shrinkage)),
            "adaptive_shrinkage_lambda": bool(int(args.adaptive_shrinkage_lambda)),
            "lambda_state": float(args.lambda_state),
            "lambda_event": float(args.lambda_event),
            "lambda_family": float(args.lambda_family),
            "adaptive_lambda_min": float(args.adaptive_lambda_min),
            "adaptive_lambda_max": float(args.adaptive_lambda_max),
            "adaptive_lambda_eps": float(args.adaptive_lambda_eps),
            "adaptive_lambda_min_total_samples": int(args.adaptive_lambda_min_total_samples),
            "enable_time_decay": bool(int(args.enable_time_decay)),
            "time_decay_half_life_days": float(args.time_decay_half_life_days) if args.time_decay_half_life_days else None,
            "time_decay_floor_weight": float(args.time_decay_floor_weight),
            "enable_regime_conditioned_decay": bool(int(args.enable_regime_conditioned_decay)),
            "regime_tau_smoothing_alpha": float(args.regime_tau_smoothing_alpha),
            "regime_tau_min_days": float(args.regime_tau_min_days),
            "regime_tau_max_days": float(args.regime_tau_max_days),
            "enable_directional_asymmetry_decay": bool(int(args.enable_directional_asymmetry_decay)),
            "directional_tau_smoothing_alpha": float(args.directional_tau_smoothing_alpha),
            "directional_tau_min_ratio": float(args.directional_tau_min_ratio),
            "directional_tau_max_ratio": float(args.directional_tau_max_ratio),
            "directional_tau_default_up_mult": float(args.directional_tau_default_up_mult),
            "directional_tau_default_down_mult": float(args.directional_tau_default_down_mult),
            "lambda_smoothing_alpha": float(args.lambda_smoothing_alpha),
            "lambda_shock_cap_pct": float(args.lambda_shock_cap_pct),
            "min_information_weight_state": float(args.min_information_weight_state),
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
            "mean_abs_shrinkage_adjustment": float(pd.to_numeric(fdr_df.get("shrinkage_adjustment_abs", 0.0), errors="coerce").fillna(0.0).mean()),
            "median_abs_shrinkage_adjustment": float(pd.to_numeric(fdr_df.get("shrinkage_adjustment_abs", 0.0), errors="coerce").fillna(0.0).median()),
            "mean_shrinkage_weight_state": float(pd.to_numeric(fdr_df.get("shrinkage_weight_state", 0.0), errors="coerce").fillna(0.0).mean()),
            "mean_lambda_state": float(pd.to_numeric(fdr_df.get("lambda_state", 0.0), errors="coerce").fillna(0.0).mean()),
            "median_lambda_state": float(pd.to_numeric(fdr_df.get("lambda_state", 0.0), errors="coerce").fillna(0.0).median()),
            "lambda_state_status_counts": {str(k): int(v) for k, v in fdr_df.get("lambda_state_status", pd.Series(dtype="object")).fillna("").value_counts().to_dict().items()},
            "mean_effective_sample_size": float(pd.to_numeric(fdr_df.get("effective_sample_size", 0.0), errors="coerce").fillna(0.0).mean()),
            "mean_weight_age_days": float(pd.to_numeric(fdr_df.get("mean_weight_age_days", 0.0), errors="coerce").fillna(0.0).mean()),
            "mean_time_decay_tau_days": float(pd.to_numeric(fdr_df.get("time_decay_tau_days", 0.0), errors="coerce").fillna(0.0).mean()),
            "mean_time_decay_learning_rate": float(pd.to_numeric(fdr_df.get("time_decay_learning_rate", 0.0), errors="coerce").fillna(0.0).mean()),
            "mean_time_decay_tau_up_days": float(pd.to_numeric(fdr_df.get("time_decay_tau_up_days", 0.0), errors="coerce").fillna(0.0).mean()),
            "mean_time_decay_tau_down_days": float(pd.to_numeric(fdr_df.get("time_decay_tau_down_days", 0.0), errors="coerce").fillna(0.0).mean()),
            "mean_time_decay_directional_ratio": float(pd.to_numeric(fdr_df.get("time_decay_directional_ratio", 0.0), errors="coerce").fillna(0.0).mean()),
            "mean_time_decay_directional_up_share": float(pd.to_numeric(fdr_df.get("time_decay_directional_up_share", 0.0), errors="coerce").fillna(0.0).mean()),
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
    with open(reports_root / "phase2_generation_diagnostics.json", "w", encoding="utf-8") as f:
        json.dump(generation_diagnostics, f, indent=2, sort_keys=True)

    finalize_manifest(
        manifest,
        "success",
        stats={
            "total_tested": int(len(fdr_df)),
            "discoveries_statistical": int(fdr_df["is_discovery"].sum()),
            "survivors_phase2": int(fdr_df["gate_phase2_final"].sum()),
            "ontology_spec_hash": run_manifest_ontology_hash or current_ontology_hash,
            "taxonomy_hash": current_ontology_components.get("taxonomy_hash"),
            "canonical_event_registry_hash": current_ontology_components.get("canonical_event_registry_hash"),
            "state_registry_hash": current_ontology_components.get("state_registry_hash"),
            "verb_lexicon_hash": current_ontology_components.get("verb_lexicon_hash"),
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
