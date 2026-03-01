"""
Hierarchical James-Stein shrinkage and time-decay weighting.

Extracted from phase2_candidate_discovery.py — pure functions, no side effects.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


log = logging.getLogger(__name__)


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

    # 3. State layer (cross-symbol)
    out["shrinkage_weight_state_group"] = np.where(
        out["lambda_state_status"] == "insufficient_data",
        1.0,
        out["n_state"] / (out["n_state"] + pd.to_numeric(out["lambda_state"], errors="coerce").fillna(float(lambda_state))),
    )
    out["shrinkage_weight_state_group"] = out["shrinkage_weight_state_group"].replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(0.0, 1.0)
    
    out["effect_shrunk_state_group"] = np.where(
        state_mask,
        out["shrinkage_weight_state_group"] * out["mean_state"] + (1.0 - out["shrinkage_weight_state_group"]) * out["effect_shrunk_event"],
        out["effect_shrunk_event"]
    )

    # 4. Symbol layer (the candidate)
    symbol_cols = state_cols + ["_symbol"]
    symbol_stats = out[symbol_cols + ["_n", "effect_raw", "_var"]].copy()
    symbol_stats = symbol_stats.rename(columns={"_n": "n_symbol", "effect_raw": "mean_symbol", "_var": "var_symbol"})
    
    lambda_symbol_df = _estimate_adaptive_lambda(
        symbol_stats,
        parent_cols=state_cols,
        child_col="_symbol",
        n_col="n_symbol",
        mean_col="mean_symbol",
        var_col="var_symbol",
        lambda_name="lambda_symbol",
        fixed_lambda=float(lambda_state),
        adaptive=bool(adaptive_lambda),
        lambda_min=float(adaptive_lambda_min),
        lambda_max=float(adaptive_lambda_max),
        eps=float(adaptive_lambda_eps),
        min_total_samples=int(adaptive_lambda_min_total_samples),
        previous_lambda_by_parent=None,
        lambda_smoothing_alpha=float(lambda_smoothing_alpha),
        lambda_shock_cap_pct=float(lambda_shock_cap_pct),
    )
    
    out = out.merge(lambda_symbol_df[state_cols + ["lambda_symbol", "lambda_symbol_status"]], on=state_cols, how="left")
    
    # If a state has only one symbol (or insufficient data to adaptively estimate symbol-level variance),
    # fall back to the fixed user-provided lambda_state to ensure small-N heavily pools toward the state.
    out["lambda_symbol_eff"] = np.where(
        out["lambda_symbol_status"].isin(["insufficient_data", "single_child"]),
        float(lambda_state),
        pd.to_numeric(out["lambda_symbol"], errors="coerce").fillna(float(lambda_state))
    )
    
    out["shrinkage_weight_state"] = out["_n"] / (out["_n"] + out["lambda_symbol_eff"])
    out["shrinkage_weight_state"] = out["shrinkage_weight_state"].replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(0.0, 1.0)

    # The candidate is shrunk towards the cross-symbol group mean!
    out["effect_shrunk_state"] = (
        out["shrinkage_weight_state"] * out["effect_raw"] 
        + (1.0 - out["shrinkage_weight_state"]) * out["effect_shrunk_state_group"]
    )

    # Build shrunken p-values from state-shrunk effect and raw standard error.
    se = out["std_return"] / np.sqrt(np.maximum(out["_n"], 1.0))
    valid_se = np.isfinite(se) & (se > 0.0) & (out["_n"] > 1.0)
    p_shrunk = out["p_value_raw"].astype(float).copy()
    t_shrunk = pd.Series(0.0, index=out.index, dtype=float)
    t_shrunk.loc[valid_se] = out.loc[valid_se, "effect_shrunk_state"] / se.loc[valid_se]
    # Vectorized: use scipy.stats.t.sf directly instead of row-by-row .apply
    from scipy.stats import t as _scipy_t
    _df_vals = (out.loc[valid_se, "_n"] - 1.0).clip(lower=1.0)
    p_shrunk.loc[valid_se] = (2.0 * _scipy_t.sf(t_shrunk.loc[valid_se].abs(), df=_df_vals)).clip(0.0, 1.0)
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
    # Vectorized: build JSON string via string concat (avoids row-by-row apply)
    out["phase2_quality_components"] = (
        '{"econ":' + out["gate_economic"].astype(int).astype(str)
        + ',"econ_cons":' + out["gate_economic_conservative"].astype(int).astype(str)
        + ',"stability":' + out["gate_stability"].astype(int).astype(str)
        + ',"state_info":' + out["gate_state_information"].astype(int).astype(str) + '}'
    )
    out["compile_eligible_phase2_fallback"] = (
        (out["phase2_quality_score"] >= float(quality_floor_fallback))
        & (out["n_events"] >= int(min_events_fallback))
    )

    # Vectorized fail_reasons: strip managed gate tokens, then OR-in new failures.
    # Strip known managed prefixes from any pre-existing fail_reasons strings.
    _prior = out.get("fail_reasons", pd.Series("", index=out.index)).fillna("").astype(str)

    def _strip_managed(s: str) -> str:
        kept = [t.strip() for t in s.split(",") if t.strip() and not t.strip().startswith(known_gate_prefixes)]
        return ",".join(dict.fromkeys(kept))

    _prior_stripped = _prior.map(_strip_managed)

    # Build one boolean column per managed gate failure
    _gate_flags: dict[str, pd.Series] = {
        "ECONOMIC_GATE": ~out["gate_economic"].astype(bool),
        "ECONOMIC_CONSERVATIVE": ~out["gate_economic_conservative"].astype(bool),
        "MIN_SAMPLE_SIZE_GATE": (out["sample_size"] < int(min_sample_size_gate)) if int(min_sample_size_gate) > 0 else pd.Series(False, index=out.index),
        "STATE_INFORMATION_WEIGHT": ~out["gate_state_information"].astype(bool),
    }
    if require_sign_stability:
        _gate_flags["STABILITY_GATE"] = ~out["gate_stability"].astype(bool)

    _gate_df = pd.DataFrame(_gate_flags, index=out.index)
    _new_reasons = _gate_df.apply(
        lambda row: ",".join(col for col in _gate_df.columns if row[col]), axis=1
    )
    # Merge prior (non-managed) tokens with new gate tokens
    out["fail_reasons"] = (
        _prior_stripped.str.cat(_new_reasons.str.strip(","), sep=",", na_rep="")
        .str.strip(",")
        .str.replace(r",+", ",", regex=True)
    )
    out["promotion_track"] = np.where(out["gate_phase2_final"], "standard", "fallback_only")
    return out
