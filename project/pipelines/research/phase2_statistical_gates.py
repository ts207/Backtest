from __future__ import annotations

import json
import re
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from pipelines.research.analyze_conditional_expectancy import _two_sided_p_from_t
from pipelines.research.phase2_event_analyzer import ActionSpec, expectancy_for_action

NUMERIC_CONDITION_PATTERN = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|==|>|<)\s*(-?\d+(?:\.\d+)?)\s*$")


def safe_float(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    if not np.isfinite(out):
        return float(default)
    return out


def parse_numeric_condition_expr(condition: str) -> Tuple[str, str, float] | None:
    match = NUMERIC_CONDITION_PATTERN.match(str(condition or "").strip())
    if not match:
        return None
    feature, operator, raw_value = match.groups()
    try:
        value = float(raw_value)
    except ValueError:
        return None
    return feature, operator, value


def condition_mask_for_numeric_expr(frame: pd.DataFrame, feature: str, operator: str, threshold: float) -> pd.Series:
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


def curvature_metrics(
    *,
    all_events: pd.DataFrame,
    condition_name: str,
    sub: pd.DataFrame,
    action: ActionSpec,
    parameter_curvature_max_penalty: float,
) -> Dict[str, object]:
    parsed = parse_numeric_condition_expr(condition_name)
    center = expectancy_for_action(sub, action)
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
    left_mask = condition_mask_for_numeric_expr(all_events, feature, operator, float(threshold - delta))
    right_mask = condition_mask_for_numeric_expr(all_events, feature, operator, float(threshold + delta))
    left = expectancy_for_action(all_events[left_mask.fillna(False)].copy(), action)
    right = expectancy_for_action(all_events[right_mask.fillna(False)].copy(), action)

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


def delay_robustness_fields(
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


def gate_year_stability(sub: pd.DataFrame, effect_col: str, min_ratio: float = 0.8) -> Tuple[bool, str]:
    if "year" not in sub.columns or sub.empty:
        return False, "insufficient_years"
    signs = []
    for _, group in sub.groupby("year", sort=True):
        x = group[effect_col].mean()
        signs.append(1 if x > 0 else -1 if x < 0 else 0)
    non_zero = [s for s in signs if s != 0]
    if not non_zero:
        return False, "all_zero"
    improvement_ratio = non_zero.count(-1) / len(non_zero)
    catastrophic_reversal = (non_zero.count(1) > 0) and (improvement_ratio < min_ratio)
    return bool(improvement_ratio >= min_ratio and not catastrophic_reversal), ",".join(str(s) for s in signs)


def gate_regime_stability(
    sub: pd.DataFrame,
    effect_col: str,
    condition_name: str,
    min_stable_splits: int = 2,
) -> Tuple[bool, int, int]:
    checks: List[pd.Series] = []
    if not condition_name.startswith("symbol_") and "symbol" in sub.columns:
        checks.append(sub.groupby("symbol")[effect_col].mean())
    if not condition_name.startswith("vol_regime_") and "vol_regime" in sub.columns:
        checks.append(sub.groupby("vol_regime")[effect_col].mean())
    if not condition_name.startswith("bull_bear_") and "bull_bear" in sub.columns:
        checks.append(sub.groupby("bull_bear")[effect_col].mean())

    stable_splits = 0
    for series in checks:
        nz = [v for v in series.tolist() if abs(v) > 1e-12]
        if not nz:
            continue
        if not any(v > 0 for v in nz):
            stable_splits += 1

    if not checks:
        return False, 0, 0

    required_splits = min(max(1, int(min_stable_splits)), len(checks))
    return stable_splits >= required_splits, stable_splits, required_splits


def split_count(sub: pd.DataFrame, label: str) -> int:
    if "split_label" not in sub.columns:
        return 0
    return int((sub["split_label"] == label).sum())


def split_mean(sub: pd.DataFrame, split_label: str, col: str) -> float:
    if "split_label" not in sub.columns or col not in sub.columns:
        return np.nan
    frame = sub[sub["split_label"] == split_label]
    if frame.empty:
        return np.nan
    values = pd.to_numeric(frame[col], errors="coerce").dropna()
    return float(values.mean()) if not values.empty else np.nan


def split_t_stat_and_p_value(sub: pd.DataFrame, split_label: str, col: str) -> Tuple[float, float]:
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


def capacity_proxy_from_frame(sub: pd.DataFrame) -> float:
    for col in ["quote_volume", "volume", "notional", "turnover", "liquidation_notional"]:
        if col not in sub.columns:
            continue
        vals = pd.to_numeric(sub[col], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        vals = vals[vals > 0]
        if vals.empty:
            continue
        return float(np.log1p(float(vals.median())))
    return np.nan


def effective_sample_size(values: np.ndarray, max_lag: int) -> Tuple[float, int]:
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


def multiplicity_penalty(*, multiplicity_k: float, num_tests_event_family: int, ess_effective: float) -> float:
    tests = max(2.0, float(num_tests_event_family))
    eff_n = max(1.0, float(ess_effective))
    return float(float(multiplicity_k) * np.sqrt(np.log(tests) / eff_n))


def apply_multiplicity_adjustments(
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
        lambda row: multiplicity_penalty(
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
            str(key): float(safe_float(val, 0.0) - safe_float(row.get("multiplicity_penalty"), 0.0))
            for key, val in raw_map.items()
        }
        fields = delay_robustness_fields(
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
