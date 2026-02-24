from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd

from engine.execution_model import estimate_transaction_cost_bps
from pipelines.research.phase2_event_analyzer import ActionSpec


COST_INPUT_COVERAGE_MIN = 0.80


def turnover_proxy_for_action(action: ActionSpec, n: int) -> np.ndarray:
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


def candidate_cost_fields(
    *,
    sub: pd.DataFrame,
    action: ActionSpec,
    expectancy_per_trade: float,
    execution_cost_config: Dict[str, float],
    stressed_cost_multiplier: float,
) -> Dict[str, float | bool]:
    n = int(len(sub))
    turnover = turnover_proxy_for_action(action=action, n=n)
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
    cost_per_trade = float(np.nanmean((cost_values * turnover) / 10_000.0))
    cost_per_trade = max(0.0, cost_per_trade)
    after_cost = float(expectancy_per_trade - cost_per_trade)
    stressed_after_cost = float(expectancy_per_trade - (float(stressed_cost_multiplier) * cost_per_trade))
    gross_proxy = max(1e-9, abs(float(expectancy_per_trade)) + cost_per_trade)
    cost_ratio = float(min(2.0, max(0.0, cost_per_trade / gross_proxy)))
    return {
        "after_cost_expectancy_per_trade": after_cost,
        "stressed_after_cost_expectancy_per_trade": stressed_after_cost,
        "turnover_proxy_mean": float(np.nanmean(turnover)),
        "avg_dynamic_cost_bps": float(np.nanmean(cost_values)),
        "cost_input_coverage": cost_input_coverage,
        "cost_model_valid": cost_model_valid,
        "cost_ratio": cost_ratio,
        "gate_after_cost_positive": bool(after_cost > 0.0),
        "gate_after_cost_stressed_positive": bool(stressed_after_cost > 0.0),
        "gate_cost_model_valid": cost_model_valid,
        "gate_cost_ratio": bool(cost_ratio < 0.60),
    }
