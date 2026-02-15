from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class AllocationConfig:
    max_weight: float = 0.45
    min_weight: float = 0.02
    volatility_target: float = 0.0
    volatility_adjustment_cap: float = 3.0
    raw_ratio_cap: float = 1_000.0
    variance_floor: float = 1e-9


def _apply_max_cap(weights: pd.Series, max_weight: float) -> pd.Series:
    capped = weights.copy()
    max_weight = min(max(max_weight, 0.0), 1.0)
    if capped.empty or max_weight <= 0.0:
        return pd.Series(0.0, index=capped.index, dtype=float)

    for _ in range(16):
        over = capped > max_weight
        if not over.any():
            break
        excess = float((capped[over] - max_weight).sum())
        capped.loc[over] = max_weight
        if excess <= 0:
            break
        under = capped < max_weight
        if not under.any():
            break
        base = capped[under]
        base_sum = float(base.sum())
        if base_sum <= 0.0:
            capped.loc[under] += excess / float(len(base))
        else:
            capped.loc[under] += excess * (base / base_sum)

    total = float(capped.sum())
    if total > 0:
        capped = capped / total
    return capped


def _apply_min_floor(weights: pd.Series, min_weight: float) -> pd.Series:
    floored = weights.copy()
    active = floored[floored > 0.0]
    if active.empty:
        return floored

    floor_target = min(max(min_weight, 0.0), 1.0 / float(len(active)))
    below = floored[(floored > 0.0) & (floored < floor_target)]
    if below.empty or floor_target <= 0.0:
        return floored

    required = float((floor_target - below).sum())
    donors = floored[floored > floor_target]
    donor_surplus = donors - floor_target
    donor_surplus_sum = float(donor_surplus.sum())
    if donor_surplus_sum <= 0.0 or required > donor_surplus_sum + 1e-12:
        return pd.Series(1.0 / float(len(active)), index=floored.index, dtype=float).where(floored > 0.0, 0.0)

    floored.loc[below.index] = floor_target
    floored.loc[donors.index] -= required * (donor_surplus / donor_surplus_sum)
    total = float(floored.sum())
    if total > 0:
        floored = floored / total
    return floored


def build_allocation_weights(candidates: pd.DataFrame, config: AllocationConfig) -> pd.DataFrame:
    if candidates.empty:
        return pd.DataFrame()

    promoted = candidates.copy()
    promoted["status"] = promoted["status"].astype(str).str.upper()
    promoted = promoted[promoted["status"] == "PROMOTED"].copy()
    promoted["candidate_symbol"] = promoted["candidate_symbol"].astype(str).str.upper()
    promoted = promoted[promoted["candidate_symbol"].ne("ALL")].copy()
    if promoted.empty:
        return pd.DataFrame()

    promoted["expectancy_per_trade"] = pd.to_numeric(promoted.get("expectancy_per_trade"), errors="coerce").fillna(0.0)
    promoted["variance"] = pd.to_numeric(promoted.get("variance"), errors="coerce").fillna(0.0)
    promoted["n_events"] = pd.to_numeric(promoted.get("n_events"), errors="coerce").fillna(0.0)

    rows = []
    for symbol, group in promoted.groupby("candidate_symbol", sort=True):
        weight_sum = float(group["n_events"].clip(lower=0.0).sum())
        if weight_sum <= 0:
            weight_sum = float(len(group))
            event_weights = np.ones(len(group), dtype=float)
        else:
            event_weights = group["n_events"].clip(lower=0.0).to_numpy(dtype=float)

        ev = float(np.average(group["expectancy_per_trade"].to_numpy(dtype=float), weights=event_weights))
        variance = float(np.average(group["variance"].to_numpy(dtype=float), weights=event_weights))
        variance = max(variance, config.variance_floor)
        raw_weight = np.clip(ev / variance, 0.0, config.raw_ratio_cap)
        symbol_vol = float(np.sqrt(variance))
        vol_adjustment = 1.0
        if config.volatility_target > 0.0 and symbol_vol > 0.0:
            vol_adjustment = min(config.volatility_adjustment_cap, config.volatility_target / symbol_vol)
        rows.append(
            {
                "symbol": symbol,
                "ev": ev,
                "variance": variance,
                "symbol_vol": symbol_vol,
                "raw_weight": float(raw_weight),
                "vol_adjustment": float(vol_adjustment),
                "adjusted_raw_weight": float(raw_weight * vol_adjustment),
                "promoted_candidates": int(len(group)),
                "n_events": int(group["n_events"].sum()),
            }
        )

    alloc = pd.DataFrame(rows).sort_values("adjusted_raw_weight", ascending=False).reset_index(drop=True)
    total_adjusted = float(alloc["adjusted_raw_weight"].sum())
    if total_adjusted <= 0.0:
        alloc["normalized_weight"] = 1.0 / float(len(alloc))
    else:
        alloc["normalized_weight"] = alloc["adjusted_raw_weight"] / total_adjusted

    capped = _apply_max_cap(alloc["normalized_weight"], config.max_weight)
    floored = _apply_min_floor(capped, config.min_weight)
    alloc["final_weight"] = floored
    alloc["allocation_rank"] = alloc["final_weight"].rank(ascending=False, method="dense").astype(int)
    return alloc.sort_values(["final_weight", "symbol"], ascending=[False, True]).reset_index(drop=True)


def allocation_metadata(config: AllocationConfig, allocation_df: pd.DataFrame) -> Dict[str, object]:
    return {
        "max_weight": config.max_weight,
        "min_weight": config.min_weight,
        "volatility_target": config.volatility_target,
        "volatility_adjustment_cap": config.volatility_adjustment_cap,
        "raw_ratio_cap": config.raw_ratio_cap,
        "variance_floor": config.variance_floor,
        "symbol_count": int(len(allocation_df)),
        "final_weight_sum": float(allocation_df["final_weight"].sum()) if not allocation_df.empty else 0.0,
    }

