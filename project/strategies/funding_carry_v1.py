from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import pandas as pd

from pipelines._lib.validation import ensure_utc_timestamp


@dataclass
class FundingCarryV1:
    name: str = "funding_carry_v1"
    required_features: List[str] = None

    def __post_init__(self) -> None:
        if self.required_features is None:
            self.required_features = ["funding_rate_scaled"]

    def generate_positions(
        self,
        bars: pd.DataFrame,
        features: pd.DataFrame,
        params: Dict[str, object],
    ) -> pd.Series:
        strategy_overrides = params.get("strategy_overrides", {})
        strategy_params = strategy_overrides.get(self.name, {}) if isinstance(strategy_overrides, dict) else {}
        if not isinstance(strategy_params, dict):
            strategy_params = {}

        local_params = dict(params)
        local_params.update(strategy_params)

        n = int(local_params.get("n", 192))
        thr = float(local_params.get("thr", 0.00005))
        gap_bars = int(local_params.get("gap_bars", 0))
        strict_missing_window = bool(local_params.get("strict_missing_window", True))
        if n <= 0:
            raise ValueError("n must be positive")
        if thr < 0:
            raise ValueError("thr must be non-negative")

        bars = bars.copy()
        features = features.copy()
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
        features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)
        ensure_utc_timestamp(bars["timestamp"], "timestamp")
        ensure_utc_timestamp(features["timestamp"], "timestamp")

        merged = bars[["timestamp", "is_gap", "gap_len"]].merge(
            features[["timestamp", "funding_rate_scaled"]],
            on="timestamp",
            how="left",
        )
        merged = merged.sort_values("timestamp").reset_index(drop=True)
        merged["is_gap"] = merged["is_gap"].fillna(False)
        merged["gap_len"] = merged["gap_len"].fillna(0)

        funding = merged["funding_rate_scaled"].astype(float)
        fp = funding.ewm(span=n, adjust=False, min_periods=n).mean()

        if strict_missing_window:
            miss = funding.isna().astype(int).rolling(window=n, min_periods=1).sum()
            funding_ok = miss == 0
        else:
            funding_ok = funding.notna()

        gap_ok = (~merged["is_gap"].astype(bool)) & (merged["gap_len"].astype(int) <= gap_bars)
        valid = funding_ok & gap_ok & fp.notna()

        pos = pd.Series(0, index=merged.index, dtype=int)
        pos.loc[valid & (fp >= thr)] = -1
        pos.loc[valid & (fp <= -thr)] = 1

        return pd.Series(pos.values, index=merged["timestamp"], name="position").astype(int)
