from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import pandas as pd

from pipelines._lib.validation import ensure_utc_timestamp


@dataclass
class TsmomV1:
    name: str = "tsmom_v1"
    required_features: List[str] = None

    def __post_init__(self) -> None:
        if self.required_features is None:
            self.required_features = ["close"]

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

        fast_n = int(local_params.get("fast_n", 16))
        slow_n = int(local_params.get("slow_n", 96))
        band_bps = float(local_params.get("band_bps", 10.0))
        if fast_n <= 0 or slow_n <= 0:
            raise ValueError("fast_n and slow_n must be positive")
        if fast_n >= slow_n:
            raise ValueError("fast_n must be less than slow_n")

        bars = bars.copy()
        features = features.copy()
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
        features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)
        ensure_utc_timestamp(bars["timestamp"], "timestamp")
        ensure_utc_timestamp(features["timestamp"], "timestamp")

        merged = bars[["timestamp", "close", "is_gap", "gap_len"]].merge(
            features[["timestamp"]],
            on="timestamp",
            how="left",
        )
        merged = merged.sort_values("timestamp").reset_index(drop=True)
        merged["is_gap"] = merged["is_gap"].fillna(False)
        merged["gap_len"] = merged["gap_len"].fillna(0)

        px = merged["close"].astype(float)
        ema_fast = px.ewm(span=fast_n, adjust=False, min_periods=fast_n).mean()
        ema_slow = px.ewm(span=slow_n, adjust=False, min_periods=slow_n).mean()
        rel_spread = (ema_fast - ema_slow) / px.replace(0.0, pd.NA)
        band = band_bps / 10000.0

        pos = pd.Series(0, index=merged.index, dtype=int)
        valid = rel_spread.notna() & (~merged["is_gap"].astype(bool)) & (merged["gap_len"].astype(int) <= 0)
        pos.loc[valid & (rel_spread > band)] = 1
        pos.loc[valid & (rel_spread < -band)] = -1

        return pd.Series(pos.values, index=merged["timestamp"], name="position").astype(int)
