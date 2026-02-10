from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import pandas as pd

from pipelines._lib.validation import ensure_utc_timestamp


@dataclass
class VolCompressionMomentumV1:
    name: str = "vol_compression_momentum_v1"
    required_features: List[str] = None

    def __post_init__(self) -> None:
        if self.required_features is None:
            self.required_features = [
                "rv_pct_2880",
                "range_96",
                "range_med_480",
                "high_96",
                "low_96",
            ]

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

        entry_rv_pct_max = float(local_params.get("entry_rv_pct_max", 12.0))
        exit_rv_pct_min = float(local_params.get("exit_rv_pct_min", 35.0))
        max_hold_bars = int(local_params.get("max_hold_bars", 64))

        bars = bars.copy()
        features = features.copy()
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
        features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)
        ensure_utc_timestamp(bars["timestamp"], "timestamp")
        ensure_utc_timestamp(features["timestamp"], "timestamp")

        merged = features.merge(
            bars[["timestamp", "high", "low", "close", "is_gap", "gap_len"]],
            on="timestamp",
            how="left",
            suffixes=("", "_bar"),
        ).sort_values("timestamp")
        merged["is_gap"] = merged["is_gap"].fillna(False)
        merged["gap_len"] = merged["gap_len"].fillna(0)
        merged["prior_high_96"] = merged["high_96"].shift(1)
        merged["prior_low_96"] = merged["low_96"].shift(1)

        required = ["rv_pct_2880", "range_96", "range_med_480", "close", "prior_high_96", "prior_low_96"]

        positions: List[int] = []
        current = 0
        entry_idx = -1

        for idx, row in merged.iterrows():
            if row[required].isna().any() or bool(row["is_gap"]) or int(row["gap_len"]) > 0:
                positions.append(current)
                continue

            if current != 0:
                held = idx - entry_idx
                if row["rv_pct_2880"] >= exit_rv_pct_min or held >= max_hold_bars:
                    current = 0
                    positions.append(0)
                    continue

            if current == 0 and row["rv_pct_2880"] <= entry_rv_pct_max:
                if row["close"] > row["prior_high_96"]:
                    current = 1
                    entry_idx = idx
                elif row["close"] < row["prior_low_96"]:
                    current = -1
                    entry_idx = idx

            positions.append(current)

        return pd.Series(positions, index=merged["timestamp"], name="position").astype(int)
