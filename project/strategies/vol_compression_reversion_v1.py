from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import pandas as pd

from pipelines._lib.validation import ensure_utc_timestamp


@dataclass
class VolCompressionReversionV1:
    name: str = "vol_compression_reversion_v1"
    required_features: List[str] = None

    def __post_init__(self) -> None:
        if self.required_features is None:
            self.required_features = [
                "rv_pct_2880",
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

        entry_rv_pct_max = float(local_params.get("entry_rv_pct_max", 15.0))
        band_frac = float(local_params.get("band_frac", 0.2))
        max_hold_bars = int(local_params.get("max_hold_bars", 24))

        bars = bars.copy()
        features = features.copy()
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
        features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)
        ensure_utc_timestamp(bars["timestamp"], "timestamp")
        ensure_utc_timestamp(features["timestamp"], "timestamp")

        merged = features.merge(
            bars[["timestamp", "close", "is_gap", "gap_len"]],
            on="timestamp",
            how="left",
            suffixes=("", "_bar"),
        ).sort_values("timestamp")
        merged["is_gap"] = merged["is_gap"].fillna(False)
        merged["gap_len"] = merged["gap_len"].fillna(0)

        required = ["rv_pct_2880", "close", "high_96", "low_96"]

        positions: List[int] = []
        current = 0
        entry_idx = -1

        for idx, row in merged.iterrows():
            if row[required].isna().any() or bool(row["is_gap"]) or int(row["gap_len"]) > 0:
                positions.append(current)
                continue

            if current != 0:
                held = idx - entry_idx
                mid = (row["high_96"] + row["low_96"]) / 2.0
                if (current > 0 and row["close"] >= mid) or (current < 0 and row["close"] <= mid) or held >= max_hold_bars:
                    current = 0
                    positions.append(0)
                    continue

            if current == 0 and row["rv_pct_2880"] <= entry_rv_pct_max:
                span = row["high_96"] - row["low_96"]
                if span <= 0:
                    positions.append(0)
                    continue
                upper_trigger = row["high_96"] - band_frac * span
                lower_trigger = row["low_96"] + band_frac * span
                if row["close"] >= upper_trigger:
                    current = -1
                    entry_idx = idx
                elif row["close"] <= lower_trigger:
                    current = 1
                    entry_idx = idx

            positions.append(current)

        return pd.Series(positions, index=merged["timestamp"], name="position").astype(int)
