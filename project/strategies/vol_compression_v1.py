from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd

from pipelines._lib.validation import ensure_utc_timestamp


@dataclass
class VolCompressionV1:
    name: str = "vol_compression_v1"
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
        """
        Generate positions for the vol compression -> expansion strategy.
        """
        strategy_overrides = params.get("strategy_overrides", {})
        if isinstance(strategy_overrides, dict):
            strategy_params = strategy_overrides.get(self.name, {})
            if not isinstance(strategy_params, dict):
                strategy_params = {}
        else:
            strategy_params = {}

        local_params = dict(params)
        local_params.update(strategy_params)

        one_trade_per_day = bool(local_params.get("one_trade_per_day", True))
        trade_day_timezone = str(local_params.get("trade_day_timezone", "UTC"))
        compression_rv_pct_max = float(local_params.get("compression_rv_pct_max", 10.0))
        compression_range_mult = float(local_params.get("compression_range_mult", 0.8))
        exit_rv_pct_revert = float(local_params.get("exit_rv_pct_revert", 40.0))
        max_hold_bars = int(local_params.get("max_hold_bars", 48))

        bars = bars.copy()
        features = features.copy()

        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
        features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)
        ensure_utc_timestamp(bars["timestamp"], "timestamp")
        ensure_utc_timestamp(features["timestamp"], "timestamp")

        feature_base = features.drop(columns=["open", "high", "low", "close"], errors="ignore")
        merged = feature_base.merge(
            bars[["timestamp", "open", "high", "low", "close", "is_gap", "gap_len"]],
            on="timestamp",
            how="left",
        )
        merged = merged.sort_values("timestamp").reset_index(drop=True)
        merged["is_gap"] = merged["is_gap"].fillna(False)
        merged["gap_len"] = merged["gap_len"].fillna(0)
        merged["prior_high_96"] = merged["high_96"].shift(1)
        merged["prior_low_96"] = merged["low_96"].shift(1)

        required_fields = self.required_features + ["prior_high_96", "prior_low_96", "close", "high", "low"]

        positions: List[int] = []
        in_position = False
        position: Dict[str, object] = {}
        last_trade_day: Optional[pd.Timestamp] = None

        for idx, row in merged.iterrows():
            ts = row["timestamp"]
            day = ts.tz_convert(trade_day_timezone).normalize()

            if row[required_fields].isna().any() or row["is_gap"] or row["gap_len"] > 0:
                positions.append(1 if in_position and position.get("direction") == "long" else -1 if in_position else 0)
                continue

            if in_position:
                bars_held = int(idx - position["entry_index"])
                exit_triggered = False

                if position["direction"] == "long" and row["low"] <= position["stop_price"]:
                    exit_triggered = True
                elif position["direction"] == "short" and row["high"] >= position["stop_price"]:
                    exit_triggered = True
                else:
                    if position["direction"] == "long" and row["high"] >= position["target_price"]:
                        exit_triggered = True
                    elif position["direction"] == "short" and row["low"] <= position["target_price"]:
                        exit_triggered = True
                    elif bars_held >= max_hold_bars:
                        exit_triggered = True
                    elif row["rv_pct_2880"] > exit_rv_pct_revert:
                        exit_triggered = True

                if exit_triggered:
                    in_position = False
                    position = {}
                    positions.append(0)
                    continue

            if in_position or (one_trade_per_day and last_trade_day is not None and day == last_trade_day):
                positions.append(1 if in_position and position.get("direction") == "long" else -1 if in_position else 0)
                continue

            compression = row["rv_pct_2880"] <= compression_rv_pct_max and row["range_96"] <= compression_range_mult * row["range_med_480"]
            if not compression:
                positions.append(0)
                continue

            if row["close"] > row["prior_high_96"]:
                direction = "long"
            elif row["close"] < row["prior_low_96"]:
                direction = "short"
            else:
                positions.append(0)
                continue

            entry_price = row["close"]
            stop_price = row["low_96"] if direction == "long" else row["high_96"]
            risk_per_unit = entry_price - stop_price if direction == "long" else stop_price - entry_price
            if risk_per_unit <= 0:
                positions.append(0)
                continue

            target_price = entry_price + 2 * risk_per_unit if direction == "long" else entry_price - 2 * risk_per_unit
            position = {
                "direction": direction,
                "entry_index": idx,
                "stop_price": stop_price,
                "target_price": target_price,
            }
            in_position = True
            last_trade_day = day
            positions.append(1 if direction == "long" else -1)

        positions_series = pd.Series(positions, index=merged["timestamp"], name="position")
        positions_series = positions_series.astype(int)
        return positions_series
