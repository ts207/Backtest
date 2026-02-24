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
                "range_96",
                "high_96",
                "low_96",
            ]

    @staticmethod
    def _pick_window_column(columns: List[str], prefix: str, fallback: str) -> str:
        if fallback in columns:
            return fallback
        candidates: List[tuple[int, str]] = []
        for col in columns:
            if not col.startswith(prefix):
                continue
            try:
                window = int(col.split("_")[-1])
            except ValueError:
                continue
            candidates.append((window, col))
        if not candidates:
            raise ValueError(f"Missing required feature prefix: {prefix}")
        # Prefer the longest smoothing window to reduce churn.
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

    def generate_positions(
        self,
        bars: pd.DataFrame,
        features: pd.DataFrame,
        params: Dict[str, object],
    ) -> pd.Series:
        """
        Generate positions for the vol compression -> expansion strategy.
        """
        one_trade_per_day = bool(params.get("one_trade_per_day", False))
        trade_day_timezone = str(params.get("trade_day_timezone", "UTC"))
        compression_rv_pct_max = float(params.get("compression_rv_pct_max", 10.0))
        compression_range_ratio_max = float(params.get("compression_range_ratio_max", 0.8))
        breakout_confirm_bars = max(0, int(params.get("breakout_confirm_bars", 0)))
        breakout_confirm_buffer_bps = float(params.get("breakout_confirm_buffer_bps", 0.0)) / 10_000.0
        max_hold_bars = max(1, int(params.get("max_hold_bars", 48)))
        volatility_exit_rv_pct = float(params.get("volatility_exit_rv_pct", 40.0))
        expansion_timeout_bars = max(0, int(params.get("expansion_timeout_bars", 0)))
        expansion_min_r = float(params.get("expansion_min_r", 0.5))
        adaptive_exit_enabled = bool(params.get("adaptive_exit_enabled", False))
        adaptive_activation_r = float(params.get("adaptive_activation_r", 1.0))
        adaptive_trail_lookback_bars = max(1, int(params.get("adaptive_trail_lookback_bars", 8)))
        adaptive_trail_buffer_bps = float(params.get("adaptive_trail_buffer_bps", 0.0)) / 10_000.0

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
        rv_pct_col = self._pick_window_column(list(merged.columns), "rv_pct_", fallback="rv_pct_2880")
        range_med_col = self._pick_window_column(list(merged.columns), "range_med_", fallback="range_med_480")
        merged["is_gap"] = merged["is_gap"].fillna(False)
        merged["gap_len"] = merged["gap_len"].fillna(0)
        merged["prior_high_96"] = merged["high_96"].shift(1)
        merged["prior_low_96"] = merged["low_96"].shift(1)

        required_fields = self.required_features + [rv_pct_col, range_med_col, "prior_high_96", "prior_low_96", "close", "high", "low"]

        positions: List[int] = []
        in_position = False
        position: Dict[str, object] = {}
        last_trade_day: Optional[pd.Timestamp] = None
        long_breakout_streak = 0
        short_breakout_streak = 0
        signal_events: List[Dict[str, object]] = []

        for idx, row in merged.iterrows():
            ts = row["timestamp"]
            day = ts.tz_convert(trade_day_timezone).normalize()

            if row[required_fields].isna().any() or row["is_gap"] or row["gap_len"] > 0:
                long_breakout_streak = 0
                short_breakout_streak = 0
                positions.append(1 if in_position and position.get("direction") == "long" else -1 if in_position else 0)
                continue

            if in_position:
                bars_held = int(idx - position["entry_index"])
                exit_triggered = False

                if position["direction"] == "long" and row["low"] <= position["stop_price"]:
                    exit_triggered = True
                    exit_reason = "stop"
                elif position["direction"] == "short" and row["high"] >= position["stop_price"]:
                    exit_triggered = True
                    exit_reason = "stop"
                else:
                    if position["direction"] == "long" and row["high"] >= position["target_price"]:
                        exit_triggered = True
                        exit_reason = "target"
                    elif position["direction"] == "short" and row["low"] <= position["target_price"]:
                        exit_triggered = True
                        exit_reason = "target"
                    elif bars_held >= max_hold_bars:
                        exit_triggered = True
                        exit_reason = "time"
                    elif row[rv_pct_col] > volatility_exit_rv_pct:
                        exit_triggered = True
                        exit_reason = "volatility_expansion"

                risk_per_unit = float(position.get("risk_per_unit", 0.0) or 0.0)
                if risk_per_unit > 0:
                    if position["direction"] == "long":
                        mfe_price = float(row["high"]) - float(position["entry_price"])
                    else:
                        mfe_price = float(position["entry_price"]) - float(row["low"])
                    mfe_r = mfe_price / risk_per_unit
                    position["mfe_r"] = max(float(position.get("mfe_r", 0.0) or 0.0), float(mfe_r))

                if (
                    not exit_triggered
                    and expansion_timeout_bars > 0
                    and bars_held >= expansion_timeout_bars
                    and float(position.get("mfe_r", 0.0) or 0.0) < expansion_min_r
                ):
                    exit_triggered = True
                    exit_reason = "no_expansion_timeout"

                if (
                    not exit_triggered
                    and adaptive_exit_enabled
                    and risk_per_unit > 0
                    and float(position.get("mfe_r", 0.0) or 0.0) >= adaptive_activation_r
                ):
                    lookback_end_idx = idx - 1
                    if lookback_end_idx >= 0:
                        start_idx = max(0, lookback_end_idx - adaptive_trail_lookback_bars + 1)
                        if position["direction"] == "long":
                            lookback_low = float(merged.loc[start_idx:lookback_end_idx, "low"].min())
                            adaptive_stop = lookback_low * (1.0 - adaptive_trail_buffer_bps)
                            position["stop_price"] = max(float(position["stop_price"]), adaptive_stop)
                            if row["low"] <= position["stop_price"]:
                                exit_triggered = True
                                exit_reason = "adaptive_trailing_stop"
                        else:
                            lookback_high = float(merged.loc[start_idx:lookback_end_idx, "high"].max())
                            adaptive_stop = lookback_high * (1.0 + adaptive_trail_buffer_bps)
                            position["stop_price"] = min(float(position["stop_price"]), adaptive_stop)
                            if row["high"] >= position["stop_price"]:
                                exit_triggered = True
                                exit_reason = "adaptive_trailing_stop"

                if exit_triggered:
                    signal_events.append(
                        {
                            "timestamp": ts.isoformat(),
                            "event": "exit",
                            "reason": exit_reason,
                            "direction": position.get("direction"),
                        }
                    )
                    in_position = False
                    position = {}
                    positions.append(0)
                    continue

            if in_position:
                positions.append(1 if in_position and position.get("direction") == "long" else -1 if in_position else 0)
                continue

            compression = row[rv_pct_col] <= compression_rv_pct_max and row["range_96"] <= (
                compression_range_ratio_max * row[range_med_col]
            )
            if not compression:
                long_breakout_streak = 0
                short_breakout_streak = 0
                positions.append(0)
                continue

            long_threshold = row["prior_high_96"] * (1.0 + breakout_confirm_buffer_bps)
            short_threshold = row["prior_low_96"] * (1.0 - breakout_confirm_buffer_bps)

            if row["close"] > long_threshold:
                long_breakout_streak += 1
            else:
                long_breakout_streak = 0

            if row["close"] < short_threshold:
                short_breakout_streak += 1
            else:
                short_breakout_streak = 0

            if long_breakout_streak >= breakout_confirm_bars + 1:
                direction = "long"
            elif short_breakout_streak >= breakout_confirm_bars + 1:
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
                "entry_price": entry_price,
                "stop_price": stop_price,
                "target_price": target_price,
                "risk_per_unit": risk_per_unit,
                "mfe_r": 0.0,
            }
            in_position = True
            last_trade_day = day
            long_breakout_streak = 0
            short_breakout_streak = 0
            signal_events.append(
                {
                    "timestamp": ts.isoformat(),
                    "event": "entry",
                    "reason": f"breakout_confirmed_{direction}",
                    "direction": direction,
                }
            )
            positions.append(1 if direction == "long" else -1)

        positions_series = pd.Series(positions, index=merged["timestamp"], name="position")
        positions_series = positions_series.astype(int)
        positions_series.attrs["signal_events"] = signal_events
        return positions_series
