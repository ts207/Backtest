from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import pandas as pd

from pipelines._lib.validation import ensure_utc_timestamp


@dataclass
class MeanReversionExhaustionV1:
    name: str = "mean_reversion_exhaustion_v1"
    required_features: List[str] = None

    def __post_init__(self) -> None:
        if self.required_features is None:
            self.required_features = ["high_96", "low_96"]

    @staticmethod
    def _price_extension(close: pd.Series, baseline: pd.Series) -> pd.Series:
        return (close - baseline) / baseline.replace(0, pd.NA)

    def generate_positions(self, bars: pd.DataFrame, features: pd.DataFrame, params: Dict[str, object]) -> pd.Series:
        extension_threshold = float(params.get("extension_threshold", 0.01))
        reversion_threshold = float(params.get("reversion_threshold", 0.002))
        max_hold_bars = max(1, int(params.get("max_hold_bars", 24)))
        use_extreme_stop = bool(params.get("use_extreme_stop", False))
        stop_extension_threshold = float(params.get("stop_extension_threshold", extension_threshold * 2.0))

        bars = bars.copy()
        features = features.copy()
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
        features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)
        ensure_utc_timestamp(bars["timestamp"], "timestamp")
        ensure_utc_timestamp(features["timestamp"], "timestamp")

        merged = bars[["timestamp", "close"]].merge(features, on="timestamp", how="left", suffixes=("", "_feat"))
        rolling_mid = pd.to_numeric((merged.get("high_96") + merged.get("low_96")) / 2.0, errors="coerce")
        # Use the close from bars (no suffix)
        close_series = pd.to_numeric(merged["close"], errors="coerce")
        extension = self._price_extension(close_series, rolling_mid).fillna(0.0)

        positions: List[int] = [0] * len(merged)
        signal_events: List[Dict[str, object]] = []
        in_position = False
        direction = 0
        entry_idx = -1

        # Optimization: Access numpy arrays or itertuples for speed.
        # Pre-convert extension to numpy array for indexed access.
        ext_values = extension.values
        timestamps = merged["timestamp"].dt.to_pydatetime() # Faster than accessing .timestamp in loop

        for idx, row in enumerate(merged.itertuples(index=False)):
            ts = timestamps[idx]
            ext = float(ext_values[idx])

            if in_position:
                bars_held = idx - entry_idx
                reverted = abs(ext) <= reversion_threshold
                timed_out = bars_held >= max_hold_bars
                stopped = use_extreme_stop and ((direction > 0 and ext < -stop_extension_threshold) or (direction < 0 and ext > stop_extension_threshold))
                if reverted or timed_out or stopped:
                    signal_events.append(
                        {
                            "timestamp": ts.isoformat(),
                            "event": "exit",
                            "reason": "reversion" if reverted else "time" if timed_out else "extreme_stop",
                            "direction": "long" if direction > 0 else "short",
                        }
                    )
                    in_position = False
                    direction = 0
                    positions[idx] = 0
                    continue

            if not in_position:
                if ext >= extension_threshold:
                    direction = -1
                elif ext <= -extension_threshold:
                    direction = 1
                else:
                    direction = 0

                if direction != 0:
                    in_position = True
                    entry_idx = idx
                    signal_events.append(
                        {
                            "timestamp": ts.isoformat(),
                            "event": "entry",
                            "reason": "extension_exhaustion",
                            "direction": "long" if direction > 0 else "short",
                        }
                    )

            positions[idx] = direction if in_position else 0

        out = pd.Series(positions, index=merged["timestamp"], name="position").astype(int)
        out.attrs["signal_events"] = signal_events
        out.attrs["strategy_metadata"] = {
            "family": "mean_reversion",
            "strategy_id": self.name,
            "key_params": {
                "extension_threshold": extension_threshold,
                "reversion_threshold": reversion_threshold,
                "max_hold_bars": max_hold_bars,
                "use_extreme_stop": use_extreme_stop,
                "stop_extension_threshold": stop_extension_threshold,
            },
        }
        return out
