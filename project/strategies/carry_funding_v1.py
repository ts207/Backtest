from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import pandas as pd

from pipelines._lib.validation import ensure_utc_timestamp


@dataclass
class CarryFundingV1:
    name: str = "carry_funding_v1"
    required_features: List[str] = None

    def __post_init__(self) -> None:
        if self.required_features is None:
            self.required_features = ["fp_norm_due"]

    @staticmethod
    def _resolve_funding_signal(frame: pd.DataFrame) -> pd.Series:
        for column in ["funding_rate_z", "funding_z", "fp_severity"]:
            if column in frame.columns:
                return pd.to_numeric(frame[column], errors="coerce")
        return pd.Series(0.0, index=frame.index, dtype=float)

    def generate_positions(self, bars: pd.DataFrame, features: pd.DataFrame, params: Dict[str, object]) -> pd.Series:
        extreme_threshold = float(params.get("funding_extreme_threshold", 1.5))
        neutral_threshold = float(params.get("funding_neutral_threshold", 0.35))
        max_hold_bars = max(1, int(params.get("max_hold_bars", 48)))
        use_vol_filter = bool(params.get("use_vol_filter", False))
        rv_pct_max = float(params.get("vol_filter_rv_pct_max", 75.0))
        min_size = max(0.1, float(params.get("funding_size_floor", 0.25)))
        max_size = max(min_size, float(params.get("funding_size_cap", 1.5)))

        bars = bars.copy()
        features = features.copy()
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
        features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)
        ensure_utc_timestamp(bars["timestamp"], "timestamp")
        ensure_utc_timestamp(features["timestamp"], "timestamp")

        merged = bars[["timestamp", "close"]].merge(features, on="timestamp", how="left")
        funding_signal = self._resolve_funding_signal(merged)
        rv_pct = pd.to_numeric(merged.get("rv_pct_2880", pd.Series(0.0, index=merged.index)), errors="coerce")

        positions: List[int] = []
        signal_events: List[Dict[str, object]] = []
        in_position = False
        direction = 0
        entry_idx = -1

        for idx, row in merged.iterrows():
            ts = row["timestamp"]
            funding = float(funding_signal.iloc[idx]) if pd.notna(funding_signal.iloc[idx]) else 0.0
            rv_value = float(rv_pct.iloc[idx]) if pd.notna(rv_pct.iloc[idx]) else 0.0

            if in_position:
                bars_held = idx - entry_idx
                normalized = abs(funding) <= neutral_threshold
                timed_out = bars_held >= max_hold_bars
                vol_exit = use_vol_filter and rv_value > rv_pct_max
                if normalized or timed_out or vol_exit:
                    signal_events.append(
                        {
                            "timestamp": ts.isoformat(),
                            "event": "exit",
                            "reason": "funding_normalized" if normalized else "time" if timed_out else "vol_filter",
                            "direction": "long" if direction > 0 else "short",
                        }
                    )
                    in_position = False
                    direction = 0
                    positions.append(0)
                    continue

            if not in_position and abs(funding) >= extreme_threshold and (not use_vol_filter or rv_value <= rv_pct_max):
                direction = -1 if funding > 0 else 1
                magnitude_scale = min(max_size, max(min_size, abs(funding) / max(extreme_threshold, 1e-9)))
                in_position = True
                entry_idx = idx
                signal_events.append(
                    {
                        "timestamp": ts.isoformat(),
                        "event": "entry",
                        "reason": "funding_extreme",
                        "direction": "long" if direction > 0 else "short",
                        "size_mult": round(float(magnitude_scale), 4),
                    }
                )
                positions.append(direction)
                continue

            positions.append(direction if in_position else 0)

        out = pd.Series(positions, index=merged["timestamp"], name="position").astype(int)
        out.attrs["signal_events"] = signal_events
        out.attrs["strategy_metadata"] = {
            "family": "carry",
            "strategy_id": self.name,
            "key_params": {
                "funding_extreme_threshold": extreme_threshold,
                "funding_neutral_threshold": neutral_threshold,
                "max_hold_bars": max_hold_bars,
                "use_vol_filter": use_vol_filter,
                "funding_size_floor": min_size,
                "funding_size_cap": max_size,
            },
        }
        return out
