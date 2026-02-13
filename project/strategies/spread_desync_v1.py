from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import pandas as pd

from pipelines._lib.validation import ensure_utc_timestamp


@dataclass
class SpreadDesyncV1:
    name: str = "spread_desync_v1"
    required_features: List[str] = None

    def __post_init__(self) -> None:
        if self.required_features is None:
            self.required_features = ["basis_zscore"]

    @staticmethod
    def _resolve_dislocation_signal(frame: pd.DataFrame) -> pd.Series:
        for col in ["basis_zscore", "cross_exchange_spread_z", "spread_zscore"]:
            if col in frame.columns:
                return pd.to_numeric(frame[col], errors="coerce")
        return pd.Series(0.0, index=frame.index, dtype=float)

    def generate_positions(self, bars: pd.DataFrame, features: pd.DataFrame, params: Dict[str, object]) -> pd.Series:
        dislocation_threshold = float(params.get("dislocation_threshold", 1.25))
        convergence_threshold = float(params.get("convergence_threshold", 0.2))
        max_hold_bars = max(1, int(params.get("max_hold_bars", 32)))

        bars = bars.copy()
        features = features.copy()
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
        features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)
        ensure_utc_timestamp(bars["timestamp"], "timestamp")
        ensure_utc_timestamp(features["timestamp"], "timestamp")

        merged = bars[["timestamp"]].merge(features, on="timestamp", how="left")
        spread_signal = self._resolve_dislocation_signal(merged)

        positions: List[int] = []
        signal_events: List[Dict[str, object]] = []
        in_position = False
        direction = 0
        entry_idx = -1

        for idx, row in merged.iterrows():
            ts = row["timestamp"]
            dislocation = float(spread_signal.iloc[idx]) if pd.notna(spread_signal.iloc[idx]) else 0.0

            if in_position:
                bars_held = idx - entry_idx
                converged = abs(dislocation) <= convergence_threshold
                timed_out = bars_held >= max_hold_bars
                if converged or timed_out:
                    signal_events.append(
                        {
                            "timestamp": ts.isoformat(),
                            "event": "exit",
                            "reason": "convergence" if converged else "time",
                            "direction": "long" if direction > 0 else "short",
                        }
                    )
                    in_position = False
                    direction = 0
                    positions.append(0)
                    continue

            if not in_position and abs(dislocation) >= dislocation_threshold:
                direction = -1 if dislocation > 0 else 1
                in_position = True
                entry_idx = idx
                signal_events.append(
                    {
                        "timestamp": ts.isoformat(),
                        "event": "entry",
                        "reason": "spread_dislocation",
                        "direction": "long" if direction > 0 else "short",
                    }
                )

            positions.append(direction if in_position else 0)

        out = pd.Series(positions, index=merged["timestamp"], name="position").astype(int)
        out.attrs["signal_events"] = signal_events
        out.attrs["strategy_metadata"] = {
            "family": "spread",
            "strategy_id": self.name,
            "key_params": {
                "dislocation_threshold": dislocation_threshold,
                "convergence_threshold": convergence_threshold,
                "max_hold_bars": max_hold_bars,
            },
        }
        return out
