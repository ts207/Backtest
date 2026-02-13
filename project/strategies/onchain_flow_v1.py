from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import pandas as pd

from pipelines._lib.validation import ensure_utc_timestamp


@dataclass
class OnchainFlowV1:
    name: str = "onchain_flow_v1"
    required_features: List[str] = None

    def __post_init__(self) -> None:
        if self.required_features is None:
            self.required_features = ["onchain_flow_mc"]

    def generate_positions(self, bars: pd.DataFrame, features: pd.DataFrame, params: Dict[str, object]) -> pd.Series:
        threshold = float(params.get("onchain_signal_threshold", 0.0))

        bars = bars.copy()
        features = features.copy()
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
        features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)
        ensure_utc_timestamp(bars["timestamp"], "timestamp")
        ensure_utc_timestamp(features["timestamp"], "timestamp")

        merged = bars[["timestamp"]].merge(features, on="timestamp", how="left")
        if "onchain_flow_mc" in merged.columns:
            signal = pd.to_numeric(merged["onchain_flow_mc"], errors="coerce").fillna(0.0)
        else:
            signal = pd.Series(0.0, index=merged.index, dtype=float)
        signal.index = merged["timestamp"]

        pos = pd.Series(0, index=merged["timestamp"], name="position", dtype=int)
        pos[signal > threshold] = 1
        pos[signal < -threshold] = -1

        pos.attrs["strategy_metadata"] = {
            "family": "onchain",
            "strategy_id": self.name,
            "key_params": {
                "onchain_signal_threshold": threshold,
            },
        }
        return pos
