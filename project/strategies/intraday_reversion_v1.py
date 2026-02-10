from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import numpy as np
import pandas as pd

from pipelines._lib.validation import ensure_utc_timestamp


@dataclass
class IntradayReversionV1:
    name: str = "intraday_reversion_v1"
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

        n = int(local_params.get("n", 64))
        thr = float(local_params.get("thr", 1.5))
        gap_bars = int(local_params.get("gap_bars", 0))
        vol_gate_enabled = bool(local_params.get("vol_gate_enabled", False))
        vol_n = int(local_params.get("vol_n", 64))
        vol_cap = float(local_params.get("vol_cap", 0.02))
        if n <= 1:
            raise ValueError("n must be > 1")
        if thr < 0:
            raise ValueError("thr must be non-negative")

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
        r1 = np.log(px.replace(0.0, np.nan)).diff()
        mu = r1.rolling(window=n, min_periods=n).mean()
        sd = r1.rolling(window=n, min_periods=n).std().replace(0.0, np.nan)
        z = ((r1 - mu) / sd).replace([np.inf, -np.inf], np.nan).fillna(0.0)

        vol_ok = pd.Series(True, index=merged.index)
        if vol_gate_enabled:
            vol = r1.rolling(window=max(2, vol_n), min_periods=max(2, vol_n)).std()
            vol_ok = vol.fillna(0.0) <= vol_cap

        gap_ok = (~merged["is_gap"].astype(bool)) & (merged["gap_len"].astype(int) <= gap_bars)
        valid = gap_ok & vol_ok

        pos = pd.Series(0, index=merged.index, dtype=int)
        pos.loc[valid & (z >= thr)] = -1
        pos.loc[valid & (z <= -thr)] = 1

        return pd.Series(pos.values, index=merged["timestamp"], name="position").astype(int)
