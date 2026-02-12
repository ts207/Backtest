from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import pandas as pd

from strategies.vol_compression_v1 import VolCompressionV1


@dataclass
class VolCompressionAdapter:
    name: str
    defaults: Dict[str, object]
    required_features: List[str] = None

    def __post_init__(self) -> None:
        self._base = VolCompressionV1()
        if self.required_features is None:
            self.required_features = list(self._base.required_features)

    def generate_positions(
        self,
        bars: pd.DataFrame,
        features: pd.DataFrame,
        params: Dict[str, object],
    ) -> pd.Series:
        merged_params = dict(params)
        # Adapter defaults are authoritative for the template behavior.
        merged_params.update(self.defaults)
        return self._base.generate_positions(bars, features, merged_params)


def build_adapter_registry() -> Dict[str, VolCompressionAdapter]:
    return {
        "liquidity_refill_lag_v1": VolCompressionAdapter(
            name="liquidity_refill_lag_v1",
            defaults={
                "breakout_confirm_bars": 1,
                "breakout_confirm_buffer_bps": 1.0,
                "max_hold_bars": 32,
                "expansion_timeout_bars": 10,
                "expansion_min_r": 0.3,
            },
        ),
        "liquidity_absence_gate_v1": VolCompressionAdapter(
            name="liquidity_absence_gate_v1",
            defaults={
                "compression_rv_pct_max": 12.0,
                "compression_range_ratio_max": 0.9,
                "breakout_confirm_bars": 1,
                "max_hold_bars": 36,
                "volatility_exit_rv_pct": 35.0,
            },
        ),
        "forced_flow_exhaustion_v1": VolCompressionAdapter(
            name="forced_flow_exhaustion_v1",
            defaults={
                "breakout_confirm_bars": 0,
                "max_hold_bars": 20,
                "expansion_timeout_bars": 6,
                "expansion_min_r": 0.25,
                "adaptive_exit_enabled": True,
                "adaptive_activation_r": 0.8,
            },
        ),
        "funding_extreme_reversal_v1": VolCompressionAdapter(
            name="funding_extreme_reversal_v1",
            defaults={
                "breakout_confirm_bars": 1,
                "breakout_confirm_buffer_bps": 4.0,
                "max_hold_bars": 24,
                "volatility_exit_rv_pct": 30.0,
            },
        ),
        "cross_venue_desync_v1": VolCompressionAdapter(
            name="cross_venue_desync_v1",
            defaults={
                "breakout_confirm_bars": 1,
                "breakout_confirm_buffer_bps": 2.0,
                "max_hold_bars": 18,
                "expansion_timeout_bars": 8,
                "expansion_min_r": 0.2,
            },
        ),
        "liquidity_vacuum_v1": VolCompressionAdapter(
            name="liquidity_vacuum_v1",
            defaults={
                "compression_rv_pct_max": 14.0,
                "compression_range_ratio_max": 1.0,
                "breakout_confirm_bars": 0,
                "max_hold_bars": 14,
                "expansion_timeout_bars": 5,
                "expansion_min_r": 0.2,
                "adaptive_exit_enabled": True,
                "adaptive_activation_r": 0.6,
            },
        ),
    }
