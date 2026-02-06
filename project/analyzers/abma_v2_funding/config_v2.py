from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ABMAFundingConfig:
    """Frozen v2 constants for the ABMA funding-boundary Phase-1 analyzer."""

    def_version: str = "v2_funding"
    event_window_minutes: int = 30
    microprice_half_life_seconds: int = 60
    trade_sign_entropy_trades: int = 50
    control_set_size: int = 5
    bootstrap_samples: int = 1000
    baseline_window_minutes: int = 15
    baseline_grid_seconds: int = 1
    baseline_anchor_step_seconds: int = 60
    min_valid_midpoints: int = 60
    sign_consistency_threshold: float = 0.70
    max_regime_flip_count: int = 1
    exclusion_pre_minutes: int = 10
    exclusion_post_minutes: int = 30
    overlay_delay_seconds: int = 300


DEFAULT_ABMA_FUNDING_CONFIG = ABMAFundingConfig()
