from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ABMAConfig:
    """Frozen v1 constants for the ABMA Phase-1 analyzer."""

    def_version: str = "v1"
    event_window_minutes: int = 30
    microprice_half_life_seconds: int = 60
    trade_sign_entropy_trades: int = 50
    control_set_size: int = 5
    bootstrap_samples: int = 1000


DEFAULT_ABMA_CONFIG = ABMAConfig()
