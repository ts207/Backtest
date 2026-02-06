from __future__ import annotations

import pandas as pd

from .config_v1 import ABMAConfig, DEFAULT_ABMA_CONFIG


def sample_matched_controls(
    events: pd.DataFrame,
    trades: pd.DataFrame,
    quotes: pd.DataFrame,
    session_calendar: pd.DataFrame,
    *,
    config: ABMAConfig = DEFAULT_ABMA_CONFIG,
) -> pd.DataFrame:
    """Sample mechanically matched non-auction controls (v1)."""
    raise NotImplementedError("ABMA v1 control sampling is not implemented yet.")
