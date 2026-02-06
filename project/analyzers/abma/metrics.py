from __future__ import annotations

import pandas as pd

from .config_v1 import ABMAConfig, DEFAULT_ABMA_CONFIG


def compute_structural_metrics(
    events: pd.DataFrame,
    trades: pd.DataFrame,
    quotes: pd.DataFrame,
    *,
    config: ABMAConfig = DEFAULT_ABMA_CONFIG,
) -> pd.DataFrame:
    """Compute v1 structural metrics for each event."""
    raise NotImplementedError("ABMA v1 metric computation is not implemented yet.")
