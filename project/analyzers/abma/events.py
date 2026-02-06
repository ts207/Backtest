from __future__ import annotations

import pandas as pd

from .config_v1 import ABMAConfig, DEFAULT_ABMA_CONFIG


def extract_auction_boundary_events(
    trades: pd.DataFrame,
    quotes: pd.DataFrame,
    session_calendar: pd.DataFrame,
    *,
    config: ABMAConfig = DEFAULT_ABMA_CONFIG,
) -> pd.DataFrame:
    """Extract open-auction boundary events (v1).

    Not implemented in the skeleton; returns NotImplementedError to prevent
    accidental use without a defined exchange calendar pipeline.
    """
    raise NotImplementedError("ABMA v1 event extraction is not implemented yet.")
