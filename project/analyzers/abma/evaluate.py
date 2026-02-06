from __future__ import annotations

import pandas as pd

from .config_v1 import ABMAConfig, DEFAULT_ABMA_CONFIG


def evaluate_stabilization(
    events: pd.DataFrame,
    controls: pd.DataFrame,
    *,
    config: ABMAConfig = DEFAULT_ABMA_CONFIG,
) -> dict[str, pd.DataFrame]:
    """Compute stabilization curves and stability gates (v1)."""
    raise NotImplementedError("ABMA v1 evaluation is not implemented yet.")
