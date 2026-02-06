from __future__ import annotations

import pandas as pd

from .config_v1 import ABMAConfig, DEFAULT_ABMA_CONFIG


def build_report_artifacts(
    evaluation_outputs: dict[str, pd.DataFrame],
    *,
    config: ABMAConfig = DEFAULT_ABMA_CONFIG,
) -> dict[str, object]:
    """Build report artifacts (plots/tables) for ABMA v1."""
    raise NotImplementedError("ABMA v1 reporting is not implemented yet.")
