from __future__ import annotations

from typing import List, Protocol

import pandas as pd


class Strategy(Protocol):
    name: str
    required_features: List[str]

    def generate_positions(
        self,
        bars: pd.DataFrame,
        features: pd.DataFrame,
        params: dict,
    ) -> pd.Series:
        """
        Generate a position stream with values in {-1, 0, 1}.
        Index must be tz-aware UTC timestamps.
        """
        raise NotImplementedError
