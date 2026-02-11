from __future__ import annotations

from typing import Iterable

import pandas as pd


def ensure_utc_timestamp(series: pd.Series, name: str) -> pd.Series:
    """
    Validate that a pandas Series of timestamps is timezone-aware UTC.
    """
    if not isinstance(series.dtype, pd.DatetimeTZDtype):
        raise ValueError(f"{name} must be timezone-aware UTC")
    if str(series.dt.tz) != "UTC":
        raise ValueError(f"{name} must be UTC")
    return series


def validate_columns(df: pd.DataFrame, required: Iterable[str]) -> None:
    """
    Ensure that a DataFrame contains the required columns.
    """
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")
