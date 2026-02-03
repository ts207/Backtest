from __future__ import annotations

from typing import Tuple

import numpy as np
import pandas as pd

from pipelines._lib.validation import ensure_utc_timestamp, validate_columns

FUNDING_MAX_ABS = 0.01
FUNDING_SCALE_CANDIDATES = (1.0, 0.01, 0.0001)


def assert_ohlcv_schema(df: pd.DataFrame) -> None:
    """
    Validate that OHLCV data has expected columns and numeric types.
    """
    required = ["timestamp", "open", "high", "low", "close", "volume"]
    validate_columns(df, required)
    for col in ["open", "high", "low", "close", "volume"]:
        if not pd.api.types.is_numeric_dtype(df[col]):
            raise ValueError(f"{col} must be numeric")


def assert_monotonic_utc_timestamp(df: pd.DataFrame, col: str = "timestamp") -> None:
    """
    Ensure timestamp column is tz-aware UTC, monotonic increasing, and unique.
    """
    validate_columns(df, [col])
    series = df[col]
    if series.isna().any():
        raise ValueError(f"{col} contains nulls")
    ensure_utc_timestamp(series, col)
    if series.duplicated().any():
        raise ValueError(f"{col} contains duplicate timestamps")
    if not series.is_monotonic_increasing:
        raise ValueError(f"{col} must be monotonic increasing")


def infer_and_apply_funding_scale(df: pd.DataFrame, col: str = "funding_rate") -> Tuple[pd.DataFrame, float]:
    """
    Infer funding rate scale and add funding_rate_scaled column.
    """
    validate_columns(df, [col])
    series = pd.to_numeric(df[col], errors="coerce")
    non_null = series.dropna()
    if non_null.empty:
        raise ValueError("No funding values available to infer scale")

    max_abs = float(non_null.abs().max())
    scale_used = None
    for scale in FUNDING_SCALE_CANDIDATES:
        if max_abs * scale <= FUNDING_MAX_ABS:
            scale_used = scale
            break

    if scale_used is None:
        raise ValueError(f"Unable to infer funding scale; max_abs={max_abs}")

    out = df.copy()
    out["funding_rate_scaled"] = series * scale_used
    return out, scale_used


def assert_funding_sane(df: pd.DataFrame, col: str = "funding_rate_scaled") -> None:
    """
    Ensure scaled funding rates are within plausible bounds.
    """
    validate_columns(df, [col])
    series = pd.to_numeric(df[col], errors="coerce")
    non_null = series.dropna()
    if non_null.empty:
        raise ValueError("No funding values to validate")
    max_abs = float(non_null.abs().max())
    if max_abs > FUNDING_MAX_ABS:
        raise ValueError(f"Funding rate exceeds {FUNDING_MAX_ABS:.4f}; max_abs={max_abs}")


def assert_funding_event_grid(
    df: pd.DataFrame, col: str = "timestamp", expected_hours: int = 8
) -> None:
    """
    Validate that funding events lie on the expected hourly grid.
    """
    validate_columns(df, [col])
    assert_monotonic_utc_timestamp(df, col)
    series = df[col]

    if series.dt.minute.ne(0).any() or series.dt.second.ne(0).any() or series.dt.microsecond.ne(0).any():
        raise ValueError("Funding timestamps must be on the hour")
    if (series.dt.hour % expected_hours != 0).any():
        raise ValueError(f"Funding timestamps must align to {expected_hours}h grid")

    diffs = series.sort_values().diff().dropna()
    if not diffs.empty:
        hours = diffs.dt.total_seconds() / 3600.0
        multiples = hours / float(expected_hours)
        if not np.allclose(multiples, np.round(multiples), atol=1e-6):
            raise ValueError("Funding timestamps must be spaced on the expected grid")


def is_constant_series(series: pd.Series) -> bool:
    """
    Return True if non-null values are constant (std == 0).
    """
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return False
    std = float(values.std(ddof=0))
    return bool(np.isclose(std, 0.0))


def coerce_timestamps_to_hour(df: pd.DataFrame, col: str = "timestamp") -> Tuple[pd.DataFrame, int]:
    """
    Round timestamps to the nearest hour and return adjusted row count.
    """
    validate_columns(df, [col])
    series = df[col]
    if series.isna().any():
        raise ValueError(f"{col} contains nulls")
    ensure_utc_timestamp(series, col)
    rounded = series.dt.round("h")
    adjusted = int((series != rounded).sum())
    out = df.copy()
    out[col] = rounded
    return out, adjusted
