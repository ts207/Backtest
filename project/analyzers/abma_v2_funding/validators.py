from __future__ import annotations

import pandas as pd


REQUIRED_TRADES_COLUMNS = {"ts", "symbol", "price", "size"}
REQUIRED_QUOTES_COLUMNS = {"ts", "symbol", "bid_px", "ask_px", "bid_sz", "ask_sz"}
REQUIRED_CALENDAR_COLUMNS = {"session_date", "open_ts", "close_ts"}
REQUIRED_FUNDING_COLUMNS = {"ts", "symbol", "event_type", "source"}


def ensure_columns(frame: pd.DataFrame, required: set[str], name: str) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


def ensure_tz_aware(series: pd.Series, column_name: str) -> pd.Series:
    if len(series) == 0:
        return pd.to_datetime(series, utc=True, errors="coerce")
    ts = pd.to_datetime(series, utc=False, errors="coerce")
    if ts.isna().any():
        raise ValueError(f"{column_name} contains invalid timestamps")
    if ts.dt.tz is None:
        raise ValueError(f"{column_name} must be timezone-aware")
    return ts.dt.tz_convert("UTC")
