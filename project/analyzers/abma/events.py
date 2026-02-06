from __future__ import annotations

import hashlib
import pandas as pd

from .config_v1 import ABMAConfig, DEFAULT_ABMA_CONFIG


REQUIRED_TRADES_COLUMNS = {"ts", "symbol", "price", "size"}
REQUIRED_QUOTES_COLUMNS = {"ts", "symbol", "bid_px", "ask_px", "bid_sz", "ask_sz"}
REQUIRED_CALENDAR_COLUMNS = {"session_date", "open_ts", "close_ts"}


def _ensure_columns(frame: pd.DataFrame, required: set[str], name: str) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


def _ensure_tz_aware(series: pd.Series, column_name: str) -> pd.Series:
    if len(series) == 0:
        return pd.to_datetime(series, utc=True, errors="coerce")
    ts = pd.to_datetime(series, utc=False, errors="coerce")
    if ts.isna().any():
        raise ValueError(f"{column_name} contains invalid timestamps")
    if ts.dt.tz is None:
        raise ValueError(f"{column_name} must be timezone-aware")
    return ts.dt.tz_convert("UTC")


def _stable_event_id(symbol: str, session_date: str, t0_utc: pd.Timestamp, version: str) -> str:
    payload = f"{version}|{symbol}|{session_date}|{t0_utc.isoformat()}"
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]
    return f"abma_{version}_{symbol}_{session_date}_{digest}"


def extract_auction_boundary_events(
    trades: pd.DataFrame,
    quotes: pd.DataFrame,
    session_calendar: pd.DataFrame,
    *,
    config: ABMAConfig = DEFAULT_ABMA_CONFIG,
) -> pd.DataFrame:
    """Extract open-auction boundaries using session open timestamps (v1)."""
    _ensure_columns(trades, REQUIRED_TRADES_COLUMNS, "trades")
    _ensure_columns(quotes, REQUIRED_QUOTES_COLUMNS, "quotes")
    _ensure_columns(session_calendar, REQUIRED_CALENDAR_COLUMNS, "session_calendar")

    trades_local = trades.copy()
    quotes_local = quotes.copy()
    cal = session_calendar.copy()

    trades_local["ts"] = _ensure_tz_aware(trades_local["ts"], "trades.ts")
    quotes_local["ts"] = _ensure_tz_aware(quotes_local["ts"], "quotes.ts")
    cal["open_ts"] = _ensure_tz_aware(cal["open_ts"], "session_calendar.open_ts")
    cal["close_ts"] = _ensure_tz_aware(cal["close_ts"], "session_calendar.close_ts")
    cal["session_date"] = pd.to_datetime(cal["session_date"], errors="coerce").dt.date
    if cal["session_date"].isna().any():
        raise ValueError("session_calendar.session_date contains invalid dates")
    if (cal["close_ts"] <= cal["open_ts"]).any():
        raise ValueError("session_calendar close_ts must be greater than open_ts")

    symbols = sorted(set(trades_local["symbol"].dropna().astype(str)).union(quotes_local["symbol"].dropna().astype(str)))
    if not symbols:
        return pd.DataFrame(
            columns=[
                "event_id",
                "symbol",
                "session_date",
                "t0",
                "window_end",
                "abma_def_version",
            ]
        )

    rows: list[dict[str, object]] = []
    for _, cal_row in cal.sort_values("open_ts").iterrows():
        session_date = str(cal_row["session_date"])
        t0 = pd.Timestamp(cal_row["open_ts"]).tz_convert("UTC")
        window_end = t0 + pd.Timedelta(minutes=config.event_window_minutes)
        for symbol in symbols:
            event_id = _stable_event_id(symbol=symbol, session_date=session_date, t0_utc=t0, version=config.def_version)
            rows.append(
                {
                    "event_id": event_id,
                    "symbol": symbol,
                    "session_date": pd.Timestamp(session_date).date(),
                    "t0": t0,
                    "window_end": window_end,
                    "abma_def_version": config.def_version,
                    "open_ts_utc": t0,
                    "close_ts_utc": pd.Timestamp(cal_row["close_ts"]).tz_convert("UTC"),
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["symbol", "t0"]).reset_index(drop=True)
