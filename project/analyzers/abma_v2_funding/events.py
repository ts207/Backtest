from __future__ import annotations

import hashlib
import pandas as pd

from .config_v2 import ABMAFundingConfig, DEFAULT_ABMA_FUNDING_CONFIG
from .validators import (
    REQUIRED_CALENDAR_COLUMNS,
    REQUIRED_FUNDING_COLUMNS,
    ensure_columns,
    ensure_tz_aware,
)


def _stable_event_id(symbol: str, session_date: str, t0_utc: pd.Timestamp, version: str) -> str:
    payload = f"{version}|{symbol}|{session_date}|{t0_utc.isoformat()}|FUNDING"
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]
    return f"abma_{version}_{symbol}_{session_date}_{digest}"


def extract_funding_boundary_events(
    funding_events: pd.DataFrame,
    session_calendar: pd.DataFrame,
    *,
    config: ABMAFundingConfig = DEFAULT_ABMA_FUNDING_CONFIG,
) -> pd.DataFrame:
    """Extract funding boundary events from run-scoped funding timestamps (v2)."""
    ensure_columns(funding_events, REQUIRED_FUNDING_COLUMNS, "funding_events")
    ensure_columns(session_calendar, REQUIRED_CALENDAR_COLUMNS, "session_calendar")

    funding = funding_events.copy()
    cal = session_calendar.copy()

    funding["ts"] = ensure_tz_aware(funding["ts"], "funding_events.ts")
    cal["open_ts"] = ensure_tz_aware(cal["open_ts"], "session_calendar.open_ts")
    cal["close_ts"] = ensure_tz_aware(cal["close_ts"], "session_calendar.close_ts")
    cal["session_date"] = pd.to_datetime(cal["session_date"], errors="coerce").dt.date
    if cal["session_date"].isna().any():
        raise ValueError("session_calendar.session_date contains invalid dates")
    if (cal["close_ts"] <= cal["open_ts"]).any():
        raise ValueError("session_calendar close_ts must be greater than open_ts")

    funding = funding[funding["event_type"].astype(str).str.upper() == "FUNDING"].copy()
    if funding.empty:
        return pd.DataFrame(
            columns=[
                "event_id",
                "symbol",
                "session_date",
                "t0",
                "window_end",
                "abma_def_version",
                "open_ts_utc",
                "close_ts_utc",
            ]
        )

    rows: list[dict[str, object]] = []
    for cal_row in cal.sort_values("open_ts").itertuples(index=False):
        session_date = str(cal_row.session_date)
        open_ts = pd.Timestamp(cal_row.open_ts).tz_convert("UTC")
        close_ts = pd.Timestamp(cal_row.close_ts).tz_convert("UTC")
        if close_ts <= open_ts:
            continue
        in_session = funding[(funding["ts"] >= open_ts) & (funding["ts"] <= close_ts)].copy()
        if in_session.empty:
            continue
        for event in in_session.sort_values("ts").itertuples(index=False):
            t0 = pd.Timestamp(event.ts).tz_convert("UTC")
            event_id = _stable_event_id(
                symbol=str(event.symbol),
                session_date=session_date,
                t0_utc=t0,
                version=config.def_version,
            )
            rows.append(
                {
                    "event_id": event_id,
                    "symbol": str(event.symbol),
                    "session_date": pd.Timestamp(session_date).date(),
                    "t0": t0,
                    "window_end": t0 + pd.Timedelta(minutes=config.event_window_minutes),
                    "abma_def_version": config.def_version,
                    "open_ts_utc": open_ts,
                    "close_ts_utc": close_ts,
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["symbol", "t0"]).reset_index(drop=True)
