from __future__ import annotations

import numpy as np
import pandas as pd

from .config_v2 import ABMAFundingConfig, DEFAULT_ABMA_FUNDING_CONFIG
from .validators import (
    REQUIRED_CALENDAR_COLUMNS,
    REQUIRED_QUOTES_COLUMNS,
    REQUIRED_TRADES_COLUMNS,
    ensure_columns,
    ensure_tz_aware,
)


def _quarter_bucket(values: pd.Series) -> pd.Series:
    if values.empty:
        return pd.Series(dtype="Int64")
    ranks = values.rank(method="average")
    denom = max(1.0, float(len(ranks) - 1))
    pct = (ranks - 1.0) / denom
    bucket = np.floor(pct * 4.0).astype(int) + 1
    return pd.Series(np.clip(bucket, 1, 4), index=values.index, dtype="Int64")


def _precompute_baselines(
    symbol_trades: pd.DataFrame,
    symbol_quotes: pd.DataFrame,
    open_ts: pd.Timestamp,
    close_ts: pd.Timestamp,
    *,
    config: ABMAFundingConfig,
) -> pd.DataFrame:
    grid = pd.date_range(start=open_ts, end=close_ts - pd.Timedelta(seconds=1), freq="1s", tz="UTC")
    if grid.empty:
        return pd.DataFrame(columns=["anchor_ts", "baseline_volume", "baseline_rv", "baseline_valid"])

    grid_df = pd.DataFrame({"anchor_ts": grid})
    quotes_grid = pd.merge_asof(grid_df, symbol_quotes.sort_values("ts"), left_on="anchor_ts", right_on="ts", direction="backward")
    mid = ((quotes_grid["bid_px"].astype(float) + quotes_grid["ask_px"].astype(float)) / 2.0).where(
        (quotes_grid["bid_px"].astype(float) > 0.0) & (quotes_grid["ask_px"].astype(float) > 0.0)
    )
    valid_mid = mid.notna()
    log_mid = np.log(mid.where(mid > 0.0))
    rv_comp = log_mid.diff().pow(2)

    window_seconds = config.baseline_window_minutes * 60
    rv_roll = rv_comp.rolling(window_seconds, min_periods=1).sum().shift(1)
    valid_roll = valid_mid.astype(int).rolling(window_seconds, min_periods=1).sum().shift(1)

    trades_sec = symbol_trades.copy()
    if not trades_sec.empty:
        trades_sec["sec"] = trades_sec["ts"].dt.floor("1s")
        vol_by_sec = trades_sec.groupby("sec", as_index=True)["size"].sum()
    else:
        vol_by_sec = pd.Series(dtype=float)

    volume_series = pd.Series(0.0, index=grid)
    if not vol_by_sec.empty:
        volume_series = volume_series.add(vol_by_sec.reindex(grid, fill_value=0.0), fill_value=0.0)
    vol_roll = volume_series.rolling(window_seconds, min_periods=1).sum().shift(1)

    out = pd.DataFrame(
        {
            "anchor_ts": grid,
            "baseline_volume": vol_roll.to_numpy(),
            "baseline_rv": rv_roll.to_numpy(),
            "valid_midpoints": valid_roll.to_numpy(),
        }
    )
    out["baseline_valid"] = out["valid_midpoints"] >= int(config.min_valid_midpoints)
    out["baseline_valid"] = out["baseline_valid"] & (out["anchor_ts"] >= open_ts + pd.Timedelta(minutes=config.baseline_window_minutes))
    return out


def _anchors_for_session(open_ts: pd.Timestamp, close_ts: pd.Timestamp, *, config: ABMAFundingConfig) -> pd.DatetimeIndex:
    start = open_ts + pd.Timedelta(minutes=config.baseline_window_minutes)
    end = close_ts - pd.Timedelta(seconds=1)
    if end < start:
        return pd.DatetimeIndex([], tz="UTC")
    return pd.date_range(start=start, end=end, freq=f"{config.baseline_anchor_step_seconds}s", tz="UTC")


def _apply_exclusion_windows(
    candidate_df: pd.DataFrame,
    event_times: pd.Series,
    *,
    config: ABMAFundingConfig,
) -> pd.DataFrame:
    if candidate_df.empty or event_times.empty:
        return candidate_df
    pre = pd.Timedelta(minutes=config.exclusion_pre_minutes)
    post = pd.Timedelta(minutes=config.exclusion_post_minutes)
    keep = pd.Series(True, index=candidate_df.index)
    for t0 in event_times:
        window_start = pd.Timestamp(t0) - pre
        window_end = pd.Timestamp(t0) + post
        keep &= ~candidate_df["anchor_ts"].between(window_start, window_end, inclusive="both")
    return candidate_df[keep].copy()


def sample_matched_controls(
    events: pd.DataFrame,
    trades: pd.DataFrame,
    quotes: pd.DataFrame,
    session_calendar: pd.DataFrame,
    *,
    config: ABMAFundingConfig = DEFAULT_ABMA_FUNDING_CONFIG,
) -> pd.DataFrame:
    """Sample mechanically matched controls for funding boundary anchors (v2)."""
    ensure_columns(events, {"event_id", "symbol", "session_date", "t0", "open_ts_utc", "close_ts_utc"}, "events")
    ensure_columns(trades, REQUIRED_TRADES_COLUMNS, "trades")
    ensure_columns(quotes, REQUIRED_QUOTES_COLUMNS, "quotes")
    ensure_columns(session_calendar, REQUIRED_CALENDAR_COLUMNS, "session_calendar")

    events_local = events.copy()
    trades_local = trades.copy()
    quotes_local = quotes.copy()

    events_local["t0"] = ensure_tz_aware(events_local["t0"], "events.t0")
    events_local["open_ts_utc"] = ensure_tz_aware(events_local["open_ts_utc"], "events.open_ts_utc")
    events_local["close_ts_utc"] = ensure_tz_aware(events_local["close_ts_utc"], "events.close_ts_utc")
    events_local["session_date"] = pd.to_datetime(events_local["session_date"], errors="coerce").dt.date
    trades_local["ts"] = ensure_tz_aware(trades_local["ts"], "trades.ts")
    quotes_local["ts"] = ensure_tz_aware(quotes_local["ts"], "quotes.ts")
    trades_local = trades_local.sort_values(["symbol", "ts"]).reset_index(drop=True)
    quotes_local = quotes_local.sort_values(["symbol", "ts"]).reset_index(drop=True)

    rows: list[dict[str, object]] = []
    no_controls: list[dict[str, object]] = []

    grouped_events = events_local.sort_values(["symbol", "session_date", "t0"]).groupby(["symbol", "session_date"], sort=False)
    for (symbol, session_date), group in grouped_events:
        symbol_trades = trades_local[trades_local["symbol"].astype(str) == str(symbol)][["ts", "size"]].copy()
        symbol_quotes = quotes_local[quotes_local["symbol"].astype(str) == str(symbol)][["ts", "bid_px", "ask_px", "bid_sz", "ask_sz"]].copy()
        if symbol_quotes.empty:
            continue

        open_ts = pd.Timestamp(group["open_ts_utc"].iloc[0]).tz_convert("UTC")
        close_ts = pd.Timestamp(group["close_ts_utc"].iloc[0]).tz_convert("UTC")
        baseline_table = _precompute_baselines(symbol_trades, symbol_quotes, open_ts, close_ts, config=config)
        if baseline_table.empty:
            continue
        baseline_lookup = baseline_table.set_index("anchor_ts")

        candidates = _anchors_for_session(open_ts, close_ts, config=config)
        if candidates.empty:
            continue

        candidate_df = (
            baseline_table[baseline_table["anchor_ts"].isin(candidates)][["anchor_ts", "baseline_volume", "baseline_rv", "baseline_valid"]]
            .copy()
        )
        candidate_df["symbol"] = symbol
        candidate_df["session_date"] = session_date
        candidate_df = candidate_df[candidate_df["baseline_valid"]].copy()
        candidate_df = _apply_exclusion_windows(candidate_df, group["t0"], config=config)
        if candidate_df.empty:
            continue
        candidate_df["rv_q"] = _quarter_bucket(candidate_df["baseline_rv"])
        candidate_df["vol_q"] = _quarter_bucket(candidate_df["baseline_volume"])
        candidate_df = candidate_df.dropna(subset=["rv_q"]).copy()

        for _, event_row in group.iterrows():
            t0 = pd.Timestamp(event_row["t0"]).tz_convert("UTC")
            if t0 not in baseline_lookup.index:
                no_controls.append(
                    {
                        "event_id": event_row["event_id"],
                        "symbol": symbol,
                        "session_date": session_date,
                        "reason": "event_anchor_outside_session_grid",
                    }
                )
                continue
            event_baseline = baseline_lookup.loc[t0]
            if not bool(event_baseline["baseline_valid"]):
                no_controls.append(
                    {
                        "event_id": event_row["event_id"],
                        "symbol": symbol,
                        "session_date": session_date,
                        "reason": "baseline_crosses_session_start_or_insufficient_midpoints",
                    }
                )
                continue

            event_rv_rank = (candidate_df["baseline_rv"] <= float(event_baseline["baseline_rv"])).mean()
            event_vol_rank = (candidate_df["baseline_volume"] <= float(event_baseline["baseline_volume"])).mean()
            event_rv_q = int(np.clip(np.ceil(event_rv_rank * 4.0), 1, 4))
            event_vol_q = int(np.clip(np.ceil(event_vol_rank * 4.0), 1, 4))

            non_event = candidate_df[candidate_df["anchor_ts"] != t0].copy()
            exact_pool = non_event[(non_event["rv_q"] == event_rv_q) & (non_event["vol_q"] == event_vol_q)]
            match_tier = "exact"
            pool = exact_pool
            if len(pool) < config.control_set_size:
                pool = non_event[non_event["rv_q"] == event_rv_q].copy()
                match_tier = "rv_only"
            if pool.empty:
                no_controls.append(
                    {
                        "event_id": event_row["event_id"],
                        "symbol": symbol,
                        "session_date": session_date,
                        "reason": "no_controls_after_rv_fallback",
                    }
                )
                continue
            pool = pool.sort_values("anchor_ts").head(config.control_set_size)
            for idx, control in enumerate(pool.itertuples(index=False), start=1):
                rows.append(
                    {
                        "control_id": f"{event_row['event_id']}_c{idx:02d}",
                        "event_id": event_row["event_id"],
                        "symbol": symbol,
                        "session_date": session_date,
                        "tc": pd.Timestamp(control.anchor_ts).tz_convert("UTC"),
                        "baseline_rv": float(control.baseline_rv),
                        "baseline_volume": float(control.baseline_volume),
                        "rv_q": int(control.rv_q),
                        "vol_q": int(control.vol_q) if pd.notna(control.vol_q) else np.nan,
                        "event_rv_q": event_rv_q,
                        "event_vol_q": event_vol_q,
                        "match_tier": match_tier,
                    }
                )

    out = pd.DataFrame(rows).sort_values(["event_id", "tc"]).reset_index(drop=True) if rows else pd.DataFrame(
        columns=[
            "control_id",
            "event_id",
            "symbol",
            "session_date",
            "tc",
            "baseline_rv",
            "baseline_volume",
            "rv_q",
            "vol_q",
            "event_rv_q",
            "event_vol_q",
            "match_tier",
        ]
    )
    out.attrs["no_controls"] = no_controls
    return out
