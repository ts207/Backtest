from __future__ import annotations

import numpy as np
import pandas as pd

from .config_v2 import ABMAFundingConfig, DEFAULT_ABMA_FUNDING_CONFIG
from .validators import REQUIRED_QUOTES_COLUMNS, REQUIRED_TRADES_COLUMNS, ensure_columns, ensure_tz_aware


def _sample_quote_grid(symbol_quotes: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
    grid = pd.date_range(start=start_ts, end=end_ts, freq="1s", tz="UTC")
    if grid.empty:
        return pd.DataFrame(columns=["ts", "bid_px", "ask_px", "bid_sz", "ask_sz"])
    grid_df = pd.DataFrame({"ts": grid})
    asof_quotes = pd.merge_asof(grid_df, symbol_quotes.sort_values("ts"), on="ts", direction="backward")
    asof_quotes["mid"] = (asof_quotes["bid_px"].astype(float) + asof_quotes["ask_px"].astype(float)) / 2.0
    asof_quotes["spread"] = (asof_quotes["ask_px"].astype(float) - asof_quotes["bid_px"].astype(float)).clip(lower=0.0)
    valid_mid = asof_quotes["mid"].where(asof_quotes["mid"] > 0.0)
    asof_quotes["eff_spread"] = asof_quotes["spread"] / valid_mid
    denom = asof_quotes["bid_sz"].astype(float) + asof_quotes["ask_sz"].astype(float)
    micro = (asof_quotes["ask_px"].astype(float) * asof_quotes["bid_sz"].astype(float) + asof_quotes["bid_px"].astype(float) * asof_quotes["ask_sz"].astype(float)) / denom.replace(0.0, np.nan)
    asof_quotes["microprice"] = micro.where(micro > 0.0)
    asof_quotes["micro_dev"] = (asof_quotes["microprice"] - asof_quotes["mid"]).abs() / valid_mid
    return asof_quotes


def _spread_half_life(series: pd.Series) -> float:
    valid = series.dropna()
    if valid.empty:
        return np.nan
    start = float(valid.iloc[0])
    target = float(valid.median())
    if not np.isfinite(start) or not np.isfinite(target):
        return np.nan
    if start <= target:
        return 0.0
    for i, value in enumerate(valid):
        if value <= target:
            return float(i)
    return np.nan


def _microprice_decay_rate(microprice: pd.Series, half_life_seconds: int) -> float:
    valid = microprice.dropna()
    if len(valid) < 3:
        return np.nan
    log_ret = np.log(valid).diff()
    var_series = log_ret.ewm(halflife=float(max(1, half_life_seconds)), adjust=False).var(bias=False).replace(0.0, np.nan).dropna()
    if len(var_series) < 3:
        return np.nan
    x = np.arange(len(var_series), dtype=float)
    y = np.log(var_series.to_numpy())
    slope, _ = np.polyfit(x, y, deg=1)
    return float(-slope)


def _trade_sign_entropy(symbol_trades: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp, n_trades: int) -> float:
    t = symbol_trades[(symbol_trades["ts"] >= start_ts) & (symbol_trades["ts"] <= end_ts)][["price"]].copy()
    if len(t) < n_trades:
        return np.nan
    price_diff = t["price"].astype(float).diff()
    signs = np.sign(price_diff).replace(0.0, np.nan).ffill().fillna(0.0)
    entropies: list[float] = []
    for idx in range(n_trades - 1, len(signs)):
        window = signs.iloc[idx - n_trades + 1 : idx + 1]
        p_pos = float((window > 0).mean())
        p_neg = float((window < 0).mean())
        h = 0.0
        for p in (p_pos, p_neg):
            if p > 0:
                h -= p * np.log2(p)
        entropies.append(h)
    if not entropies:
        return np.nan
    return float(np.mean(entropies))


def _anchor_metrics(
    symbol_trades: pd.DataFrame,
    symbol_quotes: pd.DataFrame,
    anchor_ts: pd.Timestamp,
    *,
    config: ABMAFundingConfig,
) -> tuple[dict[str, float], pd.DataFrame]:
    end_ts = anchor_ts + pd.Timedelta(minutes=config.event_window_minutes)
    sampled = _sample_quote_grid(symbol_quotes, anchor_ts, end_ts)
    spread_half_life = _spread_half_life(sampled["eff_spread"])
    micro_decay = _microprice_decay_rate(sampled["microprice"], config.microprice_half_life_seconds)
    entropy_mean = _trade_sign_entropy(symbol_trades, anchor_ts, end_ts, config.trade_sign_entropy_trades)
    return (
        {
            "spread_half_life_sec": spread_half_life,
            "microprice_var_decay_rate": micro_decay,
            "trade_sign_entropy_mean": entropy_mean,
        },
        sampled,
    )


def compute_structural_metrics(
    events: pd.DataFrame,
    controls: pd.DataFrame,
    trades: pd.DataFrame,
    quotes: pd.DataFrame,
    *,
    config: ABMAFundingConfig = DEFAULT_ABMA_FUNDING_CONFIG,
) -> dict[str, pd.DataFrame]:
    """Compute v2 funding structural metrics for events and matched controls."""
    ensure_columns(events, {"event_id", "symbol", "session_date", "t0", "window_end", "abma_def_version"}, "events")
    ensure_columns(controls, {"control_id", "event_id", "symbol", "tc"}, "controls")
    ensure_columns(trades, REQUIRED_TRADES_COLUMNS, "trades")
    ensure_columns(quotes, REQUIRED_QUOTES_COLUMNS, "quotes")

    events_local = events.copy()
    controls_local = controls.copy()
    trades_local = trades.copy()
    quotes_local = quotes.copy()

    events_local["t0"] = ensure_tz_aware(events_local["t0"], "events.t0")
    events_local["window_end"] = ensure_tz_aware(events_local["window_end"], "events.window_end")
    controls_local["tc"] = ensure_tz_aware(controls_local["tc"], "controls.tc")
    trades_local["ts"] = ensure_tz_aware(trades_local["ts"], "trades.ts")
    quotes_local["ts"] = ensure_tz_aware(quotes_local["ts"], "quotes.ts")
    trades_local["price"] = pd.to_numeric(trades_local["price"], errors="coerce")

    curve_rows: list[dict[str, object]] = []
    event_rows: list[dict[str, object]] = []
    control_summary_rows: list[dict[str, float]] = []
    delta_rows: list[dict[str, float]] = []

    for event in events_local.sort_values(["symbol", "t0"]).itertuples(index=False):
        symbol_trades = trades_local[trades_local["symbol"].astype(str) == str(event.symbol)][["ts", "price", "size"]]
        symbol_quotes = quotes_local[quotes_local["symbol"].astype(str) == str(event.symbol)][["ts", "bid_px", "ask_px", "bid_sz", "ask_sz"]]
        event_metrics, sampled_event = _anchor_metrics(symbol_trades, symbol_quotes, pd.Timestamp(event.t0).tz_convert("UTC"), config=config)
        event_curves = sampled_event.reset_index(drop=True)
        event_curves["tau_s"] = np.arange(len(event_curves))
        for row in event_curves.itertuples(index=False):
            curve_rows.append(
                {
                    "event_id": event.event_id,
                    "control_id": None,
                    "symbol": event.symbol,
                    "session_date": event.session_date,
                    "anchor_kind": "event",
                    "tau_s": int(row.tau_s),
                    "eff_spread": float(row.eff_spread) if pd.notna(row.eff_spread) else np.nan,
                    "micro_dev": float(row.micro_dev) if pd.notna(row.micro_dev) else np.nan,
                    "is_valid": bool(pd.notna(row.eff_spread)),
                }
            )

        event_controls = controls_local[controls_local["event_id"] == event.event_id].copy()
        control_metrics_rows: list[dict[str, float]] = []
        for control in event_controls.sort_values("tc").itertuples(index=False):
            metrics, sampled_control = _anchor_metrics(symbol_trades, symbol_quotes, pd.Timestamp(control.tc).tz_convert("UTC"), config=config)
            control_metrics_rows.append(metrics)
            control_curves = sampled_control.reset_index(drop=True)
            control_curves["tau_s"] = np.arange(len(control_curves))
            for crow in control_curves.itertuples(index=False):
                curve_rows.append(
                    {
                        "event_id": event.event_id,
                        "control_id": control.control_id,
                        "symbol": event.symbol,
                        "session_date": event.session_date,
                        "anchor_kind": "control",
                        "tau_s": int(crow.tau_s),
                        "eff_spread": float(crow.eff_spread) if pd.notna(crow.eff_spread) else np.nan,
                        "micro_dev": float(crow.micro_dev) if pd.notna(crow.micro_dev) else np.nan,
                        "is_valid": bool(pd.notna(crow.eff_spread)),
                    }
                )

        controls_used = int(len(control_metrics_rows))
        if control_metrics_rows:
            control_df = pd.DataFrame(control_metrics_rows)
            control_mean = control_df.mean(numeric_only=True)
            control_spread = float(control_mean.get("spread_half_life_sec", np.nan))
            control_decay = float(control_mean.get("microprice_var_decay_rate", np.nan))
            control_entropy = float(control_mean.get("trade_sign_entropy_mean", np.nan))
        else:
            control_spread = np.nan
            control_decay = np.nan
            control_entropy = np.nan

        event_rows.append(
            {
                "event_id": event.event_id,
                "symbol": event.symbol,
                "session_date": event.session_date,
                "t0": event.t0,
                "window_end": event.window_end,
                "abma_def_version": event.abma_def_version,
                "spread_half_life_sec": event_metrics["spread_half_life_sec"],
                "microprice_var_decay_rate": event_metrics["microprice_var_decay_rate"],
                "trade_sign_entropy_mean": event_metrics["trade_sign_entropy_mean"],
                "controls_used": controls_used,
            }
        )
        control_summary_rows.append(
            {
                "control_spread_half_life_mean": control_spread,
                "control_microprice_var_decay_mean": control_decay,
                "control_trade_sign_entropy_mean": control_entropy,
            }
        )
        delta_rows.append(
            {
                "delta_spread_half_life_sec": float(event_metrics["spread_half_life_sec"] - control_spread)
                if np.isfinite(event_metrics["spread_half_life_sec"]) and np.isfinite(control_spread)
                else np.nan,
                "delta_microprice_var_decay_rate": float(event_metrics["microprice_var_decay_rate"] - control_decay)
                if np.isfinite(event_metrics["microprice_var_decay_rate"]) and np.isfinite(control_decay)
                else np.nan,
                "delta_trade_sign_entropy_mean": float(event_metrics["trade_sign_entropy_mean"] - control_entropy)
                if np.isfinite(event_metrics["trade_sign_entropy_mean"]) and np.isfinite(control_entropy)
                else np.nan,
            }
        )

    event_df = pd.DataFrame(event_rows)
    control_summary_df = pd.DataFrame(control_summary_rows)
    delta_df = pd.DataFrame(delta_rows)
    curves_df = pd.DataFrame(curve_rows)
    return {
        "event": event_df,
        "control_summary": control_summary_df,
        "delta": delta_df,
        "curves": curves_df,
    }
