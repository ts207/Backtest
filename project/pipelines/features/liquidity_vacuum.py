"""
Liquidity Vacuum / Thin Book Expansion feature definitions.

This module provides utilities to detect liquidity vacuum events – episodes where
an exogenous price impulse is followed by a short window of thin order book
conditions (low traded volume and enlarged range).  The detection logic is
inspired by the vol‑shock→relaxation framework but adapted to use basic
liquidity proxies instead of realised volatility.

Liquidity vacuum events are defined as follows:

* A **shock** bar is identified when the absolute return of a bar exceeds a
  calibrated threshold (quantile of historical absolute returns).
* Starting from the bar immediately after the shock, a **vacuum** persists as
  long as two conditions hold:
    - **Low volume ratio:** the bar's volume divided by the rolling median
      volume over ``volume_window`` bars is below ``vol_ratio_floor``.
    - **Expanded range:** the bar's intrabar range (high – low) divided by
      close is greater than ``range_multiplier`` times the rolling median range
      over ``range_window`` bars.
* If at least ``min_vacuum_bars`` consecutive bars satisfy both conditions,
  an event is declared.  The event extends until the conditions fail or
  ``max_vacuum_bars`` is reached.  A cooldown period (``cooldown_bars``) is
  enforced after each event to prevent overlapping detections.

For each event, several descriptive metrics are produced, including the
duration, peak range, half‑life of the range decay, a flag for secondary
shocks within a post horizon, and the cumulative excess range relative to
baseline conditions.  These metrics can be used in Phase 1 research to
evaluate structural properties and, later, in Phase 2 to derive conditional
edges.

The module also provides a calibration routine to select a shock threshold
based on quantile sweeps and a minimum event count.

Note: This detection is deliberately simple.  It avoids any look‑ahead or
parameter sweeps beyond the quantile selection, thereby minimising risk of
overfitting.  Users wishing to refine the definition should clone this
module and adjust thresholds or logic accordingly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

# Version identifier for liquidity vacuum definition.  Increment this when
# making backwards‑incompatible changes to the detection logic or metrics.
LV_DEF_VERSION = "v1"


@dataclass(frozen=True)
class LiquidityVacuumConfig:
    """Configuration parameters for liquidity vacuum event detection."""

    # Definition version string written into event IDs
    def_version: str = LV_DEF_VERSION
    # Quantile of absolute returns used for shock identification
    shock_quantile: float = 0.99
    # Rolling window (bars) used to compute median volume
    volume_window: int = 480
    # Rolling window (bars) used to compute median range
    range_window: int = 480
    # Volume must be less than this fraction of rolling median volume to qualify as thin
    vol_ratio_floor: float = 0.5
    # Range must exceed this multiple of rolling median range to qualify as expanded
    range_multiplier: float = 1.5
    # Minimum consecutive vacuum bars required to register an event
    min_vacuum_bars: int = 3
    # Maximum number of bars allowed in a single event
    max_vacuum_bars: int = 96
    # Cooldown period after each event during which new events are not detected
    cooldown_bars: int = 12
    # Horizon over which post‑event metrics (secondary shock and half‑life) are measured
    post_horizon_bars: int = 96
    # Horizon used for area‑under‑curve calculations
    auc_horizon_bars: int = 96
    # Threshold on range expansion used to define secondary shock events
    range_expansion_threshold: float = 0.02


DEFAULT_LV_CONFIG = LiquidityVacuumConfig()


def _compute_core_series(df: pd.DataFrame, cfg: LiquidityVacuumConfig) -> pd.DataFrame:
    """
    Add basic return, volume and range series required for event detection.

    The returned frame contains:

    - ``timestamp`` (timezone‑aware)
    - ``close``, ``high``, ``low``, ``volume`` (converted to float)
    - ``abs_return``: absolute simple return between consecutive closes
    - ``vol_med``: rolling median of volume
    - ``vol_ratio``: volume divided by ``vol_med``
    - ``range_pct``: (high − low) / close
    - ``range_med``: rolling median of ``range_pct``

    Parameters
    ----------
    df : pd.DataFrame
        Input bars with columns ``timestamp``, ``close``, ``high``, ``low``, ``volume``.
    cfg : LiquidityVacuumConfig
        Detection parameters.

    Returns
    -------
    pd.DataFrame
        A copy of the input with additional columns.
    """
    out = df.copy()
    # Ensure timestamps are timezone‑aware and sorted
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True)
    out = out.sort_values("timestamp").reset_index(drop=True)

    # Cast numeric series to float for safety
    close = out["close"].astype(float)
    high = out["high"].astype(float)
    low = out["low"].astype(float)
    volume = out["volume"].astype(float)

    # Absolute simple return between consecutive closes
    out["abs_return"] = (close / close.shift(1) - 1.0).abs()
    # Rolling medians; require full window to avoid lookahead bias
    out["vol_med"] = volume.rolling(cfg.volume_window, min_periods=cfg.volume_window).median()
    out["vol_ratio"] = volume / out["vol_med"]
    # Intrabar range as fraction of close; avoid divide by zero with NaN
    out["range_pct"] = (high - low) / close.replace(0.0, np.nan)
    out["range_med"] = out["range_pct"].rolling(cfg.range_window, min_periods=cfg.range_window).median()
    return out


def _shock_threshold(series: pd.Series, quantile: float) -> float:
    """
    Compute a finite quantile of a series, ignoring infinities and NaNs.

    Parameters
    ----------
    series : pd.Series
        Numeric series for which to compute the quantile.
    quantile : float
        Desired quantile (between 0 and 1).

    Returns
    -------
    float
        Quantile value or NaN if series is empty/invalid.
    """
    clean = series.replace([np.inf, -np.inf], np.nan).dropna()
    if clean.empty:
        return np.nan
    return float(clean.quantile(quantile))


def _detect_events_with_threshold(
    df: pd.DataFrame,
    symbol: str,
    config: LiquidityVacuumConfig,
    t_shock: float,
) -> pd.DataFrame:
    """
    Detect liquidity vacuum events given a calibrated shock threshold.

    This routine scans through the core series and identifies impulse bars
    (absolute return ≥ ``t_shock``) that transition from below threshold to
    above threshold.  Following each impulse, it checks consecutive bars
    for thin liquidity conditions defined by low volume ratio and high range.

    If at least ``min_vacuum_bars`` consecutive vacuum bars are observed,
    an event is recorded.  The event ends when either condition fails or
    ``max_vacuum_bars`` is reached.  A cooldown period prevents overlapping
    events.  Various metrics (duration, peak range, half‑life, etc.) are
    computed per event.

    Parameters
    ----------
    df : pd.DataFrame
        Core series with computed columns from ``_compute_core_series``.
    symbol : str
        Symbol identifier for event ids.
    config : LiquidityVacuumConfig
        Event detection configuration.
    t_shock : float
        Threshold on ``abs_return`` used to identify impulse bars.  Must be finite.

    Returns
    -------
    pd.DataFrame
        Event rows with computed metrics.  Empty DataFrame if no events.
    """
    if not np.isfinite(t_shock):
        return pd.DataFrame()

    event_rows: List[Dict[str, object]] = []
    n = len(df)
    # Start from second bar (index 1) because return uses previous bar
    i = 1
    cooldown_until = -1
    event_num = 0

    while i < n:
        # Skip bars during cooldown
        if i <= cooldown_until:
            i += 1
            continue

        # Identify impulse onset: return crosses threshold from below to above
        ret_now = float(df["abs_return"].iat[i]) if pd.notna(df["abs_return"].iat[i]) else np.nan
        ret_prev = float(df["abs_return"].iat[i - 1]) if pd.notna(df["abs_return"].iat[i - 1]) else np.nan
        onset = np.isfinite(ret_now) and np.isfinite(ret_prev) and (ret_now >= t_shock) and (ret_prev < t_shock)
        if not onset:
            i += 1
            continue

        # Scan for consecutive vacuum bars immediately after the impulse
        start_idx = i + 1
        vac_len = 0
        end_idx: Optional[int] = None
        j = start_idx
        while j < n and vac_len < config.max_vacuum_bars:
            vol_ratio = float(df["vol_ratio"].iat[j]) if pd.notna(df["vol_ratio"].iat[j]) else np.nan
            range_med = float(df["range_med"].iat[j]) if pd.notna(df["range_med"].iat[j]) else np.nan
            range_pct = float(df["range_pct"].iat[j]) if pd.notna(df["range_pct"].iat[j]) else np.nan
            # Thin liquidity conditions: low volume and high range
            low_vol = np.isfinite(vol_ratio) and (vol_ratio < config.vol_ratio_floor)
            high_range = False
            if np.isfinite(range_med) and np.isfinite(range_pct):
                high_range = range_pct > config.range_multiplier * range_med
            if low_vol and high_range:
                vac_len += 1
                end_idx = j
                j += 1
            else:
                break
        # Record event if minimum length achieved
        if end_idx is not None and vac_len >= config.min_vacuum_bars:
            event_num += 1
            event_id = f"lv_{config.def_version}_{symbol}_{event_num:06d}"
            enter = start_idx
            exit_idx = end_idx
            duration = exit_idx - enter + 1
            # Compute basic metrics
            window = df.iloc[enter : exit_idx + 1]
            # Range time series within event
            rages = window["range_pct"].astype(float)
            # Peak range and timing relative to event start
            max_range = float(rages.max()) if not rages.empty else np.nan
            t_range_peak = int(rages.idxmax() - enter) if not rages.empty else -1
            # Half‑life: time until range decays to half its peak above baseline
            half_life = None
            if np.isfinite(max_range):
                baseline = float(window["range_med"].iat[0]) if pd.notna(window["range_med"].iat[0]) else 0.0
                target = baseline + 0.5 * (max_range - baseline)
                for k in range(len(rages)):
                    val = float(rages.iat[k]) if pd.notna(rages.iat[k]) else np.nan
                    if np.isfinite(val) and val <= target:
                        half_life = k
                        break
            # Post‑event metrics measured over fixed horizon after event end
            post_start = exit_idx + 1
            post_end = min(n - 1, exit_idx + config.post_horizon_bars)
            post = df.iloc[post_start : post_end + 1]
            # Secondary shock: flag if any return exceeds threshold in post horizon
            p_secondary = int((post["abs_return"] >= t_shock).any()) if not post.empty else 0
            # Secondary range expansion: flag if range expands beyond threshold in post horizon
            p_range_expansion = int((post["range_pct"] >= config.range_expansion_threshold).any()) if not post.empty else 0
            # Cumulative excess range (AUC) relative to baseline over fixed horizon
            auc_horizon_end = min(n - 1, exit_idx + config.auc_horizon_bars)
            auc_window = df.iloc[enter : auc_horizon_end + 1]
            baseline_range = float(window["range_med"].iat[0]) if pd.notna(window["range_med"].iat[0]) else 0.0
            excess = (auc_window["range_pct"] - baseline_range).clip(lower=0.0)
            auc_excess = float(excess.sum()) if not excess.empty else 0.0

            event_rows.append(
                {
                    "event_id": event_id,
                    "symbol": symbol,
                    "enter_idx": int(enter),
                    "exit_idx": int(exit_idx),
                    "enter_ts": df["timestamp"].iat[enter],
                    "exit_ts": df["timestamp"].iat[exit_idx],
                    "duration_bars": int(duration),
                    "max_range_pct": max_range,
                    "t_range_peak": int(t_range_peak),
                    "half_life": int(half_life) if half_life is not None else np.nan,
                    "secondary_shock": p_secondary,
                    "secondary_range_expansion": p_range_expansion,
                    "auc_excess_range": auc_excess,
                }
            )
            # Apply cooldown after event
            cooldown_until = exit_idx + config.cooldown_bars
            # Continue scanning from end of cooldown
            i = cooldown_until + 1
            continue
        # If no event, move to next bar
        i += 1
    if not event_rows:
        return pd.DataFrame()
    return pd.DataFrame(event_rows)


def calibrate_shock_threshold(
    df: pd.DataFrame,
    symbol: str,
    cfg: LiquidityVacuumConfig = DEFAULT_LV_CONFIG,
    quantiles: Sequence[float] = (0.95, 0.97, 0.98, 0.99, 0.995),
    min_events: int = 10,
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    """
    Sweep a range of quantiles to select a shock threshold with sufficient events.

    This helper computes the shock threshold for each candidate quantile and
    counts the number of events detected with that threshold.  The smallest
    quantile that yields at least ``min_events`` events is selected.  A
    summary table of candidate thresholds is returned along with the
    selected entry (including ``symbol`` and ``selected_t_shock`` fields).

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned bars containing at least ``timestamp`` and ``close``.
    symbol : str
        Symbol identifier for summary table.
    cfg : LiquidityVacuumConfig, optional
        Configuration used for event detection.
    quantiles : Sequence[float], optional
        Candidate quantiles to evaluate.
    min_events : int, optional
        Minimum number of events required to accept a threshold.

    Returns
    -------
    Tuple[pd.DataFrame, Dict[str, object]]
        (table of thresholds, selected row dict)
    """
    core = _compute_core_series(df, cfg)
    rows: List[Dict[str, object]] = []
    for q in quantiles:
        t_shock = _shock_threshold(core["abs_return"], q)
        events = _detect_events_with_threshold(core, symbol, cfg, t_shock)
        rows.append(
            {
                "symbol": symbol,
                "shock_quantile": q,
                "t_shock": t_shock,
                "event_count": int(len(events)),
                "min_events": int(min_events),
                "meets_min_events": bool(len(events) >= min_events),
            }
        )
    dfq = pd.DataFrame(rows)
    # Select first quantile meeting min_events
    sel = None
    for row in rows:
        if row["meets_min_events"]:
            sel = row
            break
    if sel is None:
        # fall back to highest quantile
        sel = rows[-1] if rows else {
            "symbol": symbol,
            "shock_quantile": np.nan,
            "t_shock": np.nan,
            "event_count": 0,
            "min_events": min_events,
            "meets_min_events": False,
        }
    # Append selected threshold info
    sel = sel.copy()
    sel["selected_quantile"] = sel["shock_quantile"]
    sel["selected_t_shock"] = sel["t_shock"]
    sel["selected_event_count"] = sel["event_count"]
    return dfq, sel


def detect_liquidity_vacuum_events(
    df: pd.DataFrame,
    symbol: str,
    cfg: LiquidityVacuumConfig = DEFAULT_LV_CONFIG,
    t_shock: Optional[float] = None,
) -> pd.DataFrame:
    """
    High‑level wrapper to detect liquidity vacuum events on a bar series.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned bars containing at least ``timestamp``, ``close``, ``high``, ``low``, ``volume``.
    symbol : str
        Symbol identifier used in event IDs.
    cfg : LiquidityVacuumConfig, optional
        Configuration for event detection.
    t_shock : float, optional
        Precomputed shock threshold.  If None, ``cfg.shock_quantile`` is used
        to compute a threshold on the fly.

    Returns
    -------
    pd.DataFrame
        Detected events with metrics.  Empty DataFrame if no events.
    """
    core = _compute_core_series(df, cfg)
    # Use provided threshold or compute from quantile
    threshold = float(t_shock) if t_shock is not None else _shock_threshold(core["abs_return"], cfg.shock_quantile)
    return _detect_events_with_threshold(core, symbol, cfg, threshold)
