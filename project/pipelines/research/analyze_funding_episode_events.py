from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir, list_parquet_files, read_parquet

EVENT_TYPES = [
    "funding_extreme_onset",
    "funding_acceleration",
    "funding_persistence_window",
    "funding_normalization",
]
PRIMARY_EVENT_TYPES = [
    "funding_extreme_onset",
    "funding_persistence_window",
    "funding_normalization",
]


def _rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    def pct_rank(values: np.ndarray) -> float:
        valid = values[~np.isnan(values)]
        if len(valid) == 0:
            return np.nan
        last = values[-1]
        if np.isnan(last):
            return np.nan
        return float(np.sum(valid <= last) / len(valid) * 100.0)

    return series.rolling(window=window, min_periods=window).apply(pct_rank, raw=True)


def _pick_window_column(columns: Iterable[str], prefix: str, fallback: str) -> str:
    candidates: List[str] = []
    for col in columns:
        if col.startswith(prefix):
            candidates.append(col)
    if candidates:
        return sorted(candidates, key=lambda c: int(c.split("_")[-1]))[0]
    if fallback in columns:
        return fallback
    raise ValueError(f"Missing required column prefix={prefix} fallback={fallback}")


def _load_features(symbol: str) -> pd.DataFrame:
    features_dir = DATA_ROOT / "lake" / "features" / "perp" / symbol / "15m" / "features_v1"
    files = list_parquet_files(features_dir)
    if not files:
        raise ValueError(f"No feature partitions found for {symbol}: {features_dir}")
    df = read_parquet(files)
    if df.empty:
        raise ValueError(f"Empty features for {symbol}")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df.sort_values("timestamp").reset_index(drop=True)


def _enforce_spacing(indices: List[int], min_spacing: int) -> List[int]:
    kept: List[int] = []
    last = -10**9
    for idx in sorted(indices):
        if idx - last >= min_spacing:
            kept.append(idx)
            last = idx
    return kept


def _extract_event_indices(
    df: pd.DataFrame,
    extreme_pct: float,
    accel_pct: float,
    accel_lookback: int,
    persistence_pct: float,
    persistence_bars: int,
    normalization_pct: float,
    normalization_lookback: int,
    min_event_spacing: int,
) -> Dict[str, List[int]]:
    f_pct = df["funding_abs_pct"].astype(float)
    f_abs = df["funding_abs"].astype(float)

    extreme_raw = ((f_pct >= extreme_pct) & (f_pct.shift(1) < extreme_pct)).fillna(False)

    accel = f_abs - f_abs.shift(accel_lookback)
    accel_pos = accel.where(accel > 0)
    accel_rank = _rolling_percentile(accel_pos.astype(float), window=2880)
    accel_raw = ((accel_rank >= accel_pct) & (accel_rank.shift(1) < accel_pct)).fillna(False)

    high = (f_pct >= persistence_pct).fillna(False).astype(int)
    run_len = high.groupby((high == 0).cumsum()).cumsum()
    persistence_raw = ((high == 1) & (run_len == persistence_bars)).fillna(False)

    recent_extreme = (
        (f_pct >= extreme_pct)
        .rolling(window=normalization_lookback, min_periods=1)
        .max()
        .fillna(0)
        .astype(bool)
    )
    normalization_raw = (
        (f_pct <= normalization_pct)
        & (f_pct.shift(1) > normalization_pct)
        & recent_extreme
    ).fillna(False)

    raw_map = {
        EVENT_TYPES[0]: np.flatnonzero(extreme_raw.values).tolist(),
        EVENT_TYPES[1]: np.flatnonzero(accel_raw.values).tolist(),
        EVENT_TYPES[2]: np.flatnonzero(persistence_raw.values).tolist(),
        EVENT_TYPES[3]: np.flatnonzero(normalization_raw.values).tolist(),
    }
    return {k: _enforce_spacing(v, min_event_spacing) for k, v in raw_map.items()}


def _event_metrics(
    df: pd.DataFrame,
    idx: int,
    vol_burst_threshold: float,
    event_lookahead: int,
    liquidation_abs_return: float,
    drawdown_abs_return: float,
    normalization_pct: float,
    pre_window: int,
    post_window: int,
) -> Dict[str, float]:
    close = df["close"].astype(float)
    rv = df["rv_96"].astype(float)
    f_pct = df["funding_abs_pct"].astype(float)

    future_end = min(len(df), idx + 1 + event_lookahead)
    future = df.iloc[idx + 1 : future_end]
    if future.empty:
        return {}

    base_close = close.iloc[idx]
    fwd_ret = future["close"].astype(float) / base_close - 1.0
    liq_hits = np.flatnonzero((fwd_ret.abs().values >= liquidation_abs_return))
    vol_hits = np.flatnonzero((future["rv_96"].astype(float).values >= vol_burst_threshold))
    liq_hit_bar = float(liq_hits[0] + 1) if len(liq_hits) else np.nan
    vol_hit_bar = float(vol_hits[0] + 1) if len(vol_hits) else np.nan

    liquidation_prob = float(len(liq_hits) > 0)
    vol_burst_prob = float(len(vol_hits) > 0)

    win_30_80 = fwd_ret.iloc[29:80] if len(fwd_ret) >= 30 else pd.Series(dtype=float)
    liq_30_80_hit = float((win_30_80.abs() >= liquidation_abs_return).any()) if not win_30_80.empty else 0.0
    drawdown_30_80_hit = float((win_30_80 <= -drawdown_abs_return).any()) if not win_30_80.empty else 0.0

    min_ret = float(fwd_ret.min()) if len(fwd_ret) else np.nan
    mae_96 = float(max(0.0, -min_ret)) if np.isfinite(min_ret) else np.nan
    realized_vol_mean_96 = float(future["rv_96"].astype(float).mean()) if len(future) else np.nan

    norm_hits = np.flatnonzero((f_pct.iloc[idx + 1 :] <= normalization_pct).values)
    if len(norm_hits) > 0:
        norm_idx = int(idx + 1 + norm_hits[0])
        time_to_norm = float(norm_idx - idx)
    else:
        norm_idx = None
        time_to_norm = np.nan

    pre_slice = rv.iloc[max(0, idx - pre_window) : idx]
    pre_vol = float(pre_slice.mean()) if len(pre_slice) > 0 else np.nan
    post_start = (norm_idx + 1) if norm_idx is not None else (idx + 1)
    post_slice = rv.iloc[post_start : min(len(df), post_start + post_window)]
    post_vol = float(post_slice.mean()) if len(post_slice) > 0 else np.nan

    if np.isfinite(pre_vol) and pre_vol > 0 and np.isfinite(post_vol):
        post_vol_decay = float((pre_vol - post_vol) / pre_vol)
    else:
        post_vol_decay = np.nan

    return {
        "liquidation_prob": liquidation_prob,
        "vol_burst_prob": vol_burst_prob,
        "liq_hit_bar": liq_hit_bar,
        "vol_hit_bar": vol_hit_bar,
        "liq_30_80_hit": liq_30_80_hit,
        "drawdown_30_80_hit": drawdown_30_80_hit,
        "mae_96": mae_96,
        "realized_vol_mean_96": realized_vol_mean_96,
        "effective_horizon": float(future_end - (idx + 1)),
        "time_to_normalization_bars": time_to_norm,
        "post_event_vol_decay": post_vol_decay,
        "pre_vol": pre_vol,
        "post_vol": post_vol,
    }


def _summarize_events(events_df: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
    if events_df.empty:
        return pd.DataFrame()
    grouped = events_df.groupby(keys, dropna=False)
    out = grouped.agg(
        event_count=("event_idx", "count"),
        liquidation_probability=("liquidation_prob", "mean"),
        vol_burst_likelihood=("vol_burst_prob", "mean"),
        time_to_normalization_median=("time_to_normalization_bars", "median"),
        time_to_normalization_p75=("time_to_normalization_bars", lambda s: s.quantile(0.75)),
        post_event_vol_decay_mean=("post_event_vol_decay", "mean"),
        post_event_vol_decay_median=("post_event_vol_decay", "median"),
    ).reset_index()
    return out.sort_values(keys).reset_index(drop=True)


def _inject_missing_event_types(summary_df: pd.DataFrame, symbols: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    num_cols = [
        "event_count",
        "liquidation_probability",
        "vol_burst_likelihood",
        "time_to_normalization_median",
        "time_to_normalization_p75",
        "post_event_vol_decay_mean",
        "post_event_vol_decay_median",
    ]

    sym_rows: List[Dict[str, object]] = []
    for symbol in symbols:
        for et in EVENT_TYPES:
            row = {"symbol": symbol, "event_type": et}
            row.update({c: 0.0 for c in num_cols})
            row["event_count"] = 0
            sym_rows.append(row)
    full_sym = pd.DataFrame(sym_rows)
    if not summary_df.empty:
        full_sym = full_sym.merge(summary_df, on=["symbol", "event_type"], how="left", suffixes=("", "_x"))
        for col in num_cols:
            if f"{col}_x" in full_sym.columns:
                full_sym[col] = full_sym[f"{col}_x"].where(full_sym[f"{col}_x"].notna(), full_sym[col])
                full_sym = full_sym.drop(columns=[f"{col}_x"])
    full_sym = full_sym.sort_values(["symbol", "event_type"]).reset_index(drop=True)

    pooled_rows: List[Dict[str, object]] = []
    for et in EVENT_TYPES:
        row = {"event_type": et}
        row.update({c: 0.0 for c in num_cols})
        row["event_count"] = 0
        pooled_rows.append(row)
    full_pooled = pd.DataFrame(pooled_rows)

    if not summary_df.empty:
        pooled_actual = (
            summary_df.groupby("event_type", as_index=False)
            .agg(
                event_count=("event_count", "sum"),
                liquidation_probability=("liquidation_probability", "mean"),
                vol_burst_likelihood=("vol_burst_likelihood", "mean"),
                time_to_normalization_median=("time_to_normalization_median", "mean"),
                time_to_normalization_p75=("time_to_normalization_p75", "mean"),
                post_event_vol_decay_mean=("post_event_vol_decay_mean", "mean"),
                post_event_vol_decay_median=("post_event_vol_decay_median", "mean"),
            )
        )
        full_pooled = full_pooled.merge(pooled_actual, on="event_type", how="left", suffixes=("", "_x"))
        for col in num_cols:
            if f"{col}_x" in full_pooled.columns:
                full_pooled[col] = full_pooled[f"{col}_x"].where(full_pooled[f"{col}_x"].notna(), full_pooled[col])
                full_pooled = full_pooled.drop(columns=[f"{col}_x"])
    full_pooled = full_pooled.sort_values(["event_type"]).reset_index(drop=True)
    return full_sym, full_pooled


def _safe_float(value: object) -> float:
    if value is None:
        return float("nan")
    try:
        out = float(value)
    except Exception:
        return float("nan")
    return out


def _build_match_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["tod_slot"] = out["timestamp"].dt.hour * 4 + (out["timestamp"].dt.minute // 15)
    out["vol_q"] = pd.qcut(out["rv_96"].astype(float), q=4, labels=["Q1", "Q2", "Q3", "Q4"], duplicates="drop")
    return out


def _build_regime_fields(df: pd.DataFrame, rv_pct_col: str, htf_window: int, htf_lookback: int) -> pd.DataFrame:
    out = df.copy()
    close = out["close"].astype(float)
    htf_base = close.rolling(window=htf_window, min_periods=htf_window).mean()
    htf_delta = htf_base - htf_base.shift(htf_lookback)
    out["bull_bear"] = np.select([htf_delta > 0, htf_delta < 0], ["bull", "bear"], default="flat")

    rv_pct = out[rv_pct_col].astype(float)
    out["vol_regime"] = np.where(rv_pct.isna(), "na", np.where(rv_pct >= 50.0, "high", "low"))
    return out


def _build_blocked_mask(n: int, event_indices: List[int], event_lookahead: int) -> np.ndarray:
    blocked = np.zeros(n, dtype=bool)
    for idx in event_indices:
        start = max(0, idx)
        end = min(n, idx + 1 + event_lookahead)
        blocked[start:end] = True
    return blocked


def _sample_controls_for_event(
    candidate_df: pd.DataFrame,
    vol_q: str,
    tod_slot: int,
    n_samples: int,
    rng: np.random.Generator,
) -> np.ndarray:
    exact = candidate_df[(candidate_df["vol_q"] == vol_q) & (candidate_df["tod_slot"] == tod_slot)]["idx"].to_numpy()
    if len(exact) >= 1:
        return rng.choice(exact, size=n_samples, replace=True)
    vol_only = candidate_df[candidate_df["vol_q"] == vol_q]["idx"].to_numpy()
    if len(vol_only) >= 1:
        return rng.choice(vol_only, size=n_samples, replace=True)
    tod_only = candidate_df[candidate_df["tod_slot"] == tod_slot]["idx"].to_numpy()
    if len(tod_only) >= 1:
        return rng.choice(tod_only, size=n_samples, replace=True)
    any_pool = candidate_df["idx"].to_numpy()
    if len(any_pool) == 0:
        return np.array([], dtype=int)
    return rng.choice(any_pool, size=n_samples, replace=True)


def _compute_hazard_curve(
    frame: pd.DataFrame,
    hit_col: str,
    max_horizon: int,
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["time_bar", "at_risk", "events", "hazard"])
    rows: List[Dict[str, float]] = []
    hit = frame[hit_col].astype(float)
    eff = frame["effective_horizon"].astype(float)
    for t in range(1, max_horizon + 1):
        at_risk = (eff >= t) & (hit.isna() | (hit >= t))
        events = hit == t
        risk_n = int(at_risk.sum())
        event_n = int(events.sum())
        hazard = float(event_n / risk_n) if risk_n > 0 else np.nan
        rows.append({"time_bar": t, "at_risk": risk_n, "events": event_n, "hazard": hazard})
    return pd.DataFrame(rows)


def _hazard_summary(curve: pd.DataFrame) -> Dict[str, float]:
    if curve.empty or curve["hazard"].dropna().empty:
        return {
            "peak_time_bar": np.nan,
            "peak_hazard": np.nan,
            "front_8_mean_hazard": np.nan,
            "mid_24_mean_hazard": np.nan,
            "late_mean_hazard": np.nan,
        }
    peak_idx = curve["hazard"].idxmax()
    front = curve[curve["time_bar"] <= 8]["hazard"].mean()
    mid = curve[(curve["time_bar"] >= 9) & (curve["time_bar"] <= 24)]["hazard"].mean()
    late = curve[curve["time_bar"] >= 25]["hazard"].mean()
    return {
        "peak_time_bar": float(curve.at[peak_idx, "time_bar"]),
        "peak_hazard": float(curve.at[peak_idx, "hazard"]),
        "front_8_mean_hazard": float(front) if pd.notna(front) else np.nan,
        "mid_24_mean_hazard": float(mid) if pd.notna(mid) else np.nan,
        "late_mean_hazard": float(late) if pd.notna(late) else np.nan,
    }


def _shape_flags(
    event_n: int,
    liq_summary: Dict[str, float],
    vol_summary: Dict[str, float],
    min_hazard_events: int,
) -> Dict[str, object]:
    if event_n < min_hazard_events:
        return {"vol_front_loaded": None, "liq_delayed": None, "shape_consistent": None}

    vol_front_loaded = bool(
        (np.isfinite(vol_summary["peak_time_bar"]) and vol_summary["peak_time_bar"] <= 8)
        or (
            np.isfinite(vol_summary["front_8_mean_hazard"])
            and np.isfinite(vol_summary["late_mean_hazard"])
            and vol_summary["front_8_mean_hazard"] > vol_summary["late_mean_hazard"]
        )
    )
    liq_delayed = bool(
        (np.isfinite(liq_summary["peak_time_bar"]) and liq_summary["peak_time_bar"] >= 9)
        or (
            np.isfinite(liq_summary["late_mean_hazard"])
            and np.isfinite(liq_summary["front_8_mean_hazard"])
            and liq_summary["late_mean_hazard"] > liq_summary["front_8_mean_hazard"]
        )
    )
    return {
        "vol_front_loaded": vol_front_loaded,
        "liq_delayed": liq_delayed,
        "shape_consistent": bool(vol_front_loaded and liq_delayed),
    }


def _build_shape_consistency_table(
    hazard_summary_df: pd.DataFrame,
    min_hazard_events: int,
    event_types: List[str],
) -> pd.DataFrame:
    required_splits = ["symbol", "bull_bear", "vol_regime"]
    rows: List[Dict[str, object]] = []
    if hazard_summary_df.empty:
        return pd.DataFrame(rows)

    for event_type in event_types:
        for split_name in required_splits:
            subset = hazard_summary_df[
                (hazard_summary_df["event_type"] == event_type)
                & (hazard_summary_df["split_name"] == split_name)
            ].copy()
            if subset.empty:
                rows.append(
                    {
                        "event_type": event_type,
                        "split_name": split_name,
                        "required_levels": 0,
                        "levels_with_events": 0,
                        "levels_shape_consistent": 0,
                        "shape_consistency_pass": False,
                        "status": "insufficient",
                    }
                )
                continue

            eligible = subset[subset["event_n"] >= min_hazard_events]
            levels_with_events = int(len(eligible))
            levels_shape_consistent = int((eligible["shape_consistent"] == True).sum()) if levels_with_events else 0
            pass_all = bool(levels_with_events > 0 and levels_shape_consistent == levels_with_events)

            rows.append(
                {
                    "event_type": event_type,
                    "split_name": split_name,
                    "required_levels": int(len(subset)),
                    "levels_with_events": levels_with_events,
                    "levels_shape_consistent": levels_shape_consistent,
                    "shape_consistency_pass": pass_all,
                    "status": "pass" if pass_all else ("fail_shape_flip" if levels_with_events > 0 else "insufficient"),
                }
            )
    return pd.DataFrame(rows).sort_values(["event_type", "split_name"]).reset_index(drop=True)


def _hazard_auc(curve: pd.DataFrame, hazard_col: str, horizon: int) -> float:
    if curve.empty:
        return np.nan
    vals = curve.loc[curve["time_bar"] <= horizon, hazard_col].astype(float).fillna(0.0)
    if vals.empty:
        return np.nan
    return float(vals.sum())


def _bootstrap_mean_ci(values: np.ndarray, n_boot: int, seed: int, alpha: float = 0.05) -> Dict[str, float]:
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    if len(arr) == 0:
        return {"mean": np.nan, "ci_low": np.nan, "ci_high": np.nan, "n": 0}
    if len(arr) == 1:
        v = float(arr[0])
        return {"mean": v, "ci_low": v, "ci_high": v, "n": 1}
    rng = np.random.default_rng(seed)
    boots = []
    for _ in range(n_boot):
        samp = rng.choice(arr, size=len(arr), replace=True)
        boots.append(float(np.mean(samp)))
    boots_arr = np.asarray(boots, dtype=float)
    return {
        "mean": float(np.mean(arr)),
        "ci_low": float(np.quantile(boots_arr, alpha / 2.0)),
        "ci_high": float(np.quantile(boots_arr, 1.0 - alpha / 2.0)),
        "n": int(len(arr)),
    }


def _bootstrap_delta_ci(
    event_values: np.ndarray,
    baseline_values: np.ndarray,
    n_boot: int,
    seed: int,
    alpha: float = 0.05,
) -> Dict[str, float]:
    ev = np.asarray(event_values, dtype=float)
    bs = np.asarray(baseline_values, dtype=float)
    ev = ev[np.isfinite(ev)]
    bs = bs[np.isfinite(bs)]
    if len(ev) == 0 or len(bs) == 0:
        return {"delta_mean": np.nan, "delta_ci_low": np.nan, "delta_ci_high": np.nan, "event_n": int(len(ev)), "baseline_n": int(len(bs))}
    rng = np.random.default_rng(seed)
    boots = []
    for _ in range(n_boot):
        ev_s = rng.choice(ev, size=len(ev), replace=True)
        bs_s = rng.choice(bs, size=len(bs), replace=True)
        boots.append(float(np.mean(ev_s) - np.mean(bs_s)))
    boots_arr = np.asarray(boots, dtype=float)
    delta = float(np.mean(ev) - np.mean(bs))
    return {
        "delta_mean": delta,
        "delta_ci_low": float(np.quantile(boots_arr, alpha / 2.0)),
        "delta_ci_high": float(np.quantile(boots_arr, 1.0 - alpha / 2.0)),
        "event_n": int(len(ev)),
        "baseline_n": int(len(bs)),
    }


def _delta_pass_label(ci_low: float, ci_high: float) -> str:
    if not (np.isfinite(ci_low) and np.isfinite(ci_high)):
        return "insufficient"
    if ci_low > 0:
        return "positive"
    if ci_high < 0:
        return "negative"
    return "includes_zero"


def _estimate_crossover_time(
    curve: pd.DataFrame,
    start_bar: int,
    confirm_bars: int,
) -> float:
    if curve.empty:
        return np.nan
    work = curve[["time_bar", "liq_hazard_event", "vol_hazard_event"]].copy()
    work = work.sort_values("time_bar").reset_index(drop=True)
    work["delta"] = work["liq_hazard_event"].astype(float) - work["vol_hazard_event"].astype(float)
    bars = work["time_bar"].astype(int).to_numpy()
    deltas = work["delta"].to_numpy(dtype=float)
    min_start = max(1, int(start_bar))
    for i, t in enumerate(bars):
        if t < min_start:
            continue
        j = i + confirm_bars
        if j > len(deltas):
            break
        window = deltas[i:j]
        if np.all(np.isfinite(window)) and float(np.mean(window)) > 0.0:
            return float(t)
    return np.nan


def _subset_by_split(frame: pd.DataFrame, split_name: str, split_value: str) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    if split_name == "pooled":
        return frame.copy()
    if split_name == "symbol":
        return frame[frame["symbol"] == split_value].copy()
    if split_name == "bull_bear":
        return frame[frame["bull_bear"] == split_value].copy()
    if split_name == "vol_regime":
        return frame[frame["vol_regime"] == split_value].copy()
    if split_name == "symbol_x_bull_bear":
        symbol, bb = split_value.split(":", 1)
        return frame[(frame["symbol"] == symbol) & (frame["bull_bear"] == bb)].copy()
    if split_name == "symbol_x_vol_regime":
        symbol, vr = split_value.split(":", 1)
        return frame[(frame["symbol"] == symbol) & (frame["vol_regime"] == vr)].copy()
    return frame.iloc[0:0].copy()


def _acceleration_delta_series(df: pd.DataFrame) -> pd.Series:
    if "funding_event_ts" not in df.columns:
        return pd.Series(dtype=float)
    funding = df[["funding_event_ts", "funding_rate_scaled"]].copy()
    funding = funding.dropna(subset=["funding_event_ts", "funding_rate_scaled"])
    if funding.empty:
        return pd.Series(dtype=float)
    funding["funding_event_ts"] = pd.to_datetime(funding["funding_event_ts"], utc=True, errors="coerce")
    funding = funding.dropna(subset=["funding_event_ts"])
    funding = funding.sort_values("funding_event_ts").drop_duplicates("funding_event_ts", keep="last")
    if len(funding) < 2:
        return pd.Series(dtype=float)
    deltas = funding["funding_rate_scaled"].astype(float).abs().diff().dropna()
    return deltas


def _count_persistent_exceedances(values: pd.Series, threshold: float, persist_n: int) -> int:
    if values.empty or not np.isfinite(threshold):
        return 0
    hits = (values >= threshold).astype(int)
    run = hits.groupby((hits == 0).cumsum()).cumsum()
    started = (hits == 1) & (run == persist_n)
    return int(started.sum())


def _transition_probability_table(
    events_df: pd.DataFrame,
    baseline_df: pd.DataFrame,
    event_type: str,
    normalization_horizon: int,
    bootstrap_iters: int,
    bootstrap_seed: int,
) -> pd.DataFrame:
    schema_cols = [
        "event_type",
        "split_name",
        "split_value",
        "event_n",
        "baseline_n",
        "event_p_vol_burst_1_8",
        "baseline_p_vol_burst_1_8",
        "delta_p_vol_burst_1_8",
        "event_p_liq_30_80",
        "baseline_p_liq_30_80",
        "delta_p_liq_30_80",
        "event_p_norm_within_h",
        "baseline_p_norm_within_h",
        "delta_p_norm_within_h",
        "event_p_drawdown_30_80",
        "baseline_p_drawdown_30_80",
        "delta_p_drawdown_30_80",
        "event_mae_96_mean",
        "baseline_mae_96_mean",
        "delta_mae_96_mean",
        "event_realized_vol_mean_96",
        "baseline_realized_vol_mean_96",
        "delta_realized_vol_mean_96",
        "delta_p_vol_burst_1_8_ci_low",
        "delta_p_vol_burst_1_8_ci_high",
        "delta_p_vol_burst_1_8_pass",
        "delta_p_liq_30_80_ci_low",
        "delta_p_liq_30_80_ci_high",
        "delta_p_liq_30_80_pass",
        "delta_p_norm_within_h_ci_low",
        "delta_p_norm_within_h_ci_high",
        "delta_p_norm_within_h_pass",
        "delta_p_drawdown_30_80_ci_low",
        "delta_p_drawdown_30_80_ci_high",
        "delta_p_drawdown_30_80_pass",
        "delta_mae_96_mean_ci_low",
        "delta_mae_96_mean_ci_high",
        "delta_mae_96_mean_pass",
        "delta_realized_vol_mean_96_ci_low",
        "delta_realized_vol_mean_96_ci_high",
        "delta_realized_vol_mean_96_pass",
    ]
    if events_df.empty:
        return pd.DataFrame(columns=schema_cols)

    def _enrich(frame: pd.DataFrame) -> pd.DataFrame:
        out = frame.copy()
        out["p_vol_burst_1_8"] = out["vol_hit_bar"].astype(float).between(1.0, 8.0, inclusive="both").astype(float)
        out["p_liq_30_80"] = out["liq_30_80_hit"].astype(float)
        out["p_norm_within_h"] = (
            out["time_to_normalization_bars"].astype(float).le(float(normalization_horizon)).fillna(False).astype(float)
        )
        out["p_drawdown_30_80"] = out["drawdown_30_80_hit"].astype(float)
        out["mae_96_mean"] = out["mae_96"].astype(float)
        out["realized_vol_mean_96"] = out["realized_vol_mean_96"].astype(float)
        return out

    ev = _enrich(events_df[events_df["event_type"] == event_type].copy())
    base = _enrich(baseline_df[baseline_df["event_type"] == event_type].copy())
    if ev.empty:
        return pd.DataFrame(columns=schema_cols)

    split_specs: List[Tuple[str, str]] = [("pooled", "ALL")]
    split_specs.extend([("symbol", x) for x in sorted(ev["symbol"].dropna().unique())])
    split_specs.extend([("bull_bear", x) for x in ["bull", "bear"]])
    split_specs.extend([("vol_regime", x) for x in ["low", "high"]])

    rows: List[Dict[str, object]] = []
    for split_name, split_value in split_specs:
        ev_sub = _subset_by_split(ev, split_name, split_value)
        base_sub = _subset_by_split(base, split_name, split_value)
        n_ev = int(len(ev_sub))
        n_base = int(len(base_sub))

        if n_ev > 0:
            ev_vol = float(ev_sub["p_vol_burst_1_8"].mean())
            ev_liq = float(ev_sub["p_liq_30_80"].mean())
            ev_norm = float(ev_sub["p_norm_within_h"].mean())
        else:
            ev_vol = np.nan
            ev_liq = np.nan
            ev_norm = np.nan
        if n_base > 0:
            base_vol = float(base_sub["p_vol_burst_1_8"].mean())
            base_liq = float(base_sub["p_liq_30_80"].mean())
            base_norm = float(base_sub["p_norm_within_h"].mean())
            base_dd = float(base_sub["p_drawdown_30_80"].mean())
            base_mae = float(base_sub["mae_96_mean"].mean())
            base_rv = float(base_sub["realized_vol_mean_96"].mean())
        else:
            base_vol = np.nan
            base_liq = np.nan
            base_norm = np.nan
            base_dd = np.nan
            base_mae = np.nan
            base_rv = np.nan

        if n_ev > 0:
            ev_dd = float(ev_sub["p_drawdown_30_80"].mean())
            ev_mae = float(ev_sub["mae_96_mean"].mean())
            ev_rv = float(ev_sub["realized_vol_mean_96"].mean())
        else:
            ev_dd = np.nan
            ev_mae = np.nan
            ev_rv = np.nan

        vol_ci = _bootstrap_delta_ci(ev_sub["p_vol_burst_1_8"].to_numpy(), base_sub["p_vol_burst_1_8"].to_numpy(), bootstrap_iters, bootstrap_seed + 11)
        liq_ci = _bootstrap_delta_ci(ev_sub["p_liq_30_80"].to_numpy(), base_sub["p_liq_30_80"].to_numpy(), bootstrap_iters, bootstrap_seed + 23)
        norm_ci = _bootstrap_delta_ci(ev_sub["p_norm_within_h"].to_numpy(), base_sub["p_norm_within_h"].to_numpy(), bootstrap_iters, bootstrap_seed + 37)
        dd_ci = _bootstrap_delta_ci(ev_sub["p_drawdown_30_80"].to_numpy(), base_sub["p_drawdown_30_80"].to_numpy(), bootstrap_iters, bootstrap_seed + 41)
        mae_ci = _bootstrap_delta_ci(ev_sub["mae_96_mean"].to_numpy(), base_sub["mae_96_mean"].to_numpy(), bootstrap_iters, bootstrap_seed + 53)
        rv_ci = _bootstrap_delta_ci(ev_sub["realized_vol_mean_96"].to_numpy(), base_sub["realized_vol_mean_96"].to_numpy(), bootstrap_iters, bootstrap_seed + 67)

        rows.append(
            {
                "event_type": event_type,
                "split_name": split_name,
                "split_value": split_value,
                "event_n": n_ev,
                "baseline_n": n_base,
                "event_p_vol_burst_1_8": ev_vol,
                "baseline_p_vol_burst_1_8": base_vol,
                "delta_p_vol_burst_1_8": ev_vol - base_vol if np.isfinite(ev_vol) and np.isfinite(base_vol) else np.nan,
                "event_p_liq_30_80": ev_liq,
                "baseline_p_liq_30_80": base_liq,
                "delta_p_liq_30_80": ev_liq - base_liq if np.isfinite(ev_liq) and np.isfinite(base_liq) else np.nan,
                "event_p_norm_within_h": ev_norm,
                "baseline_p_norm_within_h": base_norm,
                "delta_p_norm_within_h": ev_norm - base_norm if np.isfinite(ev_norm) and np.isfinite(base_norm) else np.nan,
                "event_p_drawdown_30_80": ev_dd,
                "baseline_p_drawdown_30_80": base_dd,
                "delta_p_drawdown_30_80": ev_dd - base_dd if np.isfinite(ev_dd) and np.isfinite(base_dd) else np.nan,
                "event_mae_96_mean": ev_mae,
                "baseline_mae_96_mean": base_mae,
                "delta_mae_96_mean": ev_mae - base_mae if np.isfinite(ev_mae) and np.isfinite(base_mae) else np.nan,
                "event_realized_vol_mean_96": ev_rv,
                "baseline_realized_vol_mean_96": base_rv,
                "delta_realized_vol_mean_96": ev_rv - base_rv if np.isfinite(ev_rv) and np.isfinite(base_rv) else np.nan,
                "delta_p_vol_burst_1_8_ci_low": vol_ci["delta_ci_low"],
                "delta_p_vol_burst_1_8_ci_high": vol_ci["delta_ci_high"],
                "delta_p_vol_burst_1_8_pass": _delta_pass_label(vol_ci["delta_ci_low"], vol_ci["delta_ci_high"]),
                "delta_p_liq_30_80_ci_low": liq_ci["delta_ci_low"],
                "delta_p_liq_30_80_ci_high": liq_ci["delta_ci_high"],
                "delta_p_liq_30_80_pass": _delta_pass_label(liq_ci["delta_ci_low"], liq_ci["delta_ci_high"]),
                "delta_p_norm_within_h_ci_low": norm_ci["delta_ci_low"],
                "delta_p_norm_within_h_ci_high": norm_ci["delta_ci_high"],
                "delta_p_norm_within_h_pass": _delta_pass_label(norm_ci["delta_ci_low"], norm_ci["delta_ci_high"]),
                "delta_p_drawdown_30_80_ci_low": dd_ci["delta_ci_low"],
                "delta_p_drawdown_30_80_ci_high": dd_ci["delta_ci_high"],
                "delta_p_drawdown_30_80_pass": _delta_pass_label(dd_ci["delta_ci_low"], dd_ci["delta_ci_high"]),
                "delta_mae_96_mean_ci_low": mae_ci["delta_ci_low"],
                "delta_mae_96_mean_ci_high": mae_ci["delta_ci_high"],
                "delta_mae_96_mean_pass": _delta_pass_label(mae_ci["delta_ci_low"], mae_ci["delta_ci_high"]),
                "delta_realized_vol_mean_96_ci_low": rv_ci["delta_ci_low"],
                "delta_realized_vol_mean_96_ci_high": rv_ci["delta_ci_high"],
                "delta_realized_vol_mean_96_pass": _delta_pass_label(rv_ci["delta_ci_low"], rv_ci["delta_ci_high"]),
            }
        )

    return pd.DataFrame(rows, columns=schema_cols)


def _fit_lpm(
    df: pd.DataFrame,
    outcome_col: str,
) -> Tuple[List[str], np.ndarray]:
    work = df.copy()
    work = work[
        work["symbol"].isin(["BTCUSDT", "ETHUSDT"])
        & work["bull_bear"].isin(["bull", "bear"])
        & work["vol_regime"].isin(["low", "high"])
    ].copy()
    work = work.dropna(subset=[outcome_col])
    if work.empty:
        return [], np.array([])

    work["is_bull"] = (work["bull_bear"] == "bull").astype(float)
    work["is_high_vol"] = (work["vol_regime"] == "high").astype(float)
    work["is_eth"] = (work["symbol"] == "ETHUSDT").astype(float)
    work["persistence"] = work["persistence"].astype(float)
    work["p_x_bull"] = work["persistence"] * work["is_bull"]
    work["p_x_high_vol"] = work["persistence"] * work["is_high_vol"]
    work["p_x_eth"] = work["persistence"] * work["is_eth"]

    cols = [
        "intercept",
        "persistence",
        "is_bull",
        "is_high_vol",
        "is_eth",
        "p_x_bull",
        "p_x_high_vol",
        "p_x_eth",
    ]
    work["intercept"] = 1.0
    x = work[cols].to_numpy(dtype=float)
    y = work[outcome_col].astype(float).to_numpy(dtype=float)
    beta, *_ = np.linalg.lstsq(x, y, rcond=None)
    return cols, beta


def _coef_sign(value: float, eps: float = 1e-8) -> int:
    if not np.isfinite(value):
        return 0
    if value > eps:
        return 1
    if value < -eps:
        return -1
    return 0


def _interaction_stability(
    events_df: pd.DataFrame,
    baseline_df: pd.DataFrame,
    event_type: str,
    outcome_cols: List[str],
    min_rows_per_year: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    ev = events_df[events_df["event_type"] == event_type].copy()
    base = baseline_df[baseline_df["event_type"] == event_type].copy()
    if ev.empty:
        return pd.DataFrame(), pd.DataFrame()

    ev["persistence"] = 1.0
    base["persistence"] = 0.0
    model_df = pd.concat([ev, base], ignore_index=True)
    model_df["timestamp"] = pd.to_datetime(model_df["timestamp"], utc=True, errors="coerce")
    model_df = model_df.dropna(subset=["timestamp"])
    model_df["year"] = model_df["timestamp"].dt.year.astype(int)

    coef_rows: List[Dict[str, object]] = []
    stability_rows: List[Dict[str, object]] = []
    interaction_terms = ["p_x_bull", "p_x_high_vol", "p_x_eth"]

    for outcome in outcome_cols:
        cols, beta = _fit_lpm(model_df, outcome)
        if len(cols) == 0:
            continue
        full_map = {c: float(v) for c, v in zip(cols, beta)}
        for term in interaction_terms:
            coef_rows.append(
                {
                    "event_type": event_type,
                    "scope": "full",
                    "year": "ALL",
                    "outcome": outcome,
                    "term": term,
                    "coefficient": full_map.get(term, np.nan),
                    "sign": _coef_sign(full_map.get(term, np.nan)),
                }
            )

        year_signs: Dict[Tuple[str, str], List[int]] = {}
        year_terms: Dict[Tuple[str, str], List[float]] = {}
        for year, g in model_df.groupby("year"):
            if len(g) < min_rows_per_year:
                continue
            y_cols, y_beta = _fit_lpm(g, outcome)
            if len(y_cols) == 0:
                continue
            y_map = {c: float(v) for c, v in zip(y_cols, y_beta)}
            for term in interaction_terms:
                sign = _coef_sign(y_map.get(term, np.nan))
                year_signs.setdefault((outcome, term), []).append(sign)
                year_terms.setdefault((outcome, term), []).append(float(y_map.get(term, np.nan)))
                coef_rows.append(
                    {
                        "event_type": event_type,
                        "scope": "year",
                        "year": int(year),
                        "outcome": outcome,
                        "term": term,
                        "coefficient": float(y_map.get(term, np.nan)),
                        "sign": sign,
                    }
                )

        for term in interaction_terms:
            signs = year_signs.get((outcome, term), [])
            nz = [s for s in signs if s != 0]
            full_sign = _coef_sign(full_map.get(term, np.nan))
            if len(nz) == 0:
                status = "insufficient"
                stable = False
            else:
                stable = len(set(nz)) == 1 and (full_sign == 0 or full_sign == nz[0])
                status = "pass" if stable else "fail_sign_flip"
            stability_rows.append(
                {
                    "event_type": event_type,
                    "outcome": outcome,
                    "term": term,
                    "full_coef": float(full_map.get(term, np.nan)),
                    "full_sign": full_sign,
                    "years_used": int(len(signs)),
                    "year_signs": ",".join(str(s) for s in signs) if signs else "",
                    "stable_sign_over_years": bool(stable),
                    "status": status,
                }
            )

    return pd.DataFrame(coef_rows), pd.DataFrame(stability_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze funding episode events (structure only, no direction)")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--funding_pct_window", type=int, default=2880)
    parser.add_argument("--extreme_pct", type=float, default=95.0)
    parser.add_argument("--accel_pct", type=float, default=95.0)
    parser.add_argument("--accel_lookback", type=int, default=32)
    parser.add_argument("--persistence_pct", type=float, default=85.0)
    parser.add_argument("--persistence_bars", type=int, default=12)
    parser.add_argument("--normalization_pct", type=float, default=50.0)
    parser.add_argument("--normalization_lookback", type=int, default=96)
    parser.add_argument("--min_event_spacing", type=int, default=96)
    parser.add_argument("--event_lookahead", type=int, default=96)
    parser.add_argument("--liquidation_abs_return", type=float, default=0.03)
    parser.add_argument("--drawdown_abs_return", type=float, default=0.03)
    parser.add_argument("--vol_burst_quantile", type=float, default=0.9)
    parser.add_argument("--htf_window", type=int, default=384)
    parser.add_argument("--htf_lookback", type=int, default=96)
    parser.add_argument("--pre_window", type=int, default=96)
    parser.add_argument("--post_window", type=int, default=96)
    parser.add_argument("--baseline_samples_per_event", type=int, default=3)
    parser.add_argument("--baseline_seed", type=int, default=7)
    parser.add_argument("--vol_auc_horizon", type=int, default=8)
    parser.add_argument("--liq_auc_horizon", type=int, default=96)
    parser.add_argument("--phase_tolerance_bars", type=int, default=16)
    parser.add_argument("--crossover_confirm_bars", type=int, default=4)
    parser.add_argument("--auc_bootstrap_iters", type=int, default=2000)
    parser.add_argument("--accel_quantile", type=float, default=0.995)
    parser.add_argument("--accel_persistence_intervals", type=int, default=2)
    parser.add_argument("--accel_near_zero_cutoff", type=int, default=5)
    parser.add_argument("--persistence_norm_horizon", type=int, default=96)
    parser.add_argument("--interaction_min_rows_per_year", type=int, default=150)
    parser.add_argument("--min_hazard_events", type=int, default=50)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        raise ValueError("No symbols provided")

    log_handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "funding_events" / args.run_id
    ensure_dir(out_dir)

    all_event_rows: List[Dict[str, object]] = []
    all_baseline_rows: List[Dict[str, object]] = []
    all_hazard_rows: List[Dict[str, object]] = []
    hazard_summary_rows: List[Dict[str, object]] = []
    auc_rows: List[Dict[str, object]] = []
    accel_diag_rows: List[Dict[str, object]] = []
    pooled_accel_deltas: List[pd.Series] = []
    column_map: Dict[str, Dict[str, str]] = {}
    rng = np.random.default_rng(args.baseline_seed)

    for symbol in symbols:
        df = _load_features(symbol)
        required = {"timestamp", "close", "funding_rate_scaled", "rv_96"}
        missing = sorted(required - set(df.columns))
        if missing:
            raise ValueError(f"{symbol}: missing required columns: {missing}")

        rv_pct_col = _pick_window_column(df.columns, "rv_pct_", "rv_pct_2880")
        df = _build_match_fields(df)
        df = _build_regime_fields(df, rv_pct_col=rv_pct_col, htf_window=args.htf_window, htf_lookback=args.htf_lookback)
        df["funding_abs"] = df["funding_rate_scaled"].astype(float).abs()
        df["funding_abs_pct"] = _rolling_percentile(df["funding_abs"], args.funding_pct_window)
        vol_burst_threshold = _safe_float(df["rv_96"].astype(float).quantile(args.vol_burst_quantile))
        column_map[symbol] = {"rv_pct_col": rv_pct_col, "vol_burst_threshold": vol_burst_threshold}

        accel_deltas = _acceleration_delta_series(df)
        pooled_accel_deltas.append(accel_deltas)
        accel_p95 = float(accel_deltas.quantile(0.95)) if not accel_deltas.empty else np.nan
        accel_p99 = float(accel_deltas.quantile(0.99)) if not accel_deltas.empty else np.nan
        accel_p995 = float(accel_deltas.quantile(0.995)) if not accel_deltas.empty else np.nan
        accel_thr = float(accel_deltas.quantile(args.accel_quantile)) if not accel_deltas.empty else np.nan
        accel_count = _count_persistent_exceedances(
            accel_deltas, threshold=accel_thr, persist_n=args.accel_persistence_intervals
        )
        accel_diag_rows.append(
            {
                "symbol": symbol,
                "n_intervals": int(len(accel_deltas)),
                "delta_abs_p95": accel_p95,
                "delta_abs_p99": accel_p99,
                "delta_abs_p995": accel_p995,
                "quantile_threshold": accel_thr,
                "persistence_intervals": int(args.accel_persistence_intervals),
                "quantile_based_event_count": int(accel_count),
            }
        )

        idx_map = _extract_event_indices(
            df=df,
            extreme_pct=args.extreme_pct,
            accel_pct=args.accel_pct,
            accel_lookback=args.accel_lookback,
            persistence_pct=args.persistence_pct,
            persistence_bars=args.persistence_bars,
            normalization_pct=args.normalization_pct,
            normalization_lookback=args.normalization_lookback,
            min_event_spacing=args.min_event_spacing,
        )
        all_indices = sorted({i for indices in idx_map.values() for i in indices})
        blocked = _build_blocked_mask(len(df), all_indices, args.event_lookahead)
        candidate_mask = (~blocked) & df["vol_q"].notna() & (np.arange(len(df)) + args.event_lookahead < len(df))
        candidate_df = (
            df.loc[candidate_mask, ["tod_slot", "vol_q"]]
            .copy()
            .assign(idx=np.flatnonzero(candidate_mask))
        )

        for event_type, indices in idx_map.items():
            for idx in indices:
                metrics = _event_metrics(
                    df=df,
                    idx=idx,
                    vol_burst_threshold=vol_burst_threshold,
                    event_lookahead=args.event_lookahead,
                    liquidation_abs_return=args.liquidation_abs_return,
                    drawdown_abs_return=args.drawdown_abs_return,
                    normalization_pct=args.normalization_pct,
                    pre_window=args.pre_window,
                    post_window=args.post_window,
                )
                if not metrics:
                    continue

                all_event_rows.append(
                    {
                        "symbol": symbol,
                        "event_type": event_type,
                        "event_idx": int(idx),
                        "timestamp": df.at[idx, "timestamp"].isoformat(),
                        "bull_bear": str(df.at[idx, "bull_bear"]),
                        "vol_regime": str(df.at[idx, "vol_regime"]),
                        "funding_rate_scaled": _safe_float(df.at[idx, "funding_rate_scaled"]),
                        "funding_abs_pct": _safe_float(df.at[idx, "funding_abs_pct"]),
                        **metrics,
                    }
                )
                sampled = _sample_controls_for_event(
                    candidate_df=candidate_df,
                    vol_q=str(df.at[idx, "vol_q"]) if pd.notna(df.at[idx, "vol_q"]) else "na",
                    tod_slot=int(df.at[idx, "tod_slot"]),
                    n_samples=args.baseline_samples_per_event,
                    rng=rng,
                )
                for ctrl_idx in sampled:
                    ctrl_metrics = _event_metrics(
                        df=df,
                        idx=int(ctrl_idx),
                        vol_burst_threshold=vol_burst_threshold,
                        event_lookahead=args.event_lookahead,
                        liquidation_abs_return=args.liquidation_abs_return,
                        drawdown_abs_return=args.drawdown_abs_return,
                        normalization_pct=args.normalization_pct,
                        pre_window=args.pre_window,
                        post_window=args.post_window,
                    )
                    if not ctrl_metrics:
                        continue
                    all_baseline_rows.append(
                        {
                            "symbol": symbol,
                            "event_type": event_type,
                            "event_idx": int(ctrl_idx),
                            "matched_event_idx": int(idx),
                            "control_idx": int(ctrl_idx),
                            "timestamp": df.at[int(ctrl_idx), "timestamp"].isoformat(),
                            "bull_bear": str(df.at[int(ctrl_idx), "bull_bear"]),
                            "vol_regime": str(df.at[int(ctrl_idx), "vol_regime"]),
                            "funding_rate_scaled": _safe_float(df.at[int(ctrl_idx), "funding_rate_scaled"]),
                            "funding_abs_pct": _safe_float(df.at[int(ctrl_idx), "funding_abs_pct"]),
                            **ctrl_metrics,
                        }
                    )

    events_df = pd.DataFrame(all_event_rows)
    baseline_df = pd.DataFrame(all_baseline_rows)
    by_symbol_base = _summarize_events(events_df, ["symbol", "event_type"])
    by_symbol, pooled = _inject_missing_event_types(by_symbol_base, symbols)

    event_base = _summarize_events(events_df, ["symbol", "event_type"]).rename(
        columns={
            "event_count": "event_n",
            "liquidation_probability": "event_liquidation_probability",
            "vol_burst_likelihood": "event_vol_burst_likelihood",
            "time_to_normalization_median": "event_time_to_normalization_median",
            "time_to_normalization_p75": "event_time_to_normalization_p75",
            "post_event_vol_decay_mean": "event_post_event_vol_decay_mean",
            "post_event_vol_decay_median": "event_post_event_vol_decay_median",
        }
    )
    baseline_base = _summarize_events(baseline_df, ["symbol", "event_type"]).rename(
        columns={
            "event_count": "baseline_n",
            "liquidation_probability": "baseline_liquidation_probability",
            "vol_burst_likelihood": "baseline_vol_burst_likelihood",
            "time_to_normalization_median": "baseline_time_to_normalization_median",
            "time_to_normalization_p75": "baseline_time_to_normalization_p75",
            "post_event_vol_decay_mean": "baseline_post_event_vol_decay_mean",
            "post_event_vol_decay_median": "baseline_post_event_vol_decay_median",
        }
    )
    baseline_compare = event_base.merge(baseline_base, on=["symbol", "event_type"], how="outer")
    for col in [
        "event_n",
        "baseline_n",
        "event_liquidation_probability",
        "baseline_liquidation_probability",
        "event_vol_burst_likelihood",
        "baseline_vol_burst_likelihood",
        "event_time_to_normalization_median",
        "baseline_time_to_normalization_median",
        "event_post_event_vol_decay_mean",
        "baseline_post_event_vol_decay_mean",
    ]:
        if col not in baseline_compare.columns:
            baseline_compare[col] = np.nan
    baseline_compare["delta_liquidation_probability"] = (
        baseline_compare["event_liquidation_probability"] - baseline_compare["baseline_liquidation_probability"]
    )
    baseline_compare["delta_vol_burst_likelihood"] = (
        baseline_compare["event_vol_burst_likelihood"] - baseline_compare["baseline_vol_burst_likelihood"]
    )
    baseline_compare["delta_time_to_normalization_median"] = (
        baseline_compare["event_time_to_normalization_median"]
        - baseline_compare["baseline_time_to_normalization_median"]
    )
    baseline_compare["delta_post_event_vol_decay_mean"] = (
        baseline_compare["event_post_event_vol_decay_mean"]
        - baseline_compare["baseline_post_event_vol_decay_mean"]
    )
    baseline_compare = baseline_compare.sort_values(["symbol", "event_type"]).reset_index(drop=True)

    if not events_df.empty:
        events_df["p_vol_burst_1_8"] = events_df["vol_hit_bar"].astype(float).between(1.0, 8.0, inclusive="both").astype(float)
    if not baseline_df.empty:
        baseline_df["p_vol_burst_1_8"] = baseline_df["vol_hit_bar"].astype(float).between(1.0, 8.0, inclusive="both").astype(float)

    persistence_transition_df = _transition_probability_table(
        events_df=events_df,
        baseline_df=baseline_df,
        event_type="funding_persistence_window",
        normalization_horizon=args.persistence_norm_horizon,
        bootstrap_iters=args.auc_bootstrap_iters,
        bootstrap_seed=args.baseline_seed + 101,
    ).sort_values(["split_name", "split_value"]).reset_index(drop=True)
    persistence_transition_required_df = persistence_transition_df[
        persistence_transition_df["split_name"].isin(["symbol", "bull_bear", "vol_regime"])
    ].copy()
    interaction_coef_df, interaction_stability_df = _interaction_stability(
        events_df=events_df,
        baseline_df=baseline_df,
        event_type="funding_persistence_window",
        outcome_cols=["liq_30_80_hit", "drawdown_30_80_hit", "p_vol_burst_1_8"],
        min_rows_per_year=args.interaction_min_rows_per_year,
    )

    split_specs: List[Tuple[str, str]] = [("pooled", "ALL")]
    split_specs.extend([("symbol", s) for s in symbols])
    split_specs.extend([("bull_bear", x) for x in ["bull", "bear"]])
    split_specs.extend([("vol_regime", x) for x in ["low", "high"]])
    for symbol in symbols:
        split_specs.extend([("symbol_x_bull_bear", f"{symbol}:{x}") for x in ["bull", "bear"]])
        split_specs.extend([("symbol_x_vol_regime", f"{symbol}:{x}") for x in ["low", "high"]])

    for event_type in PRIMARY_EVENT_TYPES:
        event_type_events = events_df[events_df["event_type"] == event_type].copy()
        event_type_baselines = baseline_df[baseline_df["event_type"] == event_type].copy()

        for split_name, split_value in split_specs:
            eframe = _subset_by_split(event_type_events, split_name, split_value)
            bframe = _subset_by_split(event_type_baselines, split_name, split_value)
            event_n = int(len(eframe))
            baseline_n = int(len(bframe))

            liq_event = _compute_hazard_curve(eframe, "liq_hit_bar", args.event_lookahead).rename(
                columns={"at_risk": "liq_at_risk_event", "events": "liq_events_event", "hazard": "liq_hazard_event"}
            )
            vol_event = _compute_hazard_curve(eframe, "vol_hit_bar", args.event_lookahead).rename(
                columns={"at_risk": "vol_at_risk_event", "events": "vol_events_event", "hazard": "vol_hazard_event"}
            )
            liq_base = _compute_hazard_curve(bframe, "liq_hit_bar", args.event_lookahead).rename(
                columns={"at_risk": "liq_at_risk_base", "events": "liq_events_base", "hazard": "liq_hazard_base"}
            )
            vol_base = _compute_hazard_curve(bframe, "vol_hit_bar", args.event_lookahead).rename(
                columns={"at_risk": "vol_at_risk_base", "events": "vol_events_base", "hazard": "vol_hazard_base"}
            )

            merged_curve = liq_event.merge(vol_event, on="time_bar", how="outer")
            merged_curve = merged_curve.merge(liq_base, on="time_bar", how="outer")
            merged_curve = merged_curve.merge(vol_base, on="time_bar", how="outer")
            for col in ["liq_hazard_event", "vol_hazard_event", "liq_hazard_base", "vol_hazard_base"]:
                if col not in merged_curve.columns:
                    merged_curve[col] = np.nan
            merged_curve["liq_hazard_delta"] = merged_curve["liq_hazard_event"].astype(float) - merged_curve["liq_hazard_base"].astype(float)
            merged_curve["vol_hazard_delta"] = merged_curve["vol_hazard_event"].astype(float) - merged_curve["vol_hazard_base"].astype(float)
            merged_curve["event_type"] = event_type
            merged_curve["split_name"] = split_name
            merged_curve["split_value"] = split_value
            merged_curve["event_n"] = event_n
            merged_curve["baseline_n"] = baseline_n
            all_hazard_rows.extend(merged_curve.to_dict(orient="records"))

            liq_sum = _hazard_summary(liq_event.rename(columns={"liq_hazard_event": "hazard"}))
            vol_sum = _hazard_summary(vol_event.rename(columns={"vol_hazard_event": "hazard"}))
            flags = _shape_flags(event_n, liq_sum, vol_sum, args.min_hazard_events)

            t_vol_peak = vol_sum["peak_time_bar"]
            t_liq_peak = liq_sum["peak_time_bar"]
            t_crossover = _estimate_crossover_time(
                merged_curve[["time_bar", "liq_hazard_event", "vol_hazard_event"]],
                start_bar=int(t_vol_peak) if np.isfinite(t_vol_peak) else 1,
                confirm_bars=args.crossover_confirm_bars,
            )

            auc_rows.append(
                {
                    "event_type": event_type,
                    "split_name": split_name,
                    "split_value": split_value,
                    "event_n": event_n,
                    "baseline_n": baseline_n,
                    "vol_auc_horizon": int(args.vol_auc_horizon),
                    "liq_auc_horizon": int(args.liq_auc_horizon),
                    "vol_auc_delta": _hazard_auc(merged_curve, "vol_hazard_delta", args.vol_auc_horizon),
                    "liq_auc_delta": _hazard_auc(merged_curve, "liq_hazard_delta", args.liq_auc_horizon),
                    "t_vol_peak": t_vol_peak,
                    "t_liq_peak": t_liq_peak,
                    "t_crossover": t_crossover,
                }
            )

            hazard_summary_rows.append(
                {
                    "event_type": event_type,
                    "split_name": split_name,
                    "split_value": split_value,
                    "event_n": event_n,
                    "baseline_n": baseline_n,
                    "liq_peak_time_bar": t_liq_peak,
                    "liq_peak_hazard": liq_sum["peak_hazard"],
                    "liq_front_8_mean_hazard": liq_sum["front_8_mean_hazard"],
                    "liq_mid_24_mean_hazard": liq_sum["mid_24_mean_hazard"],
                    "liq_late_mean_hazard": liq_sum["late_mean_hazard"],
                    "vol_peak_time_bar": t_vol_peak,
                    "vol_peak_hazard": vol_sum["peak_hazard"],
                    "vol_front_8_mean_hazard": vol_sum["front_8_mean_hazard"],
                    "vol_mid_24_mean_hazard": vol_sum["mid_24_mean_hazard"],
                    "vol_late_mean_hazard": vol_sum["late_mean_hazard"],
                    "t_crossover": t_crossover,
                    "vol_front_loaded": flags["vol_front_loaded"],
                    "liq_delayed": flags["liq_delayed"],
                    "shape_consistent": flags["shape_consistent"],
                }
            )

    hazard_summary_df = pd.DataFrame(hazard_summary_rows)
    shape_consistency_df = _build_shape_consistency_table(
        hazard_summary_df,
        min_hazard_events=args.min_hazard_events,
        event_types=PRIMARY_EVENT_TYPES,
    )

    auc_df = pd.DataFrame(auc_rows)
    required_auc = auc_df[auc_df["split_name"].isin(["symbol", "bull_bear", "vol_regime"])].copy()
    auc_ci_rows: List[Dict[str, object]] = []
    for event_type in PRIMARY_EVENT_TYPES:
        sub = required_auc[(required_auc["event_type"] == event_type) & (required_auc["event_n"] >= args.min_hazard_events)]
        vol_stats = _bootstrap_mean_ci(sub["vol_auc_delta"].to_numpy(), n_boot=args.auc_bootstrap_iters, seed=args.baseline_seed + 11)
        liq_stats = _bootstrap_mean_ci(sub["liq_auc_delta"].to_numpy(), n_boot=args.auc_bootstrap_iters, seed=args.baseline_seed + 23)
        auc_ci_rows.append(
            {
                "event_type": event_type,
                "n_splits": int(len(sub)),
                "vol_auc_delta_mean": vol_stats["mean"],
                "vol_auc_delta_ci_low": vol_stats["ci_low"],
                "vol_auc_delta_ci_high": vol_stats["ci_high"],
                "liq_auc_delta_mean": liq_stats["mean"],
                "liq_auc_delta_ci_low": liq_stats["ci_low"],
                "liq_auc_delta_ci_high": liq_stats["ci_high"],
            }
        )
    auc_ci_df = pd.DataFrame(auc_ci_rows)

    phase_rows: List[Dict[str, object]] = []
    for event_type in PRIMARY_EVENT_TYPES:
        sub = required_auc[(required_auc["event_type"] == event_type) & (required_auc["event_n"] >= args.min_hazard_events)].copy()
        if sub.empty:
            phase_rows.append(
                {
                    "event_type": event_type,
                    "n_splits": 0,
                    "t_vol_peak_min": np.nan,
                    "t_vol_peak_max": np.nan,
                    "t_vol_peak_spread": np.nan,
                    "t_vol_peak_pass": False,
                    "t_liq_peak_min": np.nan,
                    "t_liq_peak_max": np.nan,
                    "t_liq_peak_spread": np.nan,
                    "t_liq_peak_pass": False,
                    "t_crossover_min": np.nan,
                    "t_crossover_max": np.nan,
                    "t_crossover_spread": np.nan,
                    "t_crossover_pass": False,
                    "phase_stability_pass": False,
                    "status": "insufficient",
                }
            )
            continue
        def _spread_pass(series: pd.Series) -> Tuple[float, float, float, bool]:
            clean = series.astype(float).dropna()
            if clean.empty:
                return np.nan, np.nan, np.nan, False
            mn = float(clean.min())
            mx = float(clean.max())
            sp = float(mx - mn)
            return mn, mx, sp, bool(sp <= args.phase_tolerance_bars)

        vmin, vmax, vsp, vpass = _spread_pass(sub["t_vol_peak"])
        lmin, lmax, lsp, lpass = _spread_pass(sub["t_liq_peak"])
        cmin, cmax, csp, cpass = _spread_pass(sub["t_crossover"])
        phase_rows.append(
            {
                "event_type": event_type,
                "n_splits": int(len(sub)),
                "t_vol_peak_min": vmin,
                "t_vol_peak_max": vmax,
                "t_vol_peak_spread": vsp,
                "t_vol_peak_pass": vpass,
                "t_liq_peak_min": lmin,
                "t_liq_peak_max": lmax,
                "t_liq_peak_spread": lsp,
                "t_liq_peak_pass": lpass,
                "t_crossover_min": cmin,
                "t_crossover_max": cmax,
                "t_crossover_spread": csp,
                "t_crossover_pass": cpass,
                "phase_stability_pass": bool(vpass and lpass and cpass),
                "status": "pass" if (vpass and lpass and cpass) else "fail_or_partial",
            }
        )
    phase_df = pd.DataFrame(phase_rows)

    pooled_acc = pd.concat(pooled_accel_deltas, ignore_index=True) if pooled_accel_deltas else pd.Series(dtype=float)
    pooled_threshold = float(pooled_acc.quantile(args.accel_quantile)) if not pooled_acc.empty else np.nan
    pooled_count = _count_persistent_exceedances(
        pooled_acc, threshold=pooled_threshold, persist_n=args.accel_persistence_intervals
    )
    pooled_accel_row = {
        "symbol": "ALL",
        "n_intervals": int(len(pooled_acc)),
        "delta_abs_p95": float(pooled_acc.quantile(0.95)) if not pooled_acc.empty else np.nan,
        "delta_abs_p99": float(pooled_acc.quantile(0.99)) if not pooled_acc.empty else np.nan,
        "delta_abs_p995": float(pooled_acc.quantile(0.995)) if not pooled_acc.empty else np.nan,
        "quantile_threshold": pooled_threshold,
        "persistence_intervals": int(args.accel_persistence_intervals),
        "quantile_based_event_count": int(pooled_count),
    }
    accel_diag_df = pd.DataFrame(accel_diag_rows + [pooled_accel_row]).sort_values("symbol").reset_index(drop=True)

    output = {
        "run_id": args.run_id,
        "symbols": symbols,
        "config": {
            "funding_pct_window": args.funding_pct_window,
            "extreme_pct": args.extreme_pct,
            "accel_pct": args.accel_pct,
            "accel_lookback": args.accel_lookback,
            "persistence_pct": args.persistence_pct,
            "persistence_bars": args.persistence_bars,
            "normalization_pct": args.normalization_pct,
            "normalization_lookback": args.normalization_lookback,
            "min_event_spacing": args.min_event_spacing,
            "event_lookahead": args.event_lookahead,
            "liquidation_abs_return": args.liquidation_abs_return,
            "drawdown_abs_return": args.drawdown_abs_return,
            "vol_burst_quantile": args.vol_burst_quantile,
            "htf_window": args.htf_window,
            "htf_lookback": args.htf_lookback,
            "pre_window": args.pre_window,
            "post_window": args.post_window,
            "baseline_samples_per_event": args.baseline_samples_per_event,
            "baseline_seed": args.baseline_seed,
            "vol_auc_horizon": args.vol_auc_horizon,
            "liq_auc_horizon": args.liq_auc_horizon,
            "phase_tolerance_bars": args.phase_tolerance_bars,
            "crossover_confirm_bars": args.crossover_confirm_bars,
            "auc_bootstrap_iters": args.auc_bootstrap_iters,
            "accel_quantile": args.accel_quantile,
            "accel_persistence_intervals": args.accel_persistence_intervals,
            "accel_near_zero_cutoff": args.accel_near_zero_cutoff,
            "persistence_norm_horizon": args.persistence_norm_horizon,
            "interaction_min_rows_per_year": args.interaction_min_rows_per_year,
            "min_hazard_events": args.min_hazard_events,
        },
        "primary_event_types": PRIMARY_EVENT_TYPES,
        "column_map": column_map,
        "event_count_total": int(len(events_df)),
        "by_symbol": by_symbol.to_dict(orient="records"),
        "pooled": pooled.to_dict(orient="records"),
        "matched_baseline_comparison": baseline_compare.to_dict(orient="records"),
        "persistence_transition_model": persistence_transition_df.to_dict(orient="records"),
        "persistence_transition_model_required_splits": persistence_transition_required_df.to_dict(orient="records"),
        "interaction_coefficients": interaction_coef_df.to_dict(orient="records"),
        "interaction_stability": interaction_stability_df.to_dict(orient="records"),
        "hazard_summary": hazard_summary_df.to_dict(orient="records"),
        "shape_consistency": shape_consistency_df.to_dict(orient="records"),
        "hazard_auc_by_split": auc_df.to_dict(orient="records"),
        "hazard_auc_ci": auc_ci_df.to_dict(orient="records"),
        "phase_boundary_stability": phase_df.to_dict(orient="records"),
        "acceleration_diagnostic": accel_diag_df.to_dict(orient="records"),
        "hazard_curves": all_hazard_rows,
        "events": events_df.to_dict(orient="records"),
        "matched_baselines": baseline_df.to_dict(orient="records"),
    }

    json_path = out_dir / "funding_episode_events.json"
    json_path.write_text(json.dumps(output, indent=2), encoding="utf-8")

    lines: List[str] = [
        "# Funding Episode Events Report",
        "",
        f"Run ID: `{args.run_id}`",
        f"Symbols: `{','.join(symbols)}`",
        "",
        "Structure-only analysis (no directional signal).",
        "",
        f"Total events: `{len(events_df)}`",
        "",
        "## By Symbol",
        by_symbol.to_markdown(index=False) if not by_symbol.empty else "No rows",
        "",
        "## Pooled",
        pooled.to_markdown(index=False) if not pooled.empty else "No rows",
        "",
        "## Matched Baseline Comparison",
        "Matched non-event controls are sampled within symbol, volatility quartile, and 15m time-of-day slot.",
        "Interpretation: absolute hazards describe event geometry; delta hazards (event-baseline) describe excess risk materiality.",
        baseline_compare.to_markdown(index=False) if not baseline_compare.empty else "No rows",
        "",
        "## Persistence Transition Model (Primary Primitive)",
        f"Normalization horizon H={args.persistence_norm_horizon} bars.",
        "Probabilities are event vs matched baseline deltas for persistence-window events only.",
        persistence_transition_required_df.to_markdown(index=False) if not persistence_transition_required_df.empty else "No rows",
        "",
        "## Interaction Stability Over Time (Persistence Model)",
        f"Year splits require at least {args.interaction_min_rows_per_year} rows.",
        interaction_stability_df.to_markdown(index=False) if not interaction_stability_df.empty else "No rows",
        "",
        "## Hazard Shape Consistency (Required Splits)",
        f"Split pass requires shape consistency on levels with event_n >= {args.min_hazard_events}.",
        shape_consistency_df.to_markdown(index=False) if not shape_consistency_df.empty else "No rows",
        "",
        "## Hazard AUC Delta (Split Distribution + CI)",
        f"vol AUC uses bars 1..{args.vol_auc_horizon}; liquidation AUC uses bars 1..{args.liq_auc_horizon}.",
        auc_ci_df.to_markdown(index=False) if not auc_ci_df.empty else "No rows",
        "",
        "## Phase Boundary Stability",
        f"Pass requires spread <= {args.phase_tolerance_bars} bars across required splits.",
        phase_df.to_markdown(index=False) if not phase_df.empty else "No rows",
        "",
        "## Acceleration Diagnostic (Counts Only)",
        "Quantile thresholds are distribution-derived from |delta funding| across funding intervals.",
        "Acceleration is diagnostic-only (not part of primary event pipeline).",
        accel_diag_df.to_markdown(index=False) if not accel_diag_df.empty else "No rows",
        "",
        "## Hazard Summary (Instantaneous)",
        hazard_summary_df.to_markdown(index=False) if not hazard_summary_df.empty else "No rows",
        "",
        "## Definitions",
        f"- funding extreme onset: funding_abs_pct crosses above {args.extreme_pct}",
        f"- funding acceleration: percentile of positive funding_abs delta crosses above {args.accel_pct}",
        f"- funding persistence window: funding_abs_pct stays >= {args.persistence_pct} for {args.persistence_bars} bars",
        f"- funding normalization: funding_abs_pct crosses below {args.normalization_pct} with recent extreme context",
        "",
        "## Metrics",
        "- liquidation_probability: chance max absolute forward return exceeds threshold",
        "- vol_burst_likelihood: chance forward rv_96 exceeds symbol vol-burst threshold",
        "- time_to_normalization_median: median bars to reach normalization threshold",
        "- post_event_vol_decay_mean: mean relative drop from pre-event vol to post-event vol window",
        "- absolute hazard vs delta hazard: absolute hazard captures timing shape; delta hazard captures matched excess/deficit risk",
        "- vol_auc_delta / liq_auc_delta: integrated event-minus-baseline hazard materiality",
        "- t_vol_peak / t_liq_peak / t_crossover: measured phase boundaries from event hazards",
        "- p_vol_burst_1_8: probability of vol burst in first 8 bars after persistence event",
        "- p_liq_30_80: probability of liquidation proxy in bars 30..80 after persistence event",
        "- p_norm_within_h: probability of normalization within horizon H after persistence event",
        "",
    ]

    md_path = out_dir / "funding_episode_events.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")

    hazard_csv = out_dir / "funding_episode_hazards.csv"
    pd.DataFrame(all_hazard_rows).to_csv(hazard_csv, index=False)
    auc_csv = out_dir / "funding_episode_hazard_auc.csv"
    auc_df.to_csv(auc_csv, index=False)
    phase_csv = out_dir / "funding_episode_phase_stability.csv"
    phase_df.to_csv(phase_csv, index=False)
    transition_csv = out_dir / "funding_persistence_transition_model.csv"
    persistence_transition_df.to_csv(transition_csv, index=False)
    interaction_csv = out_dir / "funding_persistence_interaction_stability.csv"
    interaction_stability_df.to_csv(interaction_csv, index=False)
    accel_csv = out_dir / "funding_acceleration_diagnostic.csv"
    accel_diag_df.to_csv(accel_csv, index=False)

    logging.info("Wrote %s", json_path)
    logging.info("Wrote %s", md_path)
    logging.info("Wrote %s", hazard_csv)
    logging.info("Wrote %s", auc_csv)
    logging.info("Wrote %s", phase_csv)
    logging.info("Wrote %s", transition_csv)
    logging.info("Wrote %s", interaction_csv)
    logging.info("Wrote %s", accel_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
