from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import (
    choose_partition_dir,
    ensure_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
)
from pipelines.research.analyze_conditional_expectancy import _bh_adjust, build_walk_forward_split_labels


@dataclass
class CompressionEvent:
    symbol: str
    start_idx: int
    end_idx: int
    end_reason: str
    trend_state: int
    funding_bucket: str
    year: int
    vol_q: str
    bull_bear: str
    enter_ts: pd.Timestamp


EVENT_ROW_COLUMNS = [
    "symbol",
    "event_start_idx",
    "enter_ts",
    "split_label",
    "year",
    "vol_q",
    "bull_bear",
    "funding_bucket",
    "horizon",
    "end_reason",
    "trend_state",
    "breakout_dir",
    "breakout_aligns_htf",
    "time_to_expansion_bars",
    "mfe_post_end",
    "event_return",
    "event_directional_return",
]

ROBUST_GATE_PROFILES: Dict[str, Dict[str, float | int]] = {
    "discovery": {
        "min_samples": 60,
        "tstat_threshold": 1.64,
        "robust_hac_t_threshold": 1.64,
        "robust_bootstrap_alpha": 0.20,
        "robust_fdr_q": 0.20,
        "robust_hac_max_lag": 8,
        "robust_bootstrap_iters": 400,
        "robust_bootstrap_block_size": 8,
        "robust_bootstrap_seed": 7,
        "oos_min_samples": 20,
        "require_oos_positive": 1,
        "require_oos_sign_consistency": 0,
    },
    "promotion": {
        "min_samples": 100,
        "tstat_threshold": 2.0,
        "robust_hac_t_threshold": 1.96,
        "robust_bootstrap_alpha": 0.10,
        "robust_fdr_q": 0.10,
        "robust_hac_max_lag": 12,
        "robust_bootstrap_iters": 800,
        "robust_bootstrap_block_size": 8,
        "robust_bootstrap_seed": 7,
        "oos_min_samples": 40,
        "require_oos_positive": 1,
        "require_oos_sign_consistency": 1,
    },
}


def _apply_gate_profile_defaults(args: argparse.Namespace) -> argparse.Namespace:
    profile = str(getattr(args, "gate_profile", "custom")).strip().lower()
    if profile == "custom":
        return args
    overrides = ROBUST_GATE_PROFILES.get(profile)
    if not overrides:
        raise ValueError(f"Unknown gate profile: {profile}")
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def _parse_horizons(value: str) -> List[int]:
    parts = [x.strip() for x in value.split(",") if x.strip()]
    horizons = sorted({int(x) for x in parts if int(x) > 0})
    if not horizons:
        raise ValueError("At least one positive horizon is required")
    return horizons


def _pick_window_column(columns: Iterable[str], prefix: str) -> str:
    candidates: List[Tuple[int, str]] = []
    for col in columns:
        if not col.startswith(prefix):
            continue
        try:
            window = int(col.split("_")[-1])
        except ValueError:
            continue
        candidates.append((window, col))
    if not candidates:
        raise ValueError(f"Missing required feature prefix: {prefix}")
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


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


def _distribution_stats(returns: pd.Series) -> Dict[str, float]:
    clean = returns.dropna().astype(float)
    n = int(len(clean))
    if n == 0:
        return {
            "samples": 0,
            "mean_return": 0.0,
            "median_return": 0.0,
            "std_return": 0.0,
            "win_rate": 0.0,
            "p25": 0.0,
            "p75": 0.0,
            "t_stat": 0.0,
        }
    mean_val = float(clean.mean())
    median_val = float(clean.median())
    std_val = float(clean.std())
    t_stat = float(mean_val / (std_val / np.sqrt(n))) if std_val > 0.0 and n > 1 else 0.0
    return {
        "samples": n,
        "mean_return": mean_val,
        "median_return": median_val,
        "std_return": std_val,
        "win_rate": float((clean > 0).mean()),
        "p25": float(clean.quantile(0.25)),
        "p75": float(clean.quantile(0.75)),
        "t_stat": t_stat,
    }


def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(float(x) / math.sqrt(2.0)))


def _two_sided_p_from_t(t_stat: float) -> float:
    if not np.isfinite(t_stat):
        return 1.0
    z = abs(float(t_stat))
    return float(max(0.0, min(1.0, 2.0 * (1.0 - _normal_cdf(z)))))


def _newey_west_t_stat(values: pd.Series, max_lag: int) -> Tuple[float, float, int]:
    clean = pd.to_numeric(values, errors="coerce").dropna().astype(float).to_numpy()
    n = int(len(clean))
    if n < 2:
        return 0.0, 1.0, 0
    if max_lag < 0:
        max_lag = int(np.floor(4.0 * (n / 100.0) ** (2.0 / 9.0)))
    lag = int(max(0, min(max_lag, n - 1)))
    mean_val = float(clean.mean())
    residuals = clean - mean_val
    gamma0 = float(np.dot(residuals, residuals) / n)
    long_run_var = gamma0
    for l in range(1, lag + 1):
        gamma_l = float(np.dot(residuals[l:], residuals[:-l]) / n)
        weight = 1.0 - (l / (lag + 1.0))
        long_run_var += 2.0 * weight * gamma_l
    long_run_var = max(float(long_run_var), 1e-18)
    se = float(np.sqrt(long_run_var / n))
    if se <= 0.0 or not np.isfinite(se):
        return 0.0, 1.0, lag
    t_stat = float(mean_val / se)
    return t_stat, _two_sided_p_from_t(t_stat), lag


def _circular_block_bootstrap_pvalue(values: pd.Series, *, block_size: int, n_boot: int, seed: int) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna().astype(float).to_numpy()
    n = int(len(clean))
    if n < 2 or n_boot <= 0:
        return 1.0
    observed = float(clean.mean())
    if not np.isfinite(observed):
        return 1.0
    centered = clean - observed
    block = int(max(1, min(block_size, n)))
    blocks_per_draw = int(np.ceil(n / block))
    rng = np.random.default_rng(int(seed))
    exceed = 0
    for _ in range(int(n_boot)):
        starts = rng.integers(0, n, size=blocks_per_draw)
        sample = np.empty(blocks_per_draw * block, dtype=float)
        pos = 0
        for s in starts:
            idx = (int(s) + np.arange(block)) % n
            sample[pos : pos + block] = centered[idx]
            pos += block
        draw_mean = float(sample[:n].mean())
        if abs(draw_mean) >= abs(observed):
            exceed += 1
    # Add-one smoothing avoids exact zero p-values in finite Monte Carlo draws.
    return float((exceed + 1) / (int(n_boot) + 1))


def _stable_row_seed(condition: str, horizon: int, base_seed: int) -> int:
    acc = (int(base_seed) + int(horizon) * 1009) % (2**32 - 1)
    for ch in str(condition):
        acc = (acc * 131 + ord(ch)) % (2**32 - 1)
    return int(acc)


def _oos_diagnostics(
    event_frame: pd.DataFrame,
    *,
    ret_col: str,
    oos_min_samples: int,
    require_oos_positive: int,
    require_oos_sign_consistency: int,
) -> Dict[str, object]:
    if event_frame.empty or ret_col not in event_frame.columns:
        return {
            "oos_samples": 0,
            "train_mean": np.nan,
            "validation_mean": np.nan,
            "test_mean": np.nan,
            "oos_mean": np.nan,
            "oos_positive": False,
            "oos_sign_consistent": False,
            "oos_pass": False,
        }
    values = pd.to_numeric(event_frame[ret_col], errors="coerce")
    split = (
        event_frame["split_label"].astype(str)
        if "split_label" in event_frame.columns
        else pd.Series("", index=event_frame.index, dtype="object")
    )
    train_vals = values[split == "train"].dropna()
    val_vals = values[split == "validation"].dropna()
    test_vals = values[split == "test"].dropna()
    oos_vals = pd.concat([val_vals, test_vals], ignore_index=True)
    train_mean = float(train_vals.mean()) if not train_vals.empty else np.nan
    val_mean = float(val_vals.mean()) if not val_vals.empty else np.nan
    test_mean = float(test_vals.mean()) if not test_vals.empty else np.nan
    oos_mean = float(oos_vals.mean()) if not oos_vals.empty else np.nan
    oos_positive = bool(np.isfinite(oos_mean) and oos_mean > 0.0)
    oos_sign_consistent = bool(
        np.isfinite(train_mean)
        and np.isfinite(oos_mean)
        and train_mean != 0.0
        and oos_mean != 0.0
        and np.sign(train_mean) == np.sign(oos_mean)
    )
    oos_pass = bool(
        len(oos_vals) >= int(oos_min_samples)
        and (not int(require_oos_positive) or oos_positive)
        and (not int(require_oos_sign_consistency) or oos_sign_consistent)
    )
    return {
        "oos_samples": int(len(oos_vals)),
        "train_mean": train_mean,
        "validation_mean": val_mean,
        "test_mean": test_mean,
        "oos_mean": oos_mean,
        "oos_positive": oos_positive,
        "oos_sign_consistent": oos_sign_consistent,
        "oos_pass": oos_pass,
    }


def _robust_row_fields(
    *,
    event_frame: pd.DataFrame,
    ret_col: str,
    condition: str,
    horizon: int,
    hac_max_lag: int,
    bootstrap_block_size: int,
    bootstrap_iters: int,
    bootstrap_seed: int,
    oos_min_samples: int,
    require_oos_positive: int,
    require_oos_sign_consistency: int,
) -> Dict[str, object]:
    series = (
        pd.to_numeric(event_frame[ret_col], errors="coerce")
        if ret_col in event_frame.columns
        else pd.Series(dtype=float)
    )
    hac_t, hac_p, hac_used_lag = _newey_west_t_stat(series, max_lag=hac_max_lag)
    boot_seed = _stable_row_seed(condition=condition, horizon=horizon, base_seed=bootstrap_seed)
    bootstrap_p = _circular_block_bootstrap_pvalue(
        series,
        block_size=int(bootstrap_block_size),
        n_boot=int(bootstrap_iters),
        seed=boot_seed,
    )
    oos = _oos_diagnostics(
        event_frame,
        ret_col=ret_col,
        oos_min_samples=int(oos_min_samples),
        require_oos_positive=int(require_oos_positive),
        require_oos_sign_consistency=int(require_oos_sign_consistency),
    )
    return {
        "hac_t": float(hac_t),
        "hac_p": float(hac_p),
        "hac_used_lag": int(hac_used_lag),
        "bootstrap_p": float(bootstrap_p),
        **oos,
    }


def _apply_robust_survivor_gates(
    trap_df: pd.DataFrame,
    *,
    min_samples: int,
    legacy_tstat_threshold: float,
    robust_hac_t_threshold: float,
    bootstrap_alpha: float,
    fdr_q: float,
    oos_min_samples: int,
    require_oos_positive: int,
    require_oos_sign_consistency: int,
) -> pd.DataFrame:
    out = trap_df.copy()
    if out.empty:
        return out
    for col, default in (
        ("event_samples", 0.0),
        ("event_mean", 0.0),
        ("event_t", 0.0),
        ("hac_t", 0.0),
        ("hac_p", 1.0),
        ("bootstrap_p", 1.0),
        ("oos_samples", 0.0),
        ("oos_mean", np.nan),
    ):
        if col not in out.columns:
            out[col] = default
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(default)
    if "oos_sign_consistent" not in out.columns:
        out["oos_sign_consistent"] = False
    out["oos_sign_consistent"] = out["oos_sign_consistent"].astype(bool)
    if "oos_positive" not in out.columns:
        out["oos_positive"] = out["oos_mean"] > 0.0
    out["oos_positive"] = out["oos_positive"].astype(bool)
    if "oos_pass" not in out.columns:
        out["oos_pass"] = False
    out["oos_pass"] = out["oos_pass"].astype(bool)

    out["composite_p_value"] = np.maximum(out["hac_p"], out["bootstrap_p"]).clip(lower=0.0, upper=1.0)
    out["fdr_q_value"] = _bh_adjust(out["composite_p_value"]).astype(float)

    out["gate_legacy_survivor"] = (
        (out["event_samples"] >= int(min_samples))
        & (out["event_mean"] > 0.0)
        & (out["event_t"] >= float(legacy_tstat_threshold))
    )
    out["gate_robust_survivor"] = (
        (out["event_samples"] >= int(min_samples))
        & (out["event_mean"] > 0.0)
        & (out["hac_t"] >= float(robust_hac_t_threshold))
        & (out["bootstrap_p"] <= float(bootstrap_alpha))
        & (out["fdr_q_value"] <= float(fdr_q))
        & (out["oos_samples"] >= int(oos_min_samples))
        & ((not int(require_oos_positive)) | out["oos_positive"])
        & ((not int(require_oos_sign_consistency)) | out["oos_sign_consistent"])
    )
    out["gate_oos"] = (
        (out["oos_samples"] >= int(oos_min_samples))
        & ((not int(require_oos_positive)) | out["oos_positive"])
        & ((not int(require_oos_sign_consistency)) | out["oos_sign_consistent"])
    )
    return out


def _tail_report(returns: pd.Series) -> Dict[str, float]:
    clean = returns.dropna().astype(float)
    if clean.empty:
        return {
            "median": 0.0,
            "p25": 0.0,
            "p75": 0.0,
            "top_1pct_contribution": 0.0,
            "top_5pct_contribution": 0.0,
        }
    total = float(clean.sum())
    sorted_desc = clean.sort_values(ascending=False)
    n = len(sorted_desc)
    n1 = max(1, int(np.ceil(n * 0.01)))
    n5 = max(1, int(np.ceil(n * 0.05)))
    top1 = float(sorted_desc.iloc[:n1].sum())
    top5 = float(sorted_desc.iloc[:n5].sum())
    denom = total if total != 0.0 else np.nan
    return {
        "median": float(clean.median()),
        "p25": float(clean.quantile(0.25)),
        "p75": float(clean.quantile(0.75)),
        "top_1pct_contribution": float(top1 / denom) if np.isfinite(denom) else 0.0,
        "top_5pct_contribution": float(top5 / denom) if np.isfinite(denom) else 0.0,
    }


def _load_symbol_features(symbol: str, run_id: str) -> pd.DataFrame:
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "features", "perp", symbol, "5m", "features_v1"),
        DATA_ROOT / "lake" / "features" / "perp" / symbol / "5m" / "features_v1",
    ]
    features_dir = choose_partition_dir(candidates)
    files = list_parquet_files(features_dir) if features_dir else []
    if not files:
        raise ValueError(f"No features found for {symbol}: {candidates[0]}")
    df = read_parquet(files)
    if df.empty:
        raise ValueError(f"Empty features for {symbol}")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df.sort_values("timestamp").reset_index(drop=True)


def _build_features(df: pd.DataFrame, htf_window: int, htf_lookback: int, funding_pct_window: int) -> pd.DataFrame:
    rv_pct_col = _pick_window_column(df.columns, "rv_pct_")
    range_med_col = _pick_window_column(df.columns, "range_med_")

    close = df["close"].astype(float)
    htf_ma = close.rolling(window=htf_window, min_periods=htf_window).mean()
    htf_delta = htf_ma - htf_ma.shift(htf_lookback)
    trend_state = pd.Series(np.where(htf_delta > 0, 1, np.where(htf_delta < 0, -1, 0)), index=df.index)

    funding_pct = _rolling_percentile(df["funding_rate_scaled"].astype(float), funding_pct_window)
    funding_bucket = pd.Series(
        np.select(
            [funding_pct <= 20, funding_pct >= 80],
            ["low", "high"],
            default="mid",
        ),
        index=df.index,
    ).where(funding_pct.notna())

    compression = ((df[rv_pct_col] <= 10.0) & (df["range_96"] <= 0.8 * df[range_med_col])).fillna(False)

    out = df.copy()
    out["trend_state"] = trend_state
    out["funding_bucket"] = funding_bucket
    out["compression"] = compression
    out["prior_high_96"] = out["high_96"].shift(1)
    out["prior_low_96"] = out["low_96"].shift(1)
    out["breakout_up"] = out["close"] > out["prior_high_96"]
    out["breakout_down"] = out["close"] < out["prior_low_96"]
    out["breakout_any"] = out["breakout_up"] | out["breakout_down"]
    out["vol_q"] = pd.qcut(out["rv_96"], q=4, labels=["Q1", "Q2", "Q3", "Q4"], duplicates="drop")
    out["bull_bear"] = np.where(close / close.shift(96) - 1.0 >= 0, "bull", "bear")
    return out


def _leakage_check(df: pd.DataFrame, htf_window: int, htf_lookback: int) -> Dict[str, object]:
    close = df["close"].astype(float)
    full_ma = close.rolling(window=htf_window, min_periods=htf_window).mean()
    full_delta = full_ma - full_ma.shift(htf_lookback)
    full_trend = pd.Series(np.where(full_delta > 0, 1, np.where(full_delta < 0, -1, 0)), index=df.index)

    rng = np.random.default_rng(7)
    candidates = np.arange(htf_window + htf_lookback, len(df))
    if len(candidates) == 0:
        return {"pass": False, "checked": 0, "mismatches": 0}
    sample = rng.choice(candidates, size=min(500, len(candidates)), replace=False)

    mismatches = 0
    for i in sample:
        partial = close.iloc[: i + 1]
        ma = partial.rolling(window=htf_window, min_periods=htf_window).mean()
        delta = ma - ma.shift(htf_lookback)
        trend_i = int(np.sign(delta.iloc[-1])) if pd.notna(delta.iloc[-1]) else 0
        if trend_i != int(full_trend.iloc[i]):
            mismatches += 1
    return {"pass": mismatches == 0, "checked": int(len(sample)), "mismatches": int(mismatches)}


def _extract_compression_events(df: pd.DataFrame, symbol: str, max_duration: int) -> List[CompressionEvent]:
    events: List[CompressionEvent] = []
    n = len(df)
    i = 1
    while i < n:
        if not bool(df.at[i, "compression"]) or bool(df.at[i - 1, "compression"]):
            i += 1
            continue

        start = i
        max_end = min(n - 1, start + max_duration - 1)
        end = start
        end_reason = "max_duration"

        j = start
        while j <= max_end:
            if bool(df.at[j, "breakout_any"]):
                end = j
                end_reason = "breakout"
                break
            if not bool(df.at[j, "compression"]):
                end = j
                end_reason = "compression_off"
                break
            end = j
            j += 1

        ts = df.at[start, "timestamp"]
        vol_q = df.at[start, "vol_q"]
        events.append(
            CompressionEvent(
                symbol=symbol,
                start_idx=start,
                end_idx=end,
                end_reason=end_reason,
                trend_state=int(df.at[start, "trend_state"]) if pd.notna(df.at[start, "trend_state"]) else 0,
                funding_bucket=str(df.at[start, "funding_bucket"]) if pd.notna(df.at[start, "funding_bucket"]) else "na",
                year=int(ts.year),
                vol_q=str(vol_q) if pd.notna(vol_q) else "na",
                bull_bear=str(df.at[start, "bull_bear"]),
                enter_ts=pd.to_datetime(ts, utc=True),
            )
        )
        i = end + 1
    return events


def _first_expansion_after(df: pd.DataFrame, idx: int, lookahead: int) -> Tuple[int | None, int]:
    n = len(df)
    end = min(n - 1, idx + lookahead)
    for j in range(idx + 1, end + 1):
        if bool(df.at[j, "breakout_up"]):
            return j, 1
        if bool(df.at[j, "breakout_down"]):
            return j, -1
    return None, 0


def _event_rows(df: pd.DataFrame, events: List[CompressionEvent], horizons: List[int], expansion_lookahead: int, mfe_horizon: int) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    close = df["close"].to_numpy(dtype=float)
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    n = len(df)

    for ev in events:
        expansion_idx = ev.end_idx if bool(df.at[ev.end_idx, "breakout_any"]) else None
        breakout_dir = 1 if bool(df.at[ev.end_idx, "breakout_up"]) else -1 if bool(df.at[ev.end_idx, "breakout_down"]) else 0
        if expansion_idx is None:
            expansion_idx, breakout_dir = _first_expansion_after(df, ev.end_idx, expansion_lookahead)

        time_to_expansion = (expansion_idx - ev.start_idx) if expansion_idx is not None else np.nan
        aligns = bool(breakout_dir == ev.trend_state) if breakout_dir != 0 and ev.trend_state != 0 else np.nan

        mfe = np.nan
        mfe_end = min(n - 1, ev.end_idx + mfe_horizon)
        if breakout_dir != 0 and ev.end_idx + 1 <= mfe_end:
            entry = close[ev.end_idx]
            if breakout_dir > 0:
                mfe = float(np.nanmax(high[ev.end_idx + 1 : mfe_end + 1]) / entry - 1.0)
            else:
                mfe = float(entry / np.nanmin(low[ev.end_idx + 1 : mfe_end + 1]) - 1.0)

        for h in horizons:
            if ev.end_idx + h >= n:
                continue
            ret = float(close[ev.end_idx + h] / close[ev.end_idx] - 1.0)
            directional_ret = float(ret * ev.trend_state) if ev.trend_state != 0 else np.nan
            rows.append(
                {
                    "symbol": ev.symbol,
                    "event_start_idx": ev.start_idx,
                    "enter_ts": ev.enter_ts,
                    "split_label": "",
                    "year": ev.year,
                    "vol_q": ev.vol_q,
                    "bull_bear": ev.bull_bear,
                    "funding_bucket": ev.funding_bucket,
                    "horizon": h,
                    "end_reason": ev.end_reason,
                    "trend_state": ev.trend_state,
                    "breakout_dir": breakout_dir,
                    "breakout_aligns_htf": aligns,
                    "time_to_expansion_bars": time_to_expansion,
                    "mfe_post_end": mfe,
                    "event_return": ret,
                    "event_directional_return": directional_ret,
                }
            )
    return rows


def _split_sign_report(events: pd.DataFrame, col: str, ret_col: str) -> Dict[str, object]:
    if events.empty:
        return {"stable_sign": False, "groups": {}}
    grouped = events.groupby(col, dropna=False)[ret_col].mean().dropna()
    groups = {str(k): float(v) for k, v in grouped.items()}
    if grouped.empty:
        return {"stable_sign": False, "groups": groups}
    positive = grouped > 0
    stable_sign = bool(positive.all() or (~positive).all())
    return {"stable_sign": stable_sign, "groups": groups}


def _bar_condition_stats(df: pd.DataFrame, condition: str, horizon: int) -> Dict[str, float]:
    close = df["close"].astype(float)
    fwd = close.shift(-horizon) / close - 1.0

    if condition == "compression":
        mask = df["compression"]
        ret = fwd.where(mask)
    elif condition == "compression_plus_htf_trend":
        mask = df["compression"] & (df["trend_state"] != 0)
        ret = (fwd * df["trend_state"]).where(mask)
    elif condition == "compression_plus_funding_low":
        mask = df["compression"] & (df["funding_bucket"] == "low")
        ret = fwd.where(mask)
    else:
        raise ValueError(f"Unknown condition: {condition}")

    return _distribution_stats(ret)


def _event_condition_frame(events_df: pd.DataFrame, condition: str, horizon: int) -> Tuple[pd.DataFrame, str]:
    ret_col = "event_directional_return" if condition == "compression_plus_htf_trend" else "event_return"

    if events_df.empty or "horizon" not in events_df.columns:
        return pd.DataFrame(columns=EVENT_ROW_COLUMNS), ret_col

    frame = events_df[events_df["horizon"] == horizon].copy()
    if condition == "compression":
        pass
    elif condition == "compression_plus_htf_trend":
        frame = frame[frame["trend_state"] != 0]
    elif condition == "compression_plus_funding_low":
        frame = frame[frame["funding_bucket"] == "low"]
    else:
        raise ValueError(f"Unknown condition: {condition}")

    return frame, ret_col


def _split_overlap_diagnostics(events_df: pd.DataFrame, embargo_bars: int) -> Dict[str, object]:
    if events_df.empty:
        return {"pass": False, "embargo_bars": int(embargo_bars), "details": []}

    unique_events = events_df.drop_duplicates(subset=["symbol", "event_start_idx"]).copy()
    details: List[Dict[str, object]] = []
    global_pass = True

    for symbol, group in unique_events.groupby("symbol", dropna=False):
        g = group.sort_values("event_start_idx").reset_index(drop=True)
        boundary_gaps: Dict[str, int] = {}
        for left, right in [("train", "validation"), ("validation", "test")]:
            left_idx = g.index[g["split_label"] == left]
            right_idx = g.index[g["split_label"] == right]
            if len(left_idx) == 0 or len(right_idx) == 0:
                boundary_gaps[f"{left}_to_{right}"] = -1
                global_pass = False
                continue
            gap = int(right_idx.min() - left_idx.max() - 1)
            boundary_gaps[f"{left}_to_{right}"] = gap
            if gap < int(embargo_bars):
                global_pass = False

        details.append({"symbol": str(symbol), "boundary_gaps": boundary_gaps})

    return {"pass": bool(global_pass), "embargo_bars": int(embargo_bars), "details": details}


def _parameter_stability_diagnostics(
    trap_df: pd.DataFrame,
    *,
    base_min_samples: int,
    base_tstat_threshold: float,
    sample_delta: int,
    tstat_delta: float,
) -> Dict[str, object]:
    if trap_df.empty:
        return {
            "pass": False,
            "rank_consistency": 0.0,
            "performance_decay": 1.0,
            "neighborhood_supported": False,
            "scenarios": [],
        }

    scenarios = [
        {"name": "base", "min_samples": int(base_min_samples), "tstat": float(base_tstat_threshold)},
        {"name": "tight", "min_samples": int(base_min_samples + sample_delta), "tstat": float(base_tstat_threshold + tstat_delta)},
        {"name": "loose", "min_samples": max(1, int(base_min_samples - sample_delta)), "tstat": max(0.0, float(base_tstat_threshold - tstat_delta))},
    ]

    def _survivor_frame(min_samples: int, tstat: float) -> pd.DataFrame:
        sub = trap_df[(trap_df["event_samples"] >= min_samples) & (trap_df["event_mean"] > 0) & (trap_df["event_t"] >= tstat)]
        return sub.copy()

    def _survivor_set(sub: pd.DataFrame) -> set[str]:
        return {f"{r.condition}|{int(r.horizon)}" for r in sub.itertuples(index=False)}

    base_sub = _survivor_frame(int(base_min_samples), float(base_tstat_threshold))
    base_set = _survivor_set(base_sub)
    rows = []
    overlap_scores = []
    scenario_perf: Dict[str, float] = {}
    for sc in scenarios:
        sub = _survivor_frame(int(sc["min_samples"]), float(sc["tstat"]))
        sset = _survivor_set(sub)
        denom = max(1, len(base_set | sset))
        jaccard = float(len(base_set & sset) / denom)
        overlap_scores.append(jaccard)
        mean_perf = float(sub["event_mean"].mean()) if not sub.empty else np.nan
        scenario_perf[str(sc["name"])] = mean_perf
        rows.append(
            {
                **sc,
                "survivors": len(sset),
                "jaccard_to_base": jaccard,
                "mean_event_return": (None if np.isnan(mean_perf) else mean_perf),
            }
        )

    rank_consistency = float(np.mean(overlap_scores)) if overlap_scores else 0.0
    base_perf = scenario_perf.get("base", np.nan)
    valid_perf = [v for v in scenario_perf.values() if np.isfinite(v)]
    if np.isfinite(base_perf) and base_perf > 0.0 and valid_perf:
        worst_perf = float(min(valid_perf))
        performance_decay = float(max(0.0, (base_perf - worst_perf) / max(abs(base_perf), 1e-9)))
    else:
        performance_decay = 1.0

    neighborhood_supported = any(
        (row.get("name") != "base") and (int(row.get("survivors", 0)) > 0) for row in rows
    )
    passed = bool(
        len(base_set) > 0
        and neighborhood_supported
        and rank_consistency >= 0.3
        and performance_decay <= 1.0
    )
    return {
        "pass": passed,
        "rank_consistency": rank_consistency,
        "performance_decay": performance_decay,
        "neighborhood_supported": bool(neighborhood_supported),
        "scenarios": rows,
    }


def _capacity_diagnostics(events_df: pd.DataFrame, symbols: List[str], min_events_per_day: float) -> Dict[str, object]:
    if events_df.empty:
        return {"pass": False, "estimated_events_per_day": 0.0, "symbol_details": []}
    frame = events_df.copy()
    frame["date"] = pd.to_datetime(frame["enter_ts"], utc=True, errors="coerce").dt.floor("D")
    per_day = frame.groupby(["symbol", "date"], dropna=True).size().reset_index(name="event_count")
    details = []
    for sym in symbols:
        sym_rows = per_day[per_day["symbol"] == sym]
        avg_events = float(sym_rows["event_count"].mean()) if not sym_rows.empty else 0.0
        details.append({"symbol": sym, "avg_events_per_day": avg_events})
    est = float(np.mean([d["avg_events_per_day"] for d in details])) if details else 0.0
    return {
        "pass": bool(est >= float(min_events_per_day)),
        "estimated_events_per_day": est,
        "threshold_events_per_day": float(min_events_per_day),
        "symbol_details": details,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate conditional expectancy against statistical traps (event-level)")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--horizons", default="4,16,96")
    parser.add_argument("--htf_window", type=int, default=384)
    parser.add_argument("--htf_lookback", type=int, default=96)
    parser.add_argument("--funding_pct_window", type=int, default=2880)
    parser.add_argument("--max_event_duration", type=int, default=96)
    parser.add_argument("--expansion_lookahead", type=int, default=192)
    parser.add_argument("--mfe_horizon", type=int, default=96)
    parser.add_argument("--gate_profile", choices=["discovery", "promotion", "custom"], default="discovery")
    parser.add_argument("--tstat_threshold", type=float, default=2.0)
    parser.add_argument("--min_samples", type=int, default=100)
    parser.add_argument("--robust_hac_t_threshold", type=float, default=1.96)
    parser.add_argument("--robust_bootstrap_alpha", type=float, default=0.10)
    parser.add_argument("--robust_fdr_q", type=float, default=0.10)
    parser.add_argument("--robust_hac_max_lag", type=int, default=12)
    parser.add_argument("--robust_bootstrap_iters", type=int, default=800)
    parser.add_argument("--robust_bootstrap_block_size", type=int, default=8)
    parser.add_argument("--robust_bootstrap_seed", type=int, default=7)
    parser.add_argument("--oos_min_samples", type=int, default=40)
    parser.add_argument("--require_oos_positive", type=int, default=1)
    parser.add_argument("--require_oos_sign_consistency", type=int, default=1)
    parser.add_argument("--embargo_bars", type=int, default=0)
    parser.add_argument("--stability_sample_delta", type=int, default=20)
    parser.add_argument("--stability_tstat_delta", type=float, default=0.5)
    parser.add_argument("--capacity_min_events_per_day", type=float, default=0.5)
    parser.add_argument("--out_dir", default=None)
    args = parser.parse_args()
    args = _apply_gate_profile_defaults(args)

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    horizons = _parse_horizons(args.horizons)

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "expectancy" / args.run_id
    ensure_dir(out_dir)

    leakage = {}
    all_bar_df = []
    all_event_rows: List[Dict[str, object]] = []
    event_summary_rows: List[Dict[str, object]] = []

    for symbol in symbols:
        df = _load_symbol_features(symbol, run_id=args.run_id)
        df = _build_features(df, args.htf_window, args.htf_lookback, args.funding_pct_window)
        leakage[symbol] = _leakage_check(df, args.htf_window, args.htf_lookback)
        events = _extract_compression_events(df, symbol=symbol, max_duration=args.max_event_duration)
        rows = _event_rows(
            df,
            events,
            horizons=horizons,
            expansion_lookahead=args.expansion_lookahead,
            mfe_horizon=args.mfe_horizon,
        )
        all_event_rows.extend(rows)
        all_bar_df.append(df)

        breakout_count = sum(1 for e in events if e.end_reason == "breakout")
        event_summary_rows.append(
            {
                "symbol": symbol,
                "event_count": len(events),
                "breakout_end_count": breakout_count,
                "breakout_end_rate": float(breakout_count / len(events)) if events else 0.0,
            }
        )

    master_bars = pd.concat(all_bar_df, ignore_index=True)
    events_df = pd.DataFrame(all_event_rows, columns=EVENT_ROW_COLUMNS)
    if not events_df.empty:
        events_df["enter_ts"] = pd.to_datetime(events_df["enter_ts"], utc=True, errors="coerce")
        events_df["split_label"] = build_walk_forward_split_labels(events_df, time_col="enter_ts", symbol_col="symbol")

    split_overlap = _split_overlap_diagnostics(events_df, embargo_bars=args.embargo_bars)

    conditions = ["compression", "compression_plus_htf_trend", "compression_plus_funding_low"]
    trap_rows = []
    split_rows = []
    tail_rows = []
    symmetry_rows = []
    expansion_rows = []

    rng = np.random.default_rng(11)

    for condition in conditions:
        for horizon in horizons:
            bar_stats = _bar_condition_stats(master_bars, condition, horizon)
            event_frame, ret_col = _event_condition_frame(events_df, condition, horizon)
            event_series = event_frame[ret_col] if ret_col in event_frame else pd.Series(dtype=float)
            event_stats = _distribution_stats(event_series)
            robust_fields = _robust_row_fields(
                event_frame=event_frame,
                ret_col=ret_col,
                condition=condition,
                horizon=int(horizon),
                hac_max_lag=int(args.robust_hac_max_lag),
                bootstrap_block_size=int(args.robust_bootstrap_block_size),
                bootstrap_iters=int(args.robust_bootstrap_iters),
                bootstrap_seed=int(args.robust_bootstrap_seed),
                oos_min_samples=int(args.oos_min_samples),
                require_oos_positive=int(args.require_oos_positive),
                require_oos_sign_consistency=int(args.require_oos_sign_consistency),
            )

            trap_rows.append(
                {
                    "condition": condition,
                    "horizon": horizon,
                    "bar_samples": bar_stats["samples"],
                    "bar_mean": bar_stats["mean_return"],
                    "bar_t": bar_stats["t_stat"],
                    "event_samples": event_stats["samples"],
                    "event_mean": event_stats["mean_return"],
                    "event_t": event_stats["t_stat"],
                    **robust_fields,
                }
            )

            year_split = _split_sign_report(event_frame, "year", ret_col)
            vol_split = _split_sign_report(event_frame, "vol_q", ret_col)
            bull_split = _split_sign_report(event_frame, "bull_bear", ret_col)

            split_rows.append(
                {
                    "condition": condition,
                    "horizon": horizon,
                    "year_stable_sign": year_split["stable_sign"],
                    "vol_q_stable_sign": vol_split["stable_sign"],
                    "bull_bear_stable_sign": bull_split["stable_sign"],
                    "year_means": year_split["groups"],
                    "vol_q_means": vol_split["groups"],
                    "bull_bear_means": bull_split["groups"],
                }
            )

            tail = _tail_report(event_frame[ret_col] if ret_col in event_frame else pd.Series(dtype=float))
            tail_rows.append(
                {
                    "condition": condition,
                    "horizon": horizon,
                    "mean": event_stats["mean_return"],
                    "median": tail["median"],
                    "p25": tail["p25"],
                    "p75": tail["p75"],
                    "top_1pct_contribution": tail["top_1pct_contribution"],
                    "top_5pct_contribution": tail["top_5pct_contribution"],
                }
            )

            if condition == "compression_plus_htf_trend":
                base = event_frame[ret_col].dropna()
                opp = -base
                rand_sign = pd.Series(rng.choice([-1.0, 1.0], size=len(base)), index=base.index)
                rnd = base.abs() * rand_sign
                symmetry_rows.append(
                    {
                        "condition": condition,
                        "horizon": horizon,
                        "base_mean": float(base.mean()) if len(base) else 0.0,
                        "base_t": _distribution_stats(base)["t_stat"],
                        "opposite_mean": float(opp.mean()) if len(opp) else 0.0,
                        "opposite_t": _distribution_stats(opp)["t_stat"],
                        "random_mean": float(rnd.mean()) if len(rnd) else 0.0,
                        "random_t": _distribution_stats(rnd)["t_stat"],
                    }
                )

        cond_all, _ = _event_condition_frame(events_df, condition, horizons[0])
        cond_all = cond_all.drop_duplicates(subset=["symbol", "year", "vol_q", "bull_bear", "time_to_expansion_bars", "mfe_post_end", "trend_state", "funding_bucket", "end_reason", "breakout_dir"]) if not cond_all.empty else cond_all
        expansion_rows.append(
            {
                "condition": condition,
                "events": int(len(cond_all)),
                "time_to_expansion_median": float(cond_all["time_to_expansion_bars"].median()) if not cond_all.empty else np.nan,
                "time_to_expansion_p25": float(cond_all["time_to_expansion_bars"].quantile(0.25)) if not cond_all.empty else np.nan,
                "time_to_expansion_p75": float(cond_all["time_to_expansion_bars"].quantile(0.75)) if not cond_all.empty else np.nan,
                "mfe_median": float(cond_all["mfe_post_end"].median()) if not cond_all.empty else np.nan,
                "mfe_mean": float(cond_all["mfe_post_end"].mean()) if not cond_all.empty else np.nan,
                "breakout_align_rate": float(cond_all["breakout_aligns_htf"].dropna().mean()) if not cond_all.empty else np.nan,
            }
        )

    trap_df = pd.DataFrame(trap_rows)
    trap_df = _apply_robust_survivor_gates(
        trap_df,
        min_samples=int(args.min_samples),
        legacy_tstat_threshold=float(args.tstat_threshold),
        robust_hac_t_threshold=float(args.robust_hac_t_threshold),
        bootstrap_alpha=float(args.robust_bootstrap_alpha),
        fdr_q=float(args.robust_fdr_q),
        oos_min_samples=int(args.oos_min_samples),
        require_oos_positive=int(args.require_oos_positive),
        require_oos_sign_consistency=int(args.require_oos_sign_consistency),
    )
    split_df = pd.DataFrame(split_rows)
    tail_df = pd.DataFrame(tail_rows)
    symmetry_df = pd.DataFrame(symmetry_rows)
    expansion_df = pd.DataFrame(expansion_rows)
    event_summary_df = pd.DataFrame(event_summary_rows)

    legacy_survivors = trap_df[trap_df["gate_legacy_survivor"]].copy()
    survivors = trap_df[trap_df["gate_robust_survivor"]].copy()
    stability = _parameter_stability_diagnostics(
        trap_df,
        base_min_samples=args.min_samples,
        base_tstat_threshold=args.tstat_threshold,
        sample_delta=args.stability_sample_delta,
        tstat_delta=args.stability_tstat_delta,
    )
    capacity = _capacity_diagnostics(events_df, symbols=symbols, min_events_per_day=args.capacity_min_events_per_day)

    payload = {
        "run_id": args.run_id,
        "symbols": symbols,
        "horizons": horizons,
        "config": {
            "max_event_duration": args.max_event_duration,
            "expansion_lookahead": args.expansion_lookahead,
            "mfe_horizon": args.mfe_horizon,
            "embargo_bars": args.embargo_bars,
            "gate_profile": args.gate_profile,
            "survivor_definition": "promotion_grade_v1",
            "min_samples": args.min_samples,
            "legacy_tstat_threshold": args.tstat_threshold,
            "robust_hac_t_threshold": args.robust_hac_t_threshold,
            "robust_bootstrap_alpha": args.robust_bootstrap_alpha,
            "robust_fdr_q": args.robust_fdr_q,
            "robust_hac_max_lag": args.robust_hac_max_lag,
            "robust_bootstrap_iters": args.robust_bootstrap_iters,
            "robust_bootstrap_block_size": args.robust_bootstrap_block_size,
            "robust_bootstrap_seed": args.robust_bootstrap_seed,
            "oos_min_samples": args.oos_min_samples,
            "require_oos_positive": args.require_oos_positive,
            "require_oos_sign_consistency": args.require_oos_sign_consistency,
            "stability_sample_delta": args.stability_sample_delta,
            "stability_tstat_delta": args.stability_tstat_delta,
            "capacity_min_events_per_day": args.capacity_min_events_per_day,
        },
        "survivor_definition": "promotion_grade_v1",
        "event_summary": event_summary_df.to_dict(orient="records"),
        "trap_1_overlap_bar_vs_event": trap_df.to_dict(orient="records"),
        "trap_2_leakage": {"feature_leakage": leakage, "split_overlap": split_overlap},
        "trap_3_regimes": split_df.to_dict(orient="records"),
        "trap_4_tails": tail_df.to_dict(orient="records"),
        "trap_5_symmetry": symmetry_df.to_dict(orient="records"),
        "event_expansion_metrics": expansion_df.to_dict(orient="records"),
        "stability_diagnostics": stability,
        "capacity_diagnostics": capacity,
        "survivors_legacy": legacy_survivors.to_dict(orient="records"),
        "survivors": survivors.to_dict(orient="records"),
    }

    json_path = out_dir / "conditional_expectancy_robustness.json"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    lines = [
        "# Conditional Expectancy Robustness (Event-Level)",
        "",
        f"Run ID: `{args.run_id}`",
        "",
        "## Event Summary",
        event_summary_df.to_markdown(index=False) if not event_summary_df.empty else "No rows",
        "",
        "## Survivors (event-level)",
    ]
    if survivors.empty:
        lines.append("No condition survived promotion-grade robust thresholds.")
    else:
        lines.append(survivors.to_markdown(index=False))
    lines.extend(["", f"Legacy survivors (pre-upgrade): **{len(legacy_survivors)}**"])

    lines.extend(["", "## Trap 2 Leakage", json.dumps({"feature_leakage": leakage, "split_overlap": split_overlap}, indent=2), ""])
    lines.extend(["## Trap 1 Overlap (Bar vs Event)", trap_df.to_markdown(index=False) if not trap_df.empty else "No rows", ""])
    lines.extend(["## Trap 3 Regime Stability", split_df.to_markdown(index=False) if not split_df.empty else "No rows", ""])
    lines.extend(["## Trap 4 Tail Dominance", tail_df.to_markdown(index=False) if not tail_df.empty else "No rows", ""])
    lines.extend(["## Trap 5 Symmetry", symmetry_df.to_markdown(index=False) if not symmetry_df.empty else "No rows", ""])
    lines.extend(["## Event Metrics", expansion_df.to_markdown(index=False) if not expansion_df.empty else "No rows", ""])
    lines.extend(["## Stability Diagnostics", json.dumps(stability, indent=2), ""])
    lines.extend(["## Capacity Diagnostics", json.dumps(capacity, indent=2), ""])

    md_path = out_dir / "conditional_expectancy_robustness.md"
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")
    print(f"Survivors: {len(survivors)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
