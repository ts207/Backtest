from __future__ import annotations

from dataclasses import dataclass
from typing import List

import numpy as np
import pandas as pd

FP_DEF_VERSION = "v1"
_PERSISTENCE_PERCENTILE = 85.0
_PERSISTENCE_MIN_BARS = 8
_NORM_DUE_BARS = 96


@dataclass(frozen=True)
class FundingPersistenceConfig:
    def_version: str = FP_DEF_VERSION
    persistence_percentile: float = _PERSISTENCE_PERCENTILE
    persistence_min_bars: int = _PERSISTENCE_MIN_BARS
    norm_due_bars: int = _NORM_DUE_BARS


DEFAULT_FP_CONFIG = FundingPersistenceConfig()
SOURCE_EVENT_TYPE = "FUNDING_PERSISTENCE_TRIGGER"


def _rolling_percentile(series: pd.Series, window: int = 96) -> pd.Series:
    def pct_rank(values: np.ndarray) -> float:
        last = values[-1]
        return float(np.sum(values <= last) / len(values) * 100.0)

    return series.rolling(window=window, min_periods=window).apply(pct_rank, raw=True)


def _contiguous_runs(mask: pd.Series) -> List[tuple[int, int]]:
    runs: List[tuple[int, int]] = []
    start = None
    for idx, is_true in enumerate(mask.astype(bool).tolist()):
        if is_true and start is None:
            start = idx
        elif not is_true and start is not None:
            runs.append((start, idx - 1))
            start = None
    if start is not None:
        runs.append((start, len(mask) - 1))
    return runs


def build_funding_persistence_state(
    frame: pd.DataFrame,
    symbol: str,
    config: FundingPersistenceConfig = DEFAULT_FP_CONFIG,
) -> pd.DataFrame:
    required = {"timestamp", "funding_rate_scaled"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Missing required columns for funding persistence: {sorted(missing)}")

    df = frame[["timestamp", "funding_rate_scaled"]].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)

    funding_abs = df["funding_rate_scaled"].astype(float).abs().fillna(0.0)
    funding_abs_pct = _rolling_percentile(funding_abs, window=96).fillna(0.0)
    is_candidate = funding_abs_pct >= config.persistence_percentile

    active = np.zeros(len(df), dtype=np.int8)
    age = np.zeros(len(df), dtype=np.int32)
    event_id = np.array([None] * len(df), dtype=object)
    enter_ts = np.full(len(df), np.datetime64("NaT"), dtype="datetime64[ns]")
    exit_ts = np.full(len(df), np.datetime64("NaT"), dtype="datetime64[ns]")

    qualifying_runs = []
    for start_idx, end_idx in _contiguous_runs(is_candidate):
        run_len = end_idx - start_idx + 1
        if run_len >= config.persistence_min_bars:
            qualifying_runs.append((start_idx, end_idx))

    for event_num, (start_idx, end_idx) in enumerate(qualifying_runs, start=1):
        event_key = f"fp_{config.def_version}_{symbol}_{event_num:06d}"
        start_ts = df["timestamp"].iat[start_idx]
        stop_ts = df["timestamp"].iat[end_idx]
        for i in range(start_idx, end_idx + 1):
            active[i] = 1
            age[i] = i - start_idx + 1
            event_id[i] = event_key
            enter_ts[i] = start_ts.to_datetime64()
            if i == end_idx:
                exit_ts[i] = stop_ts.to_datetime64()

    out = pd.DataFrame(
        {
            "timestamp": df["timestamp"],
            "fp_def_version": config.def_version,
            "fp_source_event_type": SOURCE_EVENT_TYPE,
            "fp_active": active,
            "fp_age_bars": age,
            "fp_event_id": event_id,
            "fp_enter_ts": pd.to_datetime(enter_ts, utc=True),
            "fp_exit_ts": pd.to_datetime(exit_ts, utc=True),
            "fp_severity": ((funding_abs_pct - config.persistence_percentile) / 100.0).clip(lower=0.0),
            "fp_norm_due": ((active == 1) & (age >= config.norm_due_bars)).astype(np.int8),
        }
    )
    return out
