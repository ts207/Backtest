from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import List

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SplitWindow:
    label: str
    start: pd.Timestamp
    end: pd.Timestamp

    def to_dict(self) -> dict:
        return {"label": self.label, "start": self.start.isoformat(), "end": self.end.isoformat()}


def _normalize_ts(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tz is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts


def build_time_splits(
    *,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    train_frac: float = 0.6,
    validation_frac: float = 0.2,
    embargo_days: int = 0,
) -> List[SplitWindow]:
    """
    Deterministic walk-forward windows with optional embargo between split boundaries.
    """
    start_ts = _normalize_ts(start)
    end_ts = _normalize_ts(end)
    if start_ts > end_ts:
        raise ValueError("start must be <= end")
    if not (0.0 < float(train_frac) < 1.0):
        raise ValueError("train_frac must be in (0,1)")
    if not (0.0 < float(validation_frac) < 1.0):
        raise ValueError("validation_frac must be in (0,1)")
    if float(train_frac + validation_frac) >= 1.0:
        raise ValueError("train_frac + validation_frac must be < 1")
    if int(embargo_days) < 0:
        raise ValueError("embargo_days must be >= 0")

    total_days = max(1, int((end_ts.normalize() - start_ts.normalize()).days) + 1)
    train_days = max(1, int(np.floor(total_days * float(train_frac))))
    validation_days = max(1, int(np.floor(total_days * float(validation_frac))))
    embargo = int(embargo_days)

    train_start = start_ts
    train_end = min(end_ts, train_start + timedelta(days=train_days) - timedelta(seconds=1))

    validation_start = train_end + timedelta(days=embargo, seconds=1)
    validation_end = min(end_ts, validation_start + timedelta(days=validation_days) - timedelta(seconds=1))

    test_start = validation_end + timedelta(days=embargo, seconds=1)
    test_end = end_ts

    windows: List[SplitWindow] = []
    if train_start <= train_end:
        windows.append(SplitWindow("train", train_start, train_end))
    if validation_start <= validation_end:
        windows.append(SplitWindow("validation", validation_start, validation_end))
    if test_start <= test_end:
        windows.append(SplitWindow("test", test_start, test_end))

    if not windows:
        raise ValueError("No split windows produced for requested range/embargo")
    labels = [w.label for w in windows]
    if labels[0] != "train":
        raise ValueError("Split generation failed: missing train window")
    return windows


def build_time_splits_with_purge(
    *,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    train_frac: float = 0.6,
    validation_frac: float = 0.2,
    embargo_days: int = 0,
    purge_bars: int = 0,
    bar_duration_minutes: int = 5,
) -> List[SplitWindow]:
    """
    Like build_time_splits but trims the tail of each non-test window by purge_bars.
    Purge removes positions whose exit could overlap the embargo/next-split zone.
    purge_bars = max(horizon_bars, entry_lag_bars) + max_feature_lookback_bars.
    """
    if int(purge_bars) < 0:
        raise ValueError("purge_bars must be >= 0")
    windows = build_time_splits(
        start=start, end=end,
        train_frac=train_frac, validation_frac=validation_frac,
        embargo_days=embargo_days,
    )
    if int(purge_bars) == 0:
        return windows

    purge_delta = timedelta(minutes=int(purge_bars) * int(bar_duration_minutes))
    result: List[SplitWindow] = []
    for w in windows:
        if w.label == "test":
            result.append(w)
        else:
            new_end = w.end - purge_delta
            if new_end < w.start:
                raise ValueError(
                    f"purge_bars={purge_bars} with bar_duration_minutes={bar_duration_minutes} "
                    f"exceeds the {w.label} window length. Reduce purge_bars or extend the window."
                )
            result.append(SplitWindow(w.label, w.start, new_end))
    return result
