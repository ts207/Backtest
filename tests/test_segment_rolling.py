import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.features.build_features_v1 import _build_features_frame


def test_segment_rolling_resets_after_gap():
    timestamps = pd.date_range("2024-01-01 00:00", periods=200, freq="15min", tz="UTC")
    close = np.linspace(100.0, 120.0, num=200)
    bars = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": close,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "volume": [1.0] * 200,
            "is_gap": [False] * 200,
            "gap_len": [0] * 200,
            "funding_event_ts": pd.NaT,
            "funding_rate_scaled": 0.0,
        }
    )
    gap_idx = 100
    bars.loc[gap_idx, ["open", "high", "low", "close", "volume"]] = np.nan
    bars.loc[gap_idx, "is_gap"] = True

    features, _, _ = _build_features_frame(
        bars,
        windows={"rv": 5, "rv_pct": 10, "range": 5, "range_med": 10},
    )

    assert pd.isna(features["rv_96"].iloc[4])
    assert pd.notna(features["rv_96"].iloc[5])
    assert pd.isna(features["rv_96"].iloc[gap_idx + 1])
    assert pd.isna(features["rv_96"].iloc[gap_idx + 4])
    assert pd.isna(features["rv_96"].iloc[gap_idx + 5])
    assert pd.notna(features["rv_96"].iloc[gap_idx + 6])
