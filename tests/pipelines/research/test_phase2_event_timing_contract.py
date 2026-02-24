from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research import phase2_candidate_discovery


def test_join_events_prefers_enter_ts_over_timestamp():
    events_df = pd.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "enter_ts": pd.to_datetime(["2026-01-01T00:05:00Z"], utc=True),
        }
    )
    features_df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"],
                utc=True,
            ),
            "close": [100.0, 200.0],
        }
    )

    merged = phase2_candidate_discovery._join_events_to_features(events_df, features_df)

    assert len(merged) == 1
    assert merged.iloc[0]["close"] == 200.0


def test_calculate_expectancy_enforces_entry_lag():
    sym_events = pd.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "enter_ts": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
        }
    )
    features_df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                    "2026-01-01T00:15:00Z",
                ],
                utc=True,
            ),
            "close": [100.0, 50.0, 100.0, 100.0],
        }
    )

    lag1_mean, _, n1, _ = phase2_candidate_discovery.calculate_expectancy(
        sym_events=sym_events,
        features_df=features_df,
        rule="continuation",
        horizon="5m",
        entry_lag_bars=1,
        min_samples=1,
    )
    lag2_mean, _, n2, _ = phase2_candidate_discovery.calculate_expectancy(
        sym_events=sym_events,
        features_df=features_df,
        rule="continuation",
        horizon="5m",
        entry_lag_bars=2,
        min_samples=1,
    )

    assert n1 == 1.0
    assert n2 == 1.0
    assert lag1_mean > lag2_mean


def test_calculate_expectancy_rejects_same_bar_entry():
    sym_events = pd.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "enter_ts": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
        }
    )
    features_df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"], utc=True),
            "close": [100.0, 101.0],
        }
    )

    with pytest.raises(ValueError, match="entry_lag_bars must be >= 1"):
        phase2_candidate_discovery.calculate_expectancy(
            sym_events=sym_events,
            features_df=features_df,
            rule="continuation",
            horizon="5m",
            entry_lag_bars=0,
            min_samples=1,
        )

