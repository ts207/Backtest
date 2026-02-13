import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import validate_expectancy_traps


def test_event_condition_frame_handles_empty_events_without_horizon_column() -> None:
    events_df = pd.DataFrame()
    frame, ret_col = validate_expectancy_traps._event_condition_frame(events_df, "compression", horizon=4)
    assert frame.empty
    assert "horizon" in frame.columns
    assert ret_col == "event_return"


def test_event_condition_frame_filters_trend_condition() -> None:
    events_df = pd.DataFrame(
        [
            {"horizon": 4, "trend_state": 1, "funding_bucket": "low", "event_return": 0.01, "event_directional_return": 0.01},
            {"horizon": 4, "trend_state": 0, "funding_bucket": "low", "event_return": 0.02, "event_directional_return": 0.02},
            {"horizon": 16, "trend_state": 1, "funding_bucket": "low", "event_return": 0.03, "event_directional_return": 0.03},
        ]
    )

    frame, ret_col = validate_expectancy_traps._event_condition_frame(
        events_df, "compression_plus_htf_trend", horizon=4
    )
    assert ret_col == "event_directional_return"
    assert len(frame) == 1
    assert int(frame.iloc[0]["trend_state"]) != 0


def test_split_overlap_diagnostics_respects_embargo() -> None:
    events_df = pd.DataFrame(
        [
            {"symbol": "BTCUSDT", "event_start_idx": 1, "split_label": "train"},
            {"symbol": "BTCUSDT", "event_start_idx": 2, "split_label": "validation"},
            {"symbol": "BTCUSDT", "event_start_idx": 3, "split_label": "test"},
        ]
    )

    diag = validate_expectancy_traps._split_overlap_diagnostics(events_df, embargo_bars=1)
    assert diag["pass"] is False
    assert diag["embargo_bars"] == 1


def test_split_overlap_diagnostics_passes_when_gap_satisfies_embargo() -> None:
    events_df = pd.DataFrame(
        [
            {"symbol": "BTCUSDT", "event_start_idx": 1, "split_label": "train"},
            {"symbol": "BTCUSDT", "event_start_idx": 2, "split_label": "train"},
            {"symbol": "BTCUSDT", "event_start_idx": 3, "split_label": "holdout"},
            {"symbol": "BTCUSDT", "event_start_idx": 4, "split_label": "validation"},
            {"symbol": "BTCUSDT", "event_start_idx": 5, "split_label": "holdout"},
            {"symbol": "BTCUSDT", "event_start_idx": 6, "split_label": "test"},
        ]
    )

    diag = validate_expectancy_traps._split_overlap_diagnostics(events_df, embargo_bars=1)
    assert diag["pass"] is True
