"""
Tests for _compute_join_rate in validate_event_quality.py.

Key invariant under test
------------------------
Events carry sub-bar timestamps (e.g. 10:03:17 UTC) that will NEVER match a
5-minute bar boundary (10:00:00, 10:05:00, …) under exact timestamp equality.
The function must use merge_asof(direction="backward") so that every event
that falls within the feature table's time range still joins successfully.

A test that fails when exact equality is used is included explicitly.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Allow importing from project/ without installing the package.
PROJECT_ROOT = Path(__file__).resolve().parents[1] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research.validate_event_quality import _compute_join_rate


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bar_features(n_bars: int = 20, bar_seconds: int = 300) -> pd.DataFrame:
    """Return a minimal features DataFrame with bar-aligned timestamps."""
    base = pd.Timestamp("2024-01-01 00:00:00", tz="UTC")
    timestamps = [base + pd.Timedelta(seconds=bar_seconds * i) for i in range(n_bars)]
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "close": np.linspace(100.0, 110.0, n_bars),
        }
    )


def _events_at_bar_boundaries(features_df: pd.DataFrame) -> pd.DataFrame:
    """Events whose timestamps exactly match feature bar boundaries."""
    return pd.DataFrame({"timestamp": features_df["timestamp"].values.copy()})


def _events_offset_within_bars(features_df: pd.DataFrame, offset_seconds: int = 30) -> pd.DataFrame:
    """
    Events whose timestamps are offset_seconds AFTER each bar boundary.
    These will NEVER match exactly but should still join via merge_asof(backward).
    """
    shifted = features_df["timestamp"] + pd.Timedelta(seconds=offset_seconds)
    return pd.DataFrame({"timestamp": shifted.values})


def _exact_join_rate_reference(events_df: pd.DataFrame, features_df: pd.DataFrame) -> float:
    """
    Reference implementation using EXACT timestamp equality — the wrong approach.
    Used only to prove that exact equality fails on offset events.
    """
    ts_col = "timestamp" if "timestamp" in events_df.columns else "enter_ts"
    event_ts = pd.to_datetime(events_df[ts_col], utc=True, errors="coerce").dropna()
    feat_ts_set = set(features_df["timestamp"])
    matched = sum(1 for ts in event_ts if ts in feat_ts_set)
    return float(matched / len(event_ts)) if len(event_ts) > 0 else 0.0


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestComputeJoinRateEmpty:
    def test_empty_events_returns_zeros(self):
        features = _bar_features()
        result = _compute_join_rate(pd.DataFrame(), features, horizons_bars=[1, 3])
        assert result["features"] == 0.0
        assert result["label_1b"] == 0.0
        assert result["label_3b"] == 0.0

    def test_empty_features_returns_zeros(self):
        events = pd.DataFrame({"timestamp": [pd.Timestamp("2024-01-01 00:00:30", tz="UTC")]})
        result = _compute_join_rate(events, pd.DataFrame(), horizons_bars=[1])
        assert result["features"] == 0.0

    def test_missing_timestamp_column_returns_zeros(self):
        events = pd.DataFrame({"bar_idx": [0, 1, 2]})  # no timestamp column
        features = _bar_features()
        result = _compute_join_rate(events, features, horizons_bars=[1])
        assert result["features"] == 0.0


class TestFeatureJoinRateExactTimestamps:
    """When event timestamps exactly match bar boundaries, join rate should be 1.0."""

    def test_exact_timestamps_full_join(self):
        features = _bar_features(n_bars=10)
        events = _events_at_bar_boundaries(features)
        result = _compute_join_rate(events, features, horizons_bars=[1])
        assert result["features"] == pytest.approx(1.0)

    def test_partial_exact_match(self):
        features = _bar_features(n_bars=10)
        # Only first 5 bars match exactly; remaining 5 are offset.
        exact = features["timestamp"].iloc[:5].tolist()
        offset = (features["timestamp"].iloc[5:] + pd.Timedelta(seconds=30)).tolist()
        events = pd.DataFrame({"timestamp": exact + offset})
        result = _compute_join_rate(events, features, horizons_bars=[1])
        # All 10 should join via merge_asof (offset ones match the bar before them).
        assert result["features"] == pytest.approx(1.0)


class TestFeatureJoinRateSubBarTimestamps:
    """
    Core invariant: events with sub-bar timestamps must still join at 100 %.
    An exact-equality implementation would return 0 % — this test would FAIL
    if the function used set-membership rather than merge_asof.
    """

    def test_30s_offset_achieves_full_join(self):
        features = _bar_features(n_bars=15)
        events = _events_offset_within_bars(features, offset_seconds=30)
        result = _compute_join_rate(events, features, horizons_bars=[1, 3])
        assert result["features"] == pytest.approx(1.0), (
            "Expected 100% feature join rate for events offset 30s within bars. "
            "Exact timestamp equality was used instead of merge_asof(backward)."
        )

    def test_299s_offset_achieves_full_join(self):
        """Even an event arriving 1 second before the next bar must join the prior bar."""
        features = _bar_features(n_bars=15)
        events = _events_offset_within_bars(features, offset_seconds=299)
        result = _compute_join_rate(events, features, horizons_bars=[1])
        assert result["features"] == pytest.approx(1.0)

    def test_exact_equality_would_fail_on_offset_events(self):
        """
        Regression guard: proves that the naive set-membership approach returns 0.
        If this assertion ever fails it means the reference implementation changed
        and this test file needs to be updated accordingly.
        """
        features = _bar_features(n_bars=10)
        events = _events_offset_within_bars(features, offset_seconds=30)
        exact_rate = _exact_join_rate_reference(events, features)
        assert exact_rate == pytest.approx(0.0), (
            "Reference exact-equality join rate should be 0 for offset events. "
            "If this fails, the offset is accidentally landing on a bar boundary."
        )

    def test_events_before_feature_window_do_not_join(self):
        """Events that precede all feature bars should not join (no prior bar exists)."""
        features = _bar_features(n_bars=10)
        # Events one full bar BEFORE the first feature bar.
        too_early = features["timestamp"].min() - pd.Timedelta(seconds=300)
        events = pd.DataFrame({"timestamp": [too_early]})
        result = _compute_join_rate(events, features, horizons_bars=[1])
        assert result["features"] == pytest.approx(0.0)


class TestLabelJoinRateBasedOnMatchedBarIndex:
    """
    Label join rate must be derived from the matched bar's integer position, not
    from a second searchsorted on the event timestamp.
    """

    def test_label_join_rate_uses_matched_pos_not_event_ts(self):
        """
        Setup: 10 bars.  Events are 30 s into each bar (sub-bar offset).
        For horizon h=1 the forward bar (matched_pos + 1) must exist for
        all events except the last bar (matched_pos == 9, future_pos == 10
        which is out of range).  So label_1b should be 9/10 = 0.9.

        An implementation using searchsorted(side="left") on the event
        timestamp would compute pos = insertion point (the next bar) and
        future_pos = next_bar + 1, producing wrong counts.
        """
        features = _bar_features(n_bars=10)
        # Events offset 30s into each bar → matched to bars 0..9.
        events = _events_offset_within_bars(features, offset_seconds=30)
        result = _compute_join_rate(events, features, horizons_bars=[1])
        # matched_pos 0..8 → future_pos 1..9 (valid), matched_pos 9 → future_pos 10 (out of range)
        assert result["label_1b"] == pytest.approx(9 / 10)

    def test_label_h3_accounts_for_matched_pos(self):
        features = _bar_features(n_bars=10)
        events = _events_offset_within_bars(features, offset_seconds=30)
        result = _compute_join_rate(events, features, horizons_bars=[3])
        # matched_pos 0..6 → future_pos 3..9 (valid), 7/8/9 → 10/11/12 (out of range)
        assert result["label_3b"] == pytest.approx(7 / 10)

    def test_label_join_with_nan_close_excluded(self):
        """Close values that are NaN should not count as valid label joins."""
        features = _bar_features(n_bars=10)
        # Corrupt the forward bar for position 1 (matched to event 0, horizon 1).
        features.loc[1, "close"] = float("nan")
        events = _events_offset_within_bars(features, offset_seconds=30)
        result = _compute_join_rate(events, features, horizons_bars=[1])
        # Event at matched_pos=0 → future_pos=1 (NaN close → invalid)
        # Events at matched_pos=1..8 → future_pos=2..9 (valid close)
        # Event at matched_pos=9 → future_pos=10 (out of range)
        # Valid = 8, total = 10
        assert result["label_1b"] == pytest.approx(8 / 10)

    def test_no_close_column_gives_zero_label_rate(self):
        features = _bar_features(n_bars=10).drop(columns=["close"])
        events = _events_offset_within_bars(
            _bar_features(n_bars=10), offset_seconds=30
        )
        result = _compute_join_rate(events, features, horizons_bars=[1, 3])
        assert result["label_1b"] == pytest.approx(0.0)
        assert result["label_3b"] == pytest.approx(0.0)

    def test_enter_ts_column_accepted_as_alias(self):
        """The function must accept 'enter_ts' as the timestamp column name."""
        features = _bar_features(n_bars=5)
        events = pd.DataFrame(
            {"enter_ts": _events_offset_within_bars(features, offset_seconds=10)["timestamp"]}
        )
        result = _compute_join_rate(events, features, horizons_bars=[1])
        assert result["features"] == pytest.approx(1.0)
