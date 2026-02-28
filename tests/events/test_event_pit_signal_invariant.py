"""
Replay harness: assert that _signal flags are never set before the first bar
strictly after detected_ts.

PIT invariant: for each (symbol, event), event_*_signal[t] is True at most at
the first bar > detected_ts, and False for all t' <= detected_ts.

Grid-derived lag: no hardcoded timedelta — the bar interval is whatever the
feature grid says it is. signal_ts in events = detected_ts (nominal); the
actual tradable bar is found by scanning the grid.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from events import registry
from events.registry import _signal_ts_column, _active_signal_column


def _symbol_grid() -> pd.Series:
    return pd.Series(
        pd.to_datetime(
            [
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:05:00Z",
                "2026-01-01T00:10:00Z",
                "2026-01-01T00:15:00Z",
            ],
            utc=True,
        )
    )


def _make_events(
    signal_col: str,
    symbol: str,
    enter_ts_str: str,
    exit_ts_str: str,
    detected_ts_str: str | None = None,
) -> pd.DataFrame:
    enter_ts = pd.to_datetime(enter_ts_str, utc=True)
    exit_ts = pd.to_datetime(exit_ts_str, utc=True)
    detected_ts = pd.to_datetime(detected_ts_str, utc=True) if detected_ts_str else enter_ts
    return pd.DataFrame(
        {
            "signal_column": [signal_col],
            "symbol": [symbol],
            "timestamp": [enter_ts],
            "enter_ts": [enter_ts],
            "phenom_enter_ts": [enter_ts],
            "detected_ts": [detected_ts],
            "signal_ts": [detected_ts],
            "exit_ts": [exit_ts],
            "event_id": ["e1"],
            "event_type": ["VOL_SHOCK"],
        }
    )


def test_signal_flag_absent_at_and_before_detected_ts(monkeypatch):
    """_signal must be False at detected_ts (the bar where event was detected)."""
    monkeypatch.setattr(registry, "_load_symbol_timestamps", lambda **kwargs: _symbol_grid())

    events = _make_events(
        "vol_shock_relaxation_event",
        "BTCUSDT",
        "2026-01-01T00:05:00Z",
        "2026-01-01T00:15:00Z",
    )
    flags = registry.build_event_flags(
        events=events,
        symbols=["BTCUSDT"],
        data_root=Path("/tmp"),
        run_id="r1",
        timeframe="5m",
    )

    signal_col = _signal_ts_column("vol_shock_relaxation_event")
    assert signal_col in flags.columns, f"Expected column '{signal_col}' in flags"

    # detected bar (00:05): _event True, _signal False (not yet tradable)
    row_0005 = flags[flags["timestamp"] == pd.Timestamp("2026-01-01T00:05:00Z")].iloc[0]
    assert bool(row_0005["vol_shock_relaxation_event"]) is True
    assert bool(row_0005[signal_col]) is False

    # first bar strictly after detected_ts = 00:10: _signal True
    row_0010 = flags[flags["timestamp"] == pd.Timestamp("2026-01-01T00:10:00Z")].iloc[0]
    assert bool(row_0010[signal_col]) is True

    # bar before event: _signal False
    row_0000 = flags[flags["timestamp"] == pd.Timestamp("2026-01-01T00:00:00Z")].iloc[0]
    assert bool(row_0000[signal_col]) is False

    # bar after signal bar: _signal False (impulse, not a window)
    row_0015 = flags[flags["timestamp"] == pd.Timestamp("2026-01-01T00:15:00Z")].iloc[0]
    assert bool(row_0015[signal_col]) is False


def test_signal_flag_replay_no_retroactive_set(monkeypatch):
    """
    Sequential replay: scan bars in order and confirm that at every bar t,
    no _signal[t'] was True for t' <= detected_ts.
    """
    monkeypatch.setattr(registry, "_load_symbol_timestamps", lambda **kwargs: _symbol_grid())

    events = _make_events(
        "vol_shock_relaxation_event",
        "BTCUSDT",
        "2026-01-01T00:05:00Z",
        "2026-01-01T00:15:00Z",
    )
    flags = registry.build_event_flags(
        events=events,
        symbols=["BTCUSDT"],
        data_root=Path("/tmp"),
        run_id="r2",
        timeframe="5m",
    )

    signal_col = _signal_ts_column("vol_shock_relaxation_event")
    detected_ts = pd.Timestamp("2026-01-01T00:05:00Z", tz="UTC")
    btc_flags = flags[flags["symbol"] == "BTCUSDT"].sort_values("timestamp").reset_index(drop=True)

    signal_true_rows = btc_flags[btc_flags[signal_col]]
    assert len(signal_true_rows) == 1, (
        f"Expected exactly one _signal bar, got {len(signal_true_rows)}"
    )

    # Signal bar is the first bar strictly after detected_ts
    expected_signal_ts = pd.Timestamp("2026-01-01T00:10:00Z", tz="UTC")
    actual_signal_ts = signal_true_rows.iloc[0]["timestamp"]
    assert actual_signal_ts == expected_signal_ts

    # Replay: no _signal before expected bar
    for _, row in btc_flags.iterrows():
        ts = row["timestamp"]
        if ts <= detected_ts:
            assert not bool(row[signal_col]), (
                f"PIT violation: _signal set at {ts} which is <= detected_ts {detected_ts}"
            )


def test_detected_ts_defaults_to_enter_ts_when_absent(monkeypatch):
    """When events have no detected_ts column, registry falls back to enter_ts."""
    monkeypatch.setattr(registry, "_load_symbol_timestamps", lambda **kwargs: _symbol_grid())

    # Events WITHOUT detected_ts — simulates old analyzer output
    events = pd.DataFrame(
        {
            "signal_column": ["vol_shock_relaxation_event"],
            "symbol": ["BTCUSDT"],
            "timestamp": pd.to_datetime(["2026-01-01T00:05:00Z"], utc=True),
            "enter_ts": pd.to_datetime(["2026-01-01T00:05:00Z"], utc=True),
            "phenom_enter_ts": pd.to_datetime(["2026-01-01T00:05:00Z"], utc=True),
            "signal_ts": pd.to_datetime(["2026-01-01T00:05:00Z"], utc=True),
            "exit_ts": pd.to_datetime(["2026-01-01T00:15:00Z"], utc=True),
            "event_id": ["e_no_det"],
            "event_type": ["VOL_SHOCK"],
        }
    )

    flags = registry.build_event_flags(
        events=events,
        symbols=["BTCUSDT"],
        data_root=Path("/tmp"),
        run_id="r3",
        timeframe="5m",
    )

    signal_col = _signal_ts_column("vol_shock_relaxation_event")
    btc_flags = flags[flags["symbol"] == "BTCUSDT"].sort_values("timestamp").reset_index(drop=True)

    # Should still fire at 00:10 (first bar > enter_ts 00:05)
    row_0010 = btc_flags[btc_flags["timestamp"] == pd.Timestamp("2026-01-01T00:10:00Z")].iloc[0]
    assert bool(row_0010[signal_col]) is True
    row_0005 = btc_flags[btc_flags["timestamp"] == pd.Timestamp("2026-01-01T00:05:00Z")].iloc[0]
    assert bool(row_0005[signal_col]) is False


def test_signal_bar_clamped_to_grid_when_detected_off_grid(monkeypatch):
    """
    When detected_ts is between bars, _signal fires at the first bar strictly
    after detected_ts (grid-snapped forward).
    Event at 00:07 (off-grid); first bar > 00:07 is 00:10.
    """
    monkeypatch.setattr(registry, "_load_symbol_timestamps", lambda **kwargs: _symbol_grid())

    detected_ts = pd.Timestamp("2026-01-01T00:07:00Z", tz="UTC")
    events = pd.DataFrame(
        {
            "signal_column": ["vol_shock_relaxation_event"],
            "symbol": ["BTCUSDT"],
            "timestamp": [detected_ts],
            "enter_ts": [detected_ts],
            "phenom_enter_ts": [detected_ts],
            "detected_ts": [detected_ts],
            "signal_ts": [detected_ts],
            "exit_ts": [pd.Timestamp("2026-01-01T00:15:00Z", tz="UTC")],
            "event_id": ["e_offgrid"],
            "event_type": ["VOL_SHOCK"],
        }
    )

    flags = registry.build_event_flags(
        events=events,
        symbols=["BTCUSDT"],
        data_root=Path("/tmp"),
        run_id="r4",
        timeframe="5m",
    )

    signal_col = _signal_ts_column("vol_shock_relaxation_event")
    btc_flags = flags[flags["symbol"] == "BTCUSDT"].sort_values("timestamp").reset_index(drop=True)

    signal_true_rows = btc_flags[btc_flags[signal_col]]
    assert len(signal_true_rows) == 1
    # First bar > 00:07 on the [00:00, 00:05, 00:10, 00:15] grid is 00:10
    assert signal_true_rows.iloc[0]["timestamp"] == pd.Timestamp("2026-01-01T00:10:00Z", tz="UTC")


def test_explicit_detected_ts_later_than_enter_ts(monkeypatch):
    """
    If analyzer emits detected_ts > enter_ts (e.g. confirmation lag),
    _signal fires at first bar > detected_ts, NOT at first bar > enter_ts.
    phenom_enter_ts = 00:00, detected_ts = 00:10 → signal at 00:15.
    """
    monkeypatch.setattr(registry, "_load_symbol_timestamps", lambda **kwargs: _symbol_grid())

    phenom_ts = pd.Timestamp("2026-01-01T00:00:00Z", tz="UTC")
    detected_ts = pd.Timestamp("2026-01-01T00:10:00Z", tz="UTC")
    events = pd.DataFrame(
        {
            "signal_column": ["vol_shock_relaxation_event"],
            "symbol": ["BTCUSDT"],
            "timestamp": [phenom_ts],
            "enter_ts": [phenom_ts],
            "phenom_enter_ts": [phenom_ts],
            "detected_ts": [detected_ts],
            "signal_ts": [detected_ts],
            "exit_ts": [pd.Timestamp("2026-01-01T00:15:00Z", tz="UTC")],
            "event_id": ["e_conf_lag"],
            "event_type": ["VOL_SHOCK"],
        }
    )

    flags = registry.build_event_flags(
        events=events,
        symbols=["BTCUSDT"],
        data_root=Path("/tmp"),
        run_id="r5",
        timeframe="5m",
    )

    signal_col = _signal_ts_column("vol_shock_relaxation_event")
    btc_flags = flags[flags["symbol"] == "BTCUSDT"].sort_values("timestamp").reset_index(drop=True)

    signal_true_rows = btc_flags[btc_flags[signal_col]]
    assert len(signal_true_rows) == 1
    # First bar > detected_ts (00:10) on the grid is 00:15
    assert signal_true_rows.iloc[0]["timestamp"] == pd.Timestamp("2026-01-01T00:15:00Z", tz="UTC")

    # Bars at and before detected_ts must have _signal = False
    for ts_str in ["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z", "2026-01-01T00:10:00Z"]:
        row = btc_flags[btc_flags["timestamp"] == pd.Timestamp(ts_str, tz="UTC")].iloc[0]
        assert not bool(row[signal_col]), f"PIT violation: _signal at {ts_str} <= detected_ts"
