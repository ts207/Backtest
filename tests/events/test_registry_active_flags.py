from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from events import registry


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


def test_build_event_flags_emits_impulse_and_active_columns(monkeypatch):
    monkeypatch.setattr(registry, "_load_symbol_timestamps", lambda **kwargs: _symbol_grid())

    events = pd.DataFrame(
        {
            "signal_column": ["vol_shock_relaxation_event"],
            "symbol": ["BTCUSDT"],
            "timestamp": pd.to_datetime(["2026-01-01T00:05:00Z"], utc=True),
            "enter_ts": pd.to_datetime(["2026-01-01T00:05:00Z"], utc=True),
            "exit_ts": pd.to_datetime(["2026-01-01T00:15:00Z"], utc=True),
            "event_id": ["e1"],
            "event_type": ["VOL_SHOCK"],
        }
    )

    flags = registry.build_event_flags(
        events=events,
        symbols=["BTCUSDT"],
        data_root=Path("/tmp"),
        run_id="r1",
        timeframe="5m",
    )

    impulse_col = "vol_shock_relaxation_event"
    active_col = "vol_shock_relaxation_active"
    assert impulse_col in flags.columns
    assert active_col in flags.columns

    row_0005 = flags[flags["timestamp"] == pd.Timestamp("2026-01-01T00:05:00Z")].iloc[0]
    row_0010 = flags[flags["timestamp"] == pd.Timestamp("2026-01-01T00:10:00Z")].iloc[0]
    row_0015 = flags[flags["timestamp"] == pd.Timestamp("2026-01-01T00:15:00Z")].iloc[0]

    assert bool(row_0005[impulse_col]) is True
    assert bool(row_0005[active_col]) is True
    assert bool(row_0010[impulse_col]) is False
    assert bool(row_0010[active_col]) is True
    assert bool(row_0015[impulse_col]) is False
    assert bool(row_0015[active_col]) is True


def test_build_event_flags_all_symbol_event_sets_active_for_all_symbols(monkeypatch):
    monkeypatch.setattr(registry, "_load_symbol_timestamps", lambda **kwargs: _symbol_grid())

    events = pd.DataFrame(
        {
            "signal_column": ["liquidity_vacuum_event"],
            "symbol": ["ALL"],
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "enter_ts": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "exit_ts": pd.to_datetime(["2026-01-01T00:10:00Z"], utc=True),
            "event_id": ["e2"],
            "event_type": ["LIQUIDITY_VACUUM"],
        }
    )

    flags = registry.build_event_flags(
        events=events,
        symbols=["BTCUSDT", "ETHUSDT"],
        data_root=Path("/tmp"),
        run_id="r2",
        timeframe="5m",
    )

    active_col = "liquidity_vacuum_active"
    check_ts = pd.Timestamp("2026-01-01T00:10:00Z")
    rows = flags[flags["timestamp"] == check_ts]
    assert len(rows) == 2
    assert rows[active_col].all()


def test_merge_event_flags_for_selected_event_types_replaces_only_selected_columns():
    timestamps = pd.to_datetime(
        ["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"],
        utc=True,
    )
    existing = pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "vol_shock_relaxation_event": [True, False],
            "vol_shock_relaxation_active": [True, True],
            "liquidity_vacuum_event": [True, True],
            "liquidity_vacuum_active": [True, True],
        }
    )
    recomputed = pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "vol_shock_relaxation_event": [False, False],
            "vol_shock_relaxation_active": [False, False],
            "liquidity_vacuum_event": [False, True],
            "liquidity_vacuum_active": [False, True],
        }
    )

    merged = registry.merge_event_flags_for_selected_event_types(
        existing_flags=existing,
        recomputed_flags=recomputed,
        selected_event_types=["LIQUIDITY_VACUUM"],
    )

    # Non-selected columns must remain as in existing.
    assert merged["vol_shock_relaxation_event"].tolist() == [True, False]
    assert merged["vol_shock_relaxation_active"].tolist() == [True, True]
    # Selected columns must come from recomputed.
    assert merged["liquidity_vacuum_event"].tolist() == [False, True]
    assert merged["liquidity_vacuum_active"].tolist() == [False, True]


def test_merge_event_flags_for_selected_event_types_bootstraps_when_existing_empty():
    timestamps = pd.to_datetime(
        ["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"],
        utc=True,
    )
    recomputed = pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "vol_shock_relaxation_event": [True, False],
            "vol_shock_relaxation_active": [True, True],
        }
    )
    merged = registry.merge_event_flags_for_selected_event_types(
        existing_flags=pd.DataFrame(),
        recomputed_flags=recomputed,
        selected_event_types=["VOL_SHOCK"],
    )
    assert len(merged) == 2
    assert merged["vol_shock_relaxation_event"].tolist() == [True, False]
    assert "vol_shock_relaxation_active" in merged.columns
