"""
Integration tests for detected_ts PIT semantics across event families.

These tests simulate what each Phase1 analyzer family produces and verify:
  1. detected_ts = bar k where condition first becomes true
  2. _signal fires at bar k+1 (first grid bar strictly after detected_ts)
  3. No _signal fires at bar k or earlier

Family coverage:
  - Impulse / threshold events (OI_SPIKE, FUNDING_EXTREME) — condition true at single bar
  - Window events (VOL_SHOCK, LIQUIDATION_CASCADE) — phenom_enter_ts != detected_ts possible
  - Confirmation-lag events — detected_ts explicitly later than phenom_enter_ts
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from events import registry
from events.registry import _signal_ts_column, _active_signal_column


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _grid(n_bars: int = 20, freq_min: int = 5, start: str = "2026-01-01T00:00:00Z") -> pd.Series:
    """Return a Series of n_bars UTC timestamps spaced freq_min minutes apart."""
    base = pd.Timestamp(start, tz="UTC")
    return pd.Series([base + pd.Timedelta(minutes=freq_min * i) for i in range(n_bars)])


def _event_row(
    signal_col: str,
    symbol: str,
    enter_ts: pd.Timestamp,
    exit_ts: pd.Timestamp,
    detected_ts: pd.Timestamp | None = None,
    event_type: str = "VOL_SHOCK",
    event_id: str = "e1",
) -> pd.DataFrame:
    det = detected_ts if detected_ts is not None else enter_ts
    return pd.DataFrame(
        {
            "signal_column": [signal_col],
            "symbol": [symbol],
            "timestamp": [enter_ts],
            "enter_ts": [enter_ts],
            "phenom_enter_ts": [enter_ts],
            "detected_ts": [det],
            "signal_ts": [det],
            "exit_ts": [exit_ts],
            "event_id": [event_id],
            "event_type": [event_type],
        }
    )


def _signal_bar(flags: pd.DataFrame, signal_col: str, symbol: str) -> pd.Timestamp | None:
    """Return the single timestamp where _signal is True, or None if none."""
    rows = flags[(flags["symbol"] == symbol) & flags[signal_col]]
    if rows.empty:
        return None
    assert len(rows) == 1, f"Expected 1 signal bar, got {len(rows)}"
    return rows.iloc[0]["timestamp"]


# ---------------------------------------------------------------------------
# Family 1: OI_SPIKE / impulse threshold event
# Condition crosses at bar k; detected_ts = bar k.
# _signal must fire at bar k+1.
# ---------------------------------------------------------------------------

class TestImpulseThresholdFamily:
    """OI_SPIKE, FUNDING_EXTREME — single-bar threshold crossing."""

    def _build_flags(self, monkeypatch, k: int, n_bars: int = 12):
        """k = index of the bar where condition first becomes true (0-based)."""
        grid = _grid(n_bars)
        monkeypatch.setattr(registry, "_load_symbol_timestamps", lambda **kwargs: grid)

        enter_ts = grid.iloc[k]
        exit_ts = grid.iloc[k]  # impulse — single bar

        events = _event_row(
            signal_col="vol_shock_relaxation_event",
            symbol="ETHUSDT",
            enter_ts=enter_ts,
            exit_ts=exit_ts,
            detected_ts=enter_ts,
            event_type="OI_SPIKE_POSITIVE",
        )
        return registry.build_event_flags(
            events=events,
            symbols=["ETHUSDT"],
            data_root=Path("/tmp"),
            run_id="imp_test",
            timeframe="5m",
        ), grid

    def test_signal_at_k_plus_one(self, monkeypatch):
        k = 4
        flags, grid = self._build_flags(monkeypatch, k=k)
        sig_col = _signal_ts_column("vol_shock_relaxation_event")

        sig_ts = _signal_bar(flags, sig_col, "ETHUSDT")
        expected = grid.iloc[k + 1]
        assert sig_ts == expected, f"Expected signal at {expected}, got {sig_ts}"

    def test_no_signal_at_or_before_k(self, monkeypatch):
        k = 6
        flags, grid = self._build_flags(monkeypatch, k=k)
        sig_col = _signal_ts_column("vol_shock_relaxation_event")

        for i in range(k + 1):  # bars 0..k inclusive
            ts = grid.iloc[i]
            row = flags[(flags["symbol"] == "ETHUSDT") & (flags["timestamp"] == ts)]
            assert not row.empty
            assert not bool(row.iloc[0][sig_col]), (
                f"PIT violation: _signal at bar {i} (ts={ts}) <= detected_ts bar {k}"
            )

    def test_condition_true_only_after_k_bars(self, monkeypatch):
        """
        Simulate a rolling z-score: the threshold is exceeded only at bar k.
        With k = 7, bars 0..6 are below threshold, bar 7 crosses it.
        _signal must be at bar 8.
        """
        k = 7
        n_bars = 15
        grid = _grid(n_bars)
        monkeypatch.setattr(registry, "_load_symbol_timestamps", lambda **kwargs: grid)

        # Only bar k triggers the event
        enter_ts = grid.iloc[k]
        events = _event_row(
            "vol_shock_relaxation_event", "BTCUSDT",
            enter_ts=enter_ts, exit_ts=enter_ts, detected_ts=enter_ts,
            event_type="OI_SPIKE_NEGATIVE",
        )
        flags = registry.build_event_flags(
            events=events, symbols=["BTCUSDT"],
            data_root=Path("/tmp"), run_id="imp2", timeframe="5m",
        )

        sig_col = _signal_ts_column("vol_shock_relaxation_event")
        sig_ts = _signal_bar(flags, sig_col, "BTCUSDT")
        assert sig_ts == grid.iloc[k + 1], f"Signal should be at bar {k+1}"

        # Bars 0..k have no signal
        for i in range(k + 1):
            ts = grid.iloc[i]
            row = flags[(flags["symbol"] == "BTCUSDT") & (flags["timestamp"] == ts)].iloc[0]
            assert not bool(row[sig_col])


# ---------------------------------------------------------------------------
# Family 2: VOL_SHOCK / window event
# phenom_enter_ts = bar k (shock onset), detected_ts = bar k (same for impulse variant)
# exit_ts = bar k+6 (relaxation window)
# _active covers [k, k+6]; _signal fires at bar k+1.
# ---------------------------------------------------------------------------

class TestWindowEventFamily:
    """VOL_SHOCK, LIQUIDATION_CASCADE — event spans multiple bars."""

    def test_active_window_and_signal_bar(self, monkeypatch):
        n_bars = 16
        k = 3
        grid = _grid(n_bars)
        monkeypatch.setattr(registry, "_load_symbol_timestamps", lambda **kwargs: grid)

        enter_ts = grid.iloc[k]
        exit_ts = grid.iloc[k + 6]

        events = _event_row(
            "vol_shock_relaxation_event", "BTCUSDT",
            enter_ts=enter_ts, exit_ts=exit_ts, detected_ts=enter_ts,
        )
        flags = registry.build_event_flags(
            events=events, symbols=["BTCUSDT"],
            data_root=Path("/tmp"), run_id="win_test", timeframe="5m",
        )

        sig_col = _signal_ts_column("vol_shock_relaxation_event")
        act_col = _active_signal_column("vol_shock_relaxation_event")
        btc = flags[flags["symbol"] == "BTCUSDT"].sort_values("timestamp").reset_index(drop=True)

        # _active = True for bars k..k+6
        for i in range(k, k + 7):
            row = btc[btc["timestamp"] == grid.iloc[i]].iloc[0]
            assert bool(row[act_col]), f"_active should be True at bar {i}"

        # _signal = True only at bar k+1
        sig_ts = _signal_bar(flags, sig_col, "BTCUSDT")
        assert sig_ts == grid.iloc[k + 1]

        # _signal = False at bar k (detected bar itself)
        row_k = btc[btc["timestamp"] == grid.iloc[k]].iloc[0]
        assert not bool(row_k[sig_col])

    def test_window_event_last_bar_of_grid(self, monkeypatch):
        """When event is at the last bar, there is no next bar → _signal never fires."""
        n_bars = 8
        grid = _grid(n_bars)
        monkeypatch.setattr(registry, "_load_symbol_timestamps", lambda **kwargs: grid)

        enter_ts = grid.iloc[-1]  # last bar
        events = _event_row(
            "vol_shock_relaxation_event", "BTCUSDT",
            enter_ts=enter_ts, exit_ts=enter_ts, detected_ts=enter_ts,
        )
        flags = registry.build_event_flags(
            events=events, symbols=["BTCUSDT"],
            data_root=Path("/tmp"), run_id="win_last", timeframe="5m",
        )

        sig_col = _signal_ts_column("vol_shock_relaxation_event")
        btc = flags[flags["symbol"] == "BTCUSDT"]
        assert not btc[sig_col].any(), "_signal must not fire when no bar exists after detected_ts"


# ---------------------------------------------------------------------------
# Family 3: Confirmation-lag events
# phenom_enter_ts = bar k (phenomenon started, e.g. funding crossed extreme)
# detected_ts = bar k+m (confirmation after m bars of persistence)
# _signal fires at bar k+m+1.
# ---------------------------------------------------------------------------

class TestConfirmationLagFamily:
    """
    FUNDING_PERSISTENCE_TRIGGER — condition requires m consecutive bars.
    phenom_enter_ts = onset; detected_ts = first bar that satisfies persistence.
    """

    def test_signal_after_confirmation_lag(self, monkeypatch):
        n_bars = 20
        k_onset = 3   # funding crossed extreme at bar 3
        m = 4         # persistence required: bars 3,4,5,6 all extreme → confirmed at bar 6
        k_detected = k_onset + m - 1  # = 6

        grid = _grid(n_bars)
        monkeypatch.setattr(registry, "_load_symbol_timestamps", lambda **kwargs: grid)

        phenom_ts = grid.iloc[k_onset]
        detected_ts = grid.iloc[k_detected]

        events = pd.DataFrame(
            {
                "signal_column": ["vol_shock_relaxation_event"],
                "symbol": ["BTCUSDT"],
                "timestamp": [phenom_ts],
                "enter_ts": [phenom_ts],
                "phenom_enter_ts": [phenom_ts],
                "detected_ts": [detected_ts],
                "signal_ts": [detected_ts],
                "exit_ts": [grid.iloc[k_detected + 3]],
                "event_id": ["e_conf"],
                "event_type": ["FUNDING_PERSISTENCE_TRIGGER"],
            }
        )

        flags = registry.build_event_flags(
            events=events, symbols=["BTCUSDT"],
            data_root=Path("/tmp"), run_id="conf_lag", timeframe="5m",
        )

        sig_col = _signal_ts_column("vol_shock_relaxation_event")
        btc = flags[flags["symbol"] == "BTCUSDT"].sort_values("timestamp").reset_index(drop=True)

        # Signal fires at bar k_detected + 1
        sig_ts = _signal_bar(flags, sig_col, "BTCUSDT")
        assert sig_ts == grid.iloc[k_detected + 1], (
            f"Expected signal at bar {k_detected + 1}, got {sig_ts}"
        )

        # No signal at bars k_onset..k_detected
        for i in range(k_detected + 1):
            ts = grid.iloc[i]
            row = btc[btc["timestamp"] == ts].iloc[0]
            assert not bool(row[sig_col]), (
                f"PIT violation: _signal at bar {i} <= detected bar {k_detected}"
            )

    def test_normalize_picks_up_detected_ts_from_input(self):
        """
        normalize_phase1_events() must forward detected_ts from analyzer CSV
        rather than defaulting to enter_ts when the column is present.
        """
        from events.registry import normalize_phase1_events, EventRegistrySpec

        spec = EventRegistrySpec(
            event_type="VOL_SHOCK",
            reports_dir="vol_shock_relaxation",
            events_file="vol_shock_relaxation_events.csv",
            signal_column="vol_shock_relaxation_event",
        )
        phenom = pd.Timestamp("2026-01-01T00:00:00Z", tz="UTC")
        detected = pd.Timestamp("2026-01-01T00:20:00Z", tz="UTC")

        raw = pd.DataFrame(
            {
                "event_type": ["VOL_SHOCK"],
                "symbol": ["BTCUSDT"],
                "enter_ts": [phenom.isoformat()],
                "detected_ts": [detected.isoformat()],
                "exit_ts": [detected.isoformat()],
                "event_id": ["e_norm"],
            }
        )
        result = normalize_phase1_events(raw, spec=spec, run_id="norm_test")

        assert not result.empty
        assert "detected_ts" in result.columns
        assert result.iloc[0]["detected_ts"] == detected
        # signal_ts should equal detected_ts
        assert result.iloc[0]["signal_ts"] == detected
        # phenom_enter_ts should equal phenom
        assert result.iloc[0]["phenom_enter_ts"] == phenom
