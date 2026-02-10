import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from engine.runner import _apply_execution_stress
from pipelines.research.strategies_cost_report import (
    _aggregate_ohlcv_15m_to_tf,
    _aggregation_identity_diagnostics,
    _feature_semantic_equivalence_diagnostics,
)


def _bars(periods: int = 64) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01 00:00", periods=periods, freq="15min", tz="UTC")
    base = pd.Series(range(periods), dtype=float)
    return pd.DataFrame(
        {
            "timestamp": ts,
            "open": 100.0 + base,
            "high": 101.0 + base,
            "low": 99.0 + base,
            "close": 100.5 + base,
            "volume": 10.0,
            "funding_event_ts": pd.NaT,
            "funding_rate_scaled": 0.0,
            "is_gap": False,
            "gap_len": 0,
        }
    )


def test_aggregation_identity_passes_on_generated_4h_bars() -> None:
    bars15 = _bars(96)
    bars4h = _aggregate_ohlcv_15m_to_tf(bars15, "4h")
    diag = _aggregation_identity_diagnostics(bars15, bars4h, "4h")
    assert diag["open_mismatch_ratio"] == 0.0
    assert diag["close_mismatch_ratio"] == 0.0
    assert diag["high_mismatch_ratio"] == 0.0
    assert diag["low_mismatch_ratio"] == 0.0
    assert diag["volume_mismatch_ratio"] == 0.0


def test_feature_semantic_equivalence_returns_bounded_correlations() -> None:
    bars15 = _bars(1200)
    diag = _feature_semantic_equivalence_diagnostics(bars15, "4h")
    assert -1.0 <= float(diag["rv_pct_2880_corr"]) <= 1.0
    assert -1.0 <= float(diag["range_med_480_corr"]) <= 1.0
    assert int(diag["samples"]) > 0


def test_execution_stress_decision_grid_hours_holds_between_grid_points() -> None:
    ts = pd.date_range("2024-01-01 00:00", periods=20, freq="15min", tz="UTC")
    positions = pd.Series([1] * 4 + [-1] * 16, index=ts)
    stressed = _apply_execution_stress(
        positions,
        delay_bars=0,
        min_hold_bars=1,
        decision_grid_hours=0,
        decision_grid_bars=16,
        decision_grid_offset_bars=0,
    )
    assert stressed.iloc[0] == 1
    assert stressed.iloc[1:16].eq(1).all()


def test_execution_stress_decision_grid_offset_last_bar_of_block() -> None:
    ts = pd.date_range("2024-01-01 00:00", periods=20, freq="15min", tz="UTC")
    positions = pd.Series([-1] * 15 + [1] * 5, index=ts)
    stressed = _apply_execution_stress(
        positions,
        delay_bars=0,
        min_hold_bars=1,
        decision_grid_hours=0,
        decision_grid_bars=16,
        decision_grid_offset_bars=15,
    )
    assert stressed.iloc[:15].eq(0).all()
    assert stressed.iloc[15:].eq(1).all()
