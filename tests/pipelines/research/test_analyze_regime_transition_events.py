from __future__ import annotations

from types import SimpleNamespace
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.research.analyze_regime_transition_events as analyzer


def _sample_features(n: int = 512) -> pd.DataFrame:
    idx = np.arange(n)
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    close = 100.0 + np.cumsum(np.sin(idx / 11.0) * 0.15 + np.random.default_rng(7).normal(0.0, 0.08, n))
    high = close + np.abs(np.random.default_rng(11).normal(0.05, 0.02, n))
    low = close - np.abs(np.random.default_rng(13).normal(0.05, 0.02, n))
    return pd.DataFrame(
        {
            "timestamp": ts,
            "open": close,
            "high": high,
            "low": low,
            "close": close,
            "rv_96": pd.Series(close).pct_change().rolling(96, min_periods=8).std().fillna(0.0),
            "spread_zscore": np.random.default_rng(17).normal(0.0, 1.0, n),
            "basis_zscore": np.random.default_rng(19).normal(0.0, 1.0, n),
            "cross_exchange_spread_z": np.random.default_rng(23).normal(0.0, 1.0, n),
        }
    )


def test_detect_mask_returns_boolean_series_for_supported_event():
    df = _sample_features()
    mask = analyzer._event_mask(df, event_type="VOL_REGIME_SHIFT_EVENT")
    assert len(mask) == len(df)
    assert mask.dtype == bool


def test_main_writes_rows_for_requested_event_type(monkeypatch, tmp_path):
    monkeypatch.setattr(
        analyzer,
        "EVENT_REGISTRY_SPECS",
        {
            "VOL_REGIME_SHIFT_EVENT": SimpleNamespace(
                reports_dir="regime_transition",
                events_file="regime_transition_events.csv",
            )
        },
    )
    monkeypatch.setattr(analyzer, "_load_features", lambda run_id, symbol, timeframe: _sample_features())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "analyze_regime_transition_events.py",
            "--run_id",
            "r_test",
            "--symbols",
            "BTCUSDT",
            "--event_type",
            "VOL_REGIME_SHIFT_EVENT",
            "--out_dir",
            str(tmp_path / "out"),
        ],
    )
    rc = analyzer.main()
    assert rc == 0
    out_csv = tmp_path / "out" / "regime_transition_events.csv"
    assert out_csv.exists()
    out = pd.read_csv(out_csv)
    if not out.empty:
        assert set(out["event_type"].astype(str).unique()) == {"VOL_REGIME_SHIFT_EVENT"}
