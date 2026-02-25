from __future__ import annotations

from types import SimpleNamespace
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.research.analyze_canonical_events as aec


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
            "funding_rate_scaled": np.random.default_rng(29).normal(0.0, 1e-4, n),
            "oi_notional": 1_000_000.0 + np.random.default_rng(31).normal(0.0, 2_500.0, n).cumsum(),
            "oi_delta_1h": np.random.default_rng(37).normal(0.0, 1200.0, n),
            "liquidation_notional": np.random.default_rng(41).exponential(40.0, n),
            "range_96": np.abs(np.random.default_rng(43).normal(1.0, 0.2, n)),
            "range_med_2880": np.abs(np.random.default_rng(47).normal(1.2, 0.1, n)),
            "spread_bps": np.abs(np.random.default_rng(53).normal(4.0, 1.2, n)),
        }
    )


def test_detect_mask_returns_boolean_series_for_supported_event():
    df = _sample_features()
    mask = aec._event_mask(df, event_type="VOL_SPIKE")
    assert len(mask) == len(df)
    assert mask.dtype == bool


def test_main_writes_rows_for_requested_event_type(monkeypatch, tmp_path):
    monkeypatch.setattr(
        aec,
        "EVENT_REGISTRY_SPECS",
        {
            "VOL_SPIKE": SimpleNamespace(
                reports_dir="volatility_transition",
                events_file="volatility_transition_events.csv",
            )
        },
    )
    monkeypatch.setattr(aec, "_load_features", lambda run_id, symbol, timeframe: _sample_features())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "analyze_canonical_events.py",
            "--run_id",
            "r_test",
            "--symbols",
            "BTCUSDT",
            "--event_type",
            "VOL_SPIKE",
            "--out_dir",
            str(tmp_path / "out"),
        ],
    )
    rc = aec.main()
    assert rc == 0
    out_csv = tmp_path / "out" / "volatility_transition_events.csv"
    assert out_csv.exists()
    out = pd.read_csv(out_csv)
    if not out.empty:
        assert set(out["event_type"].astype(str).unique()) == {"VOL_SPIKE"}
