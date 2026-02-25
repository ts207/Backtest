from __future__ import annotations

import importlib
from types import SimpleNamespace
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))


def _sample_features(n: int = 640) -> pd.DataFrame:
    idx = np.arange(n)
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    close = 100.0 + np.cumsum(np.sin(idx / 13.0) * 0.12 + np.random.default_rng(7).normal(0.0, 0.08, n))
    high = close + np.abs(np.random.default_rng(11).normal(0.05, 0.02, n))
    low = close - np.abs(np.random.default_rng(13).normal(0.05, 0.02, n))
    spread_bps = np.abs(np.random.default_rng(17).normal(4.0, 1.2, n))
    return pd.DataFrame(
        {
            "timestamp": ts,
            "open": close,
            "high": high,
            "low": low,
            "close": close,
            "rv_96": pd.Series(close).pct_change().rolling(96, min_periods=16).std().fillna(0.0),
            "spread_zscore": np.random.default_rng(19).normal(0.0, 1.0, n),
            "basis_zscore": np.random.default_rng(23).normal(0.0, 1.0, n),
            "cross_exchange_spread_z": np.random.default_rng(29).normal(0.0, 1.0, n),
            "funding_rate_scaled": np.random.default_rng(31).normal(0.0, 1e-4, n),
            "oi_notional": 1_000_000.0 + np.random.default_rng(37).normal(0.0, 2_000.0, n).cumsum(),
            "oi_delta_1h": np.random.default_rng(41).normal(0.0, 1_000.0, n),
            "liquidation_notional": np.random.default_rng(43).exponential(30.0, n),
            "range_96": np.abs(np.random.default_rng(47).normal(1.0, 0.2, n)),
            "range_med_2880": np.abs(np.random.default_rng(53).normal(1.2, 0.1, n)),
            "spread_bps": spread_bps,
        }
    )


CASES = [
    (
        "pipelines.research.analyze_liquidity_dislocation_events",
        "DEPTH_COLLAPSE",
        "liquidity_dislocation",
        "liquidity_dislocation_events.csv",
    ),
    (
        "pipelines.research.analyze_volatility_transition_events",
        "VOL_SPIKE",
        "volatility_transition",
        "volatility_transition_events.csv",
    ),
    (
        "pipelines.research.analyze_positioning_extremes_events",
        "FUNDING_FLIP",
        "positioning_extremes",
        "positioning_extremes_events.csv",
    ),
    (
        "pipelines.research.analyze_forced_flow_and_exhaustion_events",
        "TREND_EXHAUSTION_TRIGGER",
        "forced_flow_and_exhaustion",
        "forced_flow_and_exhaustion_events.csv",
    ),
    (
        "pipelines.research.analyze_statistical_dislocation_events",
        "ZSCORE_STRETCH",
        "statistical_dislocation",
        "statistical_dislocation_events.csv",
    ),
    (
        "pipelines.research.analyze_information_desync_events",
        "SPOT_PERP_BASIS_SHOCK",
        "information_desync",
        "information_desync_events.csv",
    ),
    (
        "pipelines.research.analyze_temporal_structure_events",
        "SESSION_OPEN_EVENT",
        "temporal_structure",
        "temporal_structure_events.csv",
    ),
]


@pytest.mark.parametrize(("module_name", "event_type", "_reports_dir", "_events_file"), CASES)
def test_family_wave2_event_mask_returns_bool(module_name: str, event_type: str, _reports_dir: str, _events_file: str):
    module = importlib.import_module(module_name)
    mask = module._event_mask(_sample_features(), event_type=event_type)
    assert mask.dtype == bool
    assert len(mask) == len(_sample_features())


@pytest.mark.parametrize(("module_name", "event_type", "reports_dir", "events_file"), CASES)
def test_family_wave2_main_writes_target_event(monkeypatch, tmp_path, module_name: str, event_type: str, reports_dir: str, events_file: str):
    module = importlib.import_module(module_name)
    monkeypatch.setattr(
        module,
        "EVENT_REGISTRY_SPECS",
        {event_type: SimpleNamespace(reports_dir=reports_dir, events_file=events_file)},
    )
    monkeypatch.setattr(module, "_load_features", lambda run_id, symbol, timeframe: _sample_features())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            f"{module_name.split('.')[-1]}.py",
            "--run_id",
            "r_test",
            "--symbols",
            "BTCUSDT",
            "--event_type",
            event_type,
            "--out_dir",
            str(tmp_path / "out"),
        ],
    )
    rc = module.main()
    assert rc == 0
    out_csv = tmp_path / "out" / events_file
    assert out_csv.exists()
    out = pd.read_csv(out_csv)
    if not out.empty:
        assert set(out["event_type"].astype(str).unique()) == {event_type}
