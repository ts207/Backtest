from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research.analyze_oi_shock_events import (
    DEFAULT_FLUSH_PCT_TH,
    _detect_oi_events_for_symbol,
)


def test_default_flush_threshold_is_half_percent():
    assert DEFAULT_FLUSH_PCT_TH == -0.005


def test_detect_oi_events_emits_flush_on_half_percent_drop():
    ts = pd.date_range("2024-09-01", periods=6, freq="5min", tz="UTC")
    df = pd.DataFrame(
        {
            "timestamp": ts,
            "close": [100, 100, 100, 100, 100, 100],
            "oi_notional": [1000.0, 1001.0, 1002.0, 1003.0, 997.0, 996.0],
        }
    )

    events = _detect_oi_events_for_symbol(
        symbol="BTCUSDT",
        df=df,
        oi_window=3,
        spike_z_th=9.0,  # suppress spike detections; isolate flush logic
        flush_pct_th=DEFAULT_FLUSH_PCT_TH,
    )

    flush_events = [e for e in events if e["event_type"] == "OI_FLUSH"]
    assert len(flush_events) >= 1
