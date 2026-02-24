from __future__ import annotations

import pandas as pd

from features.funding_persistence import build_funding_persistence_state


def test_funding_persistence_state_includes_source_event_type_column():
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=128, freq="5min", tz="UTC"),
            "funding_rate_scaled": [0.002] * 128,
        }
    )

    out = build_funding_persistence_state(frame=frame, symbol="BTCUSDT")

    assert "fp_source_event_type" in out.columns
    assert set(out["fp_source_event_type"].dropna().unique().tolist()) == {"FUNDING_PERSISTENCE_TRIGGER"}
