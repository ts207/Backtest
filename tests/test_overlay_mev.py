import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from engine import runner
from strategies.overlay_registry import apply_overlay, list_applicable_overlays, list_overlays


def test_builtin_overlays_include_mev() -> None:
    names = set(list_overlays())
    assert "funding_extreme_filter" in names
    assert "mev_aware_risk_filter" in names
    applicable = set(list_applicable_overlays())
    assert "mev_aware_risk_filter" in applicable


def test_apply_overlay_attaches_runtime_config() -> None:
    out = apply_overlay("mev_aware_risk_filter", {"overlays": []})
    assert "overlay_specs" in out
    assert "mev_aware_risk_filter" in out["overlay_specs"]
    assert "overlay_runtime" in out
    assert out["overlay_runtime"]["mev_aware_risk_filter"]["type"] == "mev_aware_risk_filter"


def test_mev_overlay_blocks_entries_and_updates_binding_stats() -> None:
    ts = pd.date_range("2024-01-01", periods=5, freq="15min", tz="UTC")
    positions = pd.Series([0, 1, 1, 0, 1], index=ts, dtype=int)
    features = pd.DataFrame(
        {
            "liquidation_notional": [0.0, 500.0, 2000.0, 0.0, 3000.0],
            "oi_notional": [1000.0, 1000.0, 1000.0, 1000.0, 1000.0],
        },
        index=ts,
    )
    overlay_runtime = {
        "mev_aware_risk_filter": {
            "type": "mev_aware_risk_filter",
            "throttle_start_bps": 4000.0,
            "block_threshold_bps": 15000.0,
        }
    }

    updated, stats = runner._apply_runtime_overlays(positions, features, overlay_runtime)
    assert updated.tolist() == [0, 0, 0, 0, 0]
    assert stats["mev_aware_risk_filter"]["delayed_entries"] == 1
    assert stats["mev_aware_risk_filter"]["blocked_entries"] == 1
    assert stats["mev_aware_risk_filter"]["changed_bars"] == 3

    frame = pd.DataFrame({"timestamp": ts, "pos": updated.values})
    binding = runner._overlay_binding_stats(
        ["mev_aware_risk_filter"],
        "BTCUSDT",
        frame,
        overlay_stats=stats,
    )
    assert binding["binding_stats"][0]["overlay"] == "mev_aware_risk_filter"
    assert binding["binding_stats"][0]["blocked_entries"] == 1
    assert binding["binding_stats"][0]["delayed_entries"] == 1
