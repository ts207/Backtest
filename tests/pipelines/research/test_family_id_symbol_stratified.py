"""
F-3: family_id must include symbol so BH-FDR is applied per-symbol, not pooled.
"""
from __future__ import annotations
import pandas as pd
import pytest

SAMPLE_ROWS = [
    {"event_type": "VOL_SHOCK", "rule_template": "mean_reversion", "horizon": "5m",
     "symbol": "BTCUSDT", "conditioning": "all"},
    {"event_type": "VOL_SHOCK", "rule_template": "mean_reversion", "horizon": "5m",
     "symbol": "ETHUSDT", "conditioning": "all"},
    {"event_type": "VOL_SHOCK", "rule_template": "mean_reversion", "horizon": "5m",
     "symbol": "BTCUSDT", "conditioning": "vol_regime_high"},
]

def _build_family_id(row: dict) -> str:
    """Reproduce the family_id construction from phase2_candidate_discovery.py."""
    from pipelines.research.phase2_candidate_discovery import _make_family_id
    return _make_family_id(row["symbol"], row["event_type"], row["rule_template"],
                           row["horizon"], row["conditioning"])

class TestFamilyIdIncludesSymbol:
    def test_family_id_starts_with_symbol(self):
        for row in SAMPLE_ROWS:
            fid = _build_family_id(row)
            assert fid.startswith(row["symbol"]), (
                f"family_id '{fid}' must start with symbol '{row['symbol']}'"
            )

    def test_btc_and_eth_have_different_family_ids_for_same_template(self):
        btc = _build_family_id(SAMPLE_ROWS[0])
        eth = _build_family_id(SAMPLE_ROWS[1])
        assert btc != eth, "BTC and ETH must produce different family_ids for same (event, rule, horizon, cond)"

    def test_family_id_format_is_symbol_prefixed(self):
        fid = _build_family_id(SAMPLE_ROWS[0])
        parts = fid.split("_", 1)
        assert parts[0] == "BTCUSDT", f"First segment must be symbol, got '{parts[0]}'"
