from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.bh_fdr_grouping import canonical_bh_group_key
from pipelines.research.phase2_candidate_discovery import _make_family_id


def test_bh_fdr_group_key_stability():
    key1 = canonical_bh_group_key(
        canonical_family="INFORMATION_DESYNC",
        canonical_event_type="CROSS_VENUE_DESYNC",
        template_verb="desync_repair",
        horizon="5m",
        state_id="DESYNC_PERSISTENCE_STATE",
        symbol="BTCUSDT",
        include_symbol=True,
    )
    key2 = canonical_bh_group_key(
        canonical_family="INFORMATION_DESYNC",
        canonical_event_type="CROSS_VENUE_DESYNC",
        template_verb="desync_repair",
        horizon="5m",
        state_id="DESYNC_PERSISTENCE_STATE",
        symbol="BTCUSDT",
        include_symbol=True,
    )
    assert key1 == key2


def test_make_family_id_ignores_conditioning_label():
    fid_all = _make_family_id(
        "BTCUSDT",
        "CROSS_VENUE_DESYNC",
        "desync_repair",
        "5m",
        "all",
        canonical_family="INFORMATION_DESYNC",
        state_id="DESYNC_PERSISTENCE_STATE",
    )
    fid_cond = _make_family_id(
        "BTCUSDT",
        "CROSS_VENUE_DESYNC",
        "desync_repair",
        "5m",
        "vol_regime_high",
        canonical_family="INFORMATION_DESYNC",
        state_id="DESYNC_PERSISTENCE_STATE",
    )
    assert fid_all == fid_cond

