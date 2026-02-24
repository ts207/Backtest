from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from features.liquidity_vacuum import detect_liquidity_vacuum_events as canonical_liquidity_vacuum
from features.vol_shock_relaxation import detect_vol_shock_relaxation_events as canonical_vol_shock
from pipelines.features.liquidity_vacuum import detect_liquidity_vacuum_events as pipeline_liquidity_vacuum
from pipelines.features.vol_shock_relaxation import detect_vol_shock_relaxation_events as pipeline_vol_shock


def test_pipeline_vol_shock_detector_is_canonical_alias():
    assert pipeline_vol_shock is canonical_vol_shock


def test_pipeline_liquidity_vacuum_detector_is_canonical_alias():
    assert pipeline_liquidity_vacuum is canonical_liquidity_vacuum
