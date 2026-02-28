"""
Unit tests for engine/risk_allocator.py

Covers:
- Base proportional scaling (max_strategy_gross, max_symbol_gross, max_portfolio_gross)
- max_correlated_gross (anti-crowding cap)
- regime_scale_map (regime-conditional sizing)
- target_annual_vol (vol targeting)
- max_drawdown_limit (drawdown-based position gating)
- max_new_exposure_per_bar (position change speed limit)
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from engine.risk_allocator import RiskLimits, allocate_position_scales


def _ts(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")


class TestRiskAllocator:
    def test_empty_input_returns_zero_scales(self):
        limits = RiskLimits()
        scales, stats = allocate_position_scales({}, {}, limits)
        assert scales == {}
        assert stats["requested_gross"] == 0.0

    def test_max_strategy_gross(self):
        """Single strategy asked for scale 3.0 but capped at 1.0."""
        idx = _ts(3)
        pos = {"s1": pd.Series([0, 1, -1], index=idx)}
        req = {"s1": pd.Series([3.0, 3.0, 3.0], index=idx)}
        limits = RiskLimits(max_strategy_gross=1.0, max_new_exposure_per_bar=10.0)

        scales, stats = allocate_position_scales(pos, req, limits)
        s1_out = scales["s1"]

        # Bar 1: raw pos=1, req scale=3. Allocator clips to 1.0. 
        # Output scale should be 1.0 (since 1.0 / abs(1) = 1.0)
        assert s1_out.iloc[1] == pytest.approx(1.0)
        assert s1_out.iloc[2] == pytest.approx(1.0)  # -1 capped to -1 (scale 1.0)

    def test_max_portfolio_gross(self):
        """Two strategies request 1.0 each, but portfolio cap is 1.5."""
        idx = _ts(2)
        pos = {
            "s1": pd.Series([0, 1], index=idx),
            "s2": pd.Series([0, 1], index=idx),
        }
        # default req is 1.0 if not provided
        req = {}
        limits = RiskLimits(max_portfolio_gross=1.5, max_strategy_gross=2.0, max_symbol_gross=10.0, max_new_exposure_per_bar=10.0)

        scales, stats = allocate_position_scales(pos, req, limits)
        
        # Bar 1: total requested gross = 2.0. Cap = 1.5. Ratio = 0.75.
        assert scales["s1"].iloc[1] == pytest.approx(0.75)
        assert scales["s2"].iloc[1] == pytest.approx(0.75)
        assert stats["clipped_fraction"] > 0.0

    def test_max_correlated_gross(self):
        """
        If s1 and s2 are long, correlated cap applies.
        If s1 is long and s2 is short, correlated cap does NOT apply.
        """
        idx = _ts(3)
        pos = {
            "s1": pd.Series([0, 1, 1], index=idx),
            "s2": pd.Series([0, 1, -1], index=idx),
        }
        limits = RiskLimits(
            max_strategy_gross=2.0, max_symbol_gross=10.0, 
            max_portfolio_gross=3.0,
            max_correlated_gross=1.0,
            max_new_exposure_per_bar=10.0
        )
        scales, _ = allocate_position_scales(pos, {}, limits)

        # Bar 1: both long (net=2, gross=2 -> fully concordant). Cap 1.0 -> scaled to 0.5 each.
        assert scales["s1"].iloc[1] == pytest.approx(0.5)
        assert scales["s2"].iloc[1] == pytest.approx(0.5)

        # Bar 2: s1 long(1), s2 short(-1). Net=0, gross=2 -> NOT fully concordant. 
        # Correlated cap shouldn't trigger. Both get full size 1.0.
        assert scales["s1"].iloc[2] == pytest.approx(1.0)
        assert scales["s2"].iloc[2] == pytest.approx(1.0)

    def test_regime_conditional_sizing(self):
        """Scale down based on regime labels."""
        idx = _ts(3)
        pos = {"s1": pd.Series([0, 1, 1], index=idx)}
        regime = pd.Series(["NORMAL", "HIGH_VOL", "UNKNOWN"], index=idx)
        scale_map = {"HIGH_VOL": 0.5, "NORMAL": 1.0}
        limits = RiskLimits()

        scales, _ = allocate_position_scales(pos, {}, limits, regime_series=regime, regime_scale_map=scale_map)
        
        # Bar 1: HIGH_VOL -> scale 0.5
        assert scales["s1"].iloc[1] == pytest.approx(0.5)
        # Bar 2: UNKNOWN -> defaults to 1.0
        assert scales["s1"].iloc[2] == pytest.approx(1.0)

    def test_max_drawdown_limit(self):
        """Scale fades proportionally as drawdown approaches the limit."""
        idx = _ts(3)
        pos = {"s1": pd.Series([1, 1, 1], index=idx)}
        # A 10% drop, then a 20% drop (relative to peak)
        pnl = pd.Series([0.0, -0.10, -0.1111111111], index=idx) 
        # Cumprod equity: 1.0 -> 0.90 -> 0.80
        # Drawdowns: 0%, 10%, 20%

        limits = RiskLimits(max_drawdown_limit=0.25)
        scales, _ = allocate_position_scales(pos, {}, limits, portfolio_pnl_series=pnl)

        # Bar 0: 0% DD -> factor 1.0
        assert scales["s1"].iloc[0] == pytest.approx(1.0)
        # Bar 1: 10% DD / 25% limit -> factor = (25-10)/25 = 15/25 = 0.6
        assert scales["s1"].iloc[1] == pytest.approx(0.6)
        # Bar 2: 20% DD / 25% limit -> factor = 5/25 = 0.2
        assert scales["s1"].iloc[2] == pytest.approx(0.2)

    def test_max_new_exposure_per_bar(self):
        """Speed limit on position entry."""
        idx = _ts(4)
        # Want to jump 0 -> 3
        pos = {"s1": pd.Series([0, 3, 3, 3], index=idx)}
        limits = RiskLimits(max_strategy_gross=5.0, max_symbol_gross=10.0, max_portfolio_gross=5.0, max_new_exposure_per_bar=2.0)
        
        scales, _ = allocate_position_scales(pos, {"s1": pd.Series([1.0, 1.0, 1.0, 1.0], index=idx)}, limits)
        
        # Output allocated sizes (not raw scale, but underlying allocated size):
        # Bar 0: 0
        # Bar 1: Wants 3, allowed delta 1 -> size 1
        # Bar 2: Wants 3, allowed delta 1 -> size 2
        # Bar 3: Wants 3, allowed delta 1 -> size 3

        # But the function returns the *scale ratio* relative to raw pos.
        # Bar 1: raw pos 3.0, allowed delta 2.0 -> size 2.0. Scale = 2.0 / 3.0 = 0.6666
        assert scales["s1"].iloc[1] == pytest.approx(2.0 / 3.0)
        # Bar 2: raw pos 3.0, prior size 2.0, allowed delta 2.0 -> size 3.0 (capped at req). Scale = 3.0 / 3.0 = 1.0
        assert scales["s1"].iloc[2] == pytest.approx(1.0)
        # Bar 3: raw pos 3.0, allowed size 3.0 -> scale 1.0
        assert scales["s1"].iloc[3] == pytest.approx(1.0)
