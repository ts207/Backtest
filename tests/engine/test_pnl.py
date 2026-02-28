"""
Unit tests for engine/pnl.py

Covers:
- compute_returns: basic, gap NaN propagation
- compute_returns_next_open: entry vs hold bar split
- compute_pnl_components: gross, cost, funding, borrow, NaN zeroing
- compute_pnl: delegation smoke
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from engine.pnl import (
    compute_pnl,
    compute_pnl_components,
    compute_returns,
    compute_returns_next_open,
)


def _ts(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")


# ---------------------------------------------------------------------------
# compute_returns
# ---------------------------------------------------------------------------

class TestComputeReturns:
    def test_basic_returns(self):
        close = pd.Series([100.0, 101.0, 99.0, 103.0], index=_ts(4))
        ret = compute_returns(close)
        assert np.isnan(ret.iloc[0])  # first bar always NaN
        assert pytest.approx(ret.iloc[1]) == 1.0 / 100.0
        assert pytest.approx(ret.iloc[2]) == -2.0 / 101.0
        assert pytest.approx(ret.iloc[3]) == 4.0 / 99.0

    def test_gap_produces_nan(self):
        """NaN in the close series propagates as NaN return, not silently filled."""
        close = pd.Series([100.0, np.nan, 102.0], index=_ts(3))
        ret = compute_returns(close)
        assert np.isnan(ret.iloc[1])
        # Bar after gap: 102/NaN → NaN
        assert np.isnan(ret.iloc[2])

    def test_constant_price_is_zero_return(self):
        close = pd.Series([50.0, 50.0, 50.0], index=_ts(3))
        ret = compute_returns(close)
        assert pytest.approx(ret.iloc[1]) == 0.0
        assert pytest.approx(ret.iloc[2]) == 0.0


# ---------------------------------------------------------------------------
# compute_returns_next_open
# ---------------------------------------------------------------------------

class TestComputeReturnsNextOpen:
    def test_entry_bar_uses_next_open(self):
        """At an entry bar (prior=0, current≠0), return = open[t+1]/close[t] - 1."""
        idx = _ts(4)
        close = pd.Series([100.0, 101.0, 102.0, 103.0], index=idx)
        open_ = pd.Series([99.5, 100.5, 101.5, 102.5], index=idx)
        # Position goes long at bar 1
        positions = pd.Series([0, 1, 1, 0], index=idx)
        ret = compute_returns_next_open(close, open_, positions)
        # Bar 1 is entry: return = open[2]/close[1] - 1 = 101.5/101 - 1
        assert pytest.approx(ret.iloc[1]) == 101.5 / 101.0 - 1.0

    def test_hold_bar_uses_close_to_close(self):
        """Holding bar (both prior and current non-zero) uses standard close-to-close."""
        idx = _ts(4)
        close = pd.Series([100.0, 101.0, 102.0, 103.0], index=idx)
        open_ = pd.Series([99.5, 100.5, 101.5, 102.5], index=idx)
        positions = pd.Series([0, 1, 1, 0], index=idx)
        ret = compute_returns_next_open(close, open_, positions)
        # Bar 2 is a hold bar: close-to-close = 102/101 - 1
        assert pytest.approx(ret.iloc[2]) == 102.0 / 101.0 - 1.0

        # At idx 0, close=0.0. The next open is open_[1] = 100.5.
        # So return at idx 0 is open_[1]/close[0] - 1 -> 100.5 / 0.0 - 1 -> inf -> NaN.
        # But this is assigned to `entry_ret` based on when the ENTRY happens.
        # The entry (pos 0 -> 1) is at idx=1.
        # At idx=1, close[1] is 101.0, next_open[2] = 101.5. Return is 101.5/101 - 1.
        # Wait, the entry is at index 1.
        # `is_entry` is True at index 1.
        # `entry_ret` at index 1 uses `next_open[1] / safe_close[1] - 1`.
        # Wait, `entry_ret = next_open / safe_close - 1` with `next_open = open_.shift(-1)`.
        # So at index 1: `next_open[1]` is `open_[2]` (101.5).
        # `safe_close[1]` is `close[1]` (101.0).
        # The entry return at index 1 is 101.5/101.0 - 1 = 0.00495.
        # The test intended to test zero close causing NaN, but put the zero at idx 0 instead of idx 1.
        idx = _ts(3)
        close = pd.Series([100.0, 0.0, 102.0], index=idx)
        open_ = pd.Series([99.5, 100.5, 101.5], index=idx)
        positions = pd.Series([0, 1, 0], index=idx)
        ret = compute_returns_next_open(close, open_, positions)
        # Entry at idx 1. close[1] = 0.0, open[2] = 101.5. open[2]/close[1] - 1 -> inf -> NaN.
        assert np.isnan(ret.iloc[1]) or not np.isfinite(ret.iloc[1])


# ---------------------------------------------------------------------------
# compute_pnl_components
# ---------------------------------------------------------------------------

class TestComputePnlComponents:
    def _make_series(self, values, n=None):
        idx = _ts(len(values) if n is None else n)
        return pd.Series(values, index=idx[: len(values)])

    def test_gross_pnl_uses_prior_position(self):
        """gross_pnl[t] = pos[t-1] * ret[t]."""
        idx = _ts(4)
        pos = pd.Series([0.0, 1.0, 1.0, 0.0], index=idx)
        ret = pd.Series([0.0, 0.01, 0.02, -0.01], index=idx)
        result = compute_pnl_components(pos, ret, cost_bps=0.0)
        # Bar 0: prior=0 → gross=0
        assert result["gross_pnl"].iloc[0] == pytest.approx(0.0)
        # Bar 1: prior=0 (from fill) → gross = 0 * 0.01 = 0
        assert result["gross_pnl"].iloc[1] == pytest.approx(0.0)
        # Bar 2: prior=1 → gross = 1 * 0.02 = 0.02
        assert result["gross_pnl"].iloc[2] == pytest.approx(0.02)
        # Bar 3: prior=1 → gross = 1 * (-0.01) = -0.01
        assert result["gross_pnl"].iloc[3] == pytest.approx(-0.01)

    def test_trading_cost_on_turnover(self):
        """trading_cost = |pos_change| * cost_bps / 10000."""
        idx = _ts(3)
        pos = pd.Series([0.0, 1.0, 0.0], index=idx)
        ret = pd.Series([0.0, 0.0, 0.0], index=idx)
        result = compute_pnl_components(pos, ret, cost_bps=10.0)
        # Bar 1: enter → |1-0| * 10/10000 = 0.001
        assert result["trading_cost"].iloc[1] == pytest.approx(0.001)
        # Bar 2: exit → |0-1| * 10/10000 = 0.001
        assert result["trading_cost"].iloc[2] == pytest.approx(0.001)

    def test_cost_bps_as_series(self):
        """cost_bps can be a per-bar Series."""
        idx = _ts(3)
        pos = pd.Series([0.0, 1.0, 0.0], index=idx)
        ret = pd.Series([0.0, 0.0, 0.0], index=idx)
        cost_series = pd.Series([0.0, 20.0, 5.0], index=idx)
        result = compute_pnl_components(pos, ret, cost_bps=cost_series)
        assert result["trading_cost"].iloc[1] == pytest.approx(20.0 / 10000.0)
        assert result["trading_cost"].iloc[2] == pytest.approx(5.0 / 10000.0)

    def test_funding_pnl_long_positive_funding(self):
        """Long pays positive funding: funding_pnl = -pos * funding_rate."""
        idx = _ts(3)
        pos = pd.Series([0.0, 1.0, 1.0], index=idx)
        ret = pd.Series([0.0, 0.0, 0.0], index=idx)
        funding = pd.Series([0.0, 0.001, 0.001], index=idx)
        result = compute_pnl_components(pos, ret, cost_bps=0.0, funding_rate=funding)
        # Bar 2: prior_pos=1, funding=0.001 → funding_pnl = -1 * 0.001 = -0.001
        assert result["funding_pnl"].iloc[2] == pytest.approx(-0.001)

    def test_borrow_cost_only_on_shorts(self):
        """Borrow cost applies only to long short exposure (prior_pos < 0)."""
        idx = _ts(3)
        pos = pd.Series([0.0, -1.0, -1.0], index=idx)
        ret = pd.Series([0.0, 0.0, 0.0], index=idx)
        borrow = pd.Series([0.0, 0.0, 0.0005], index=idx)
        result = compute_pnl_components(pos, ret, cost_bps=0.0, borrow_rate=borrow)
        # Bar 2: prior_pos=-1, borrow=0.0005 → borrow_cost = |-1| * 0.0005 = 0.0005
        assert result["borrow_cost"].iloc[2] == pytest.approx(0.0005)

    def test_nan_return_bars_zeroed(self):
        """NaN return bars must produce zero across all PnL components."""
        idx = _ts(4)
        pos = pd.Series([0.0, 1.0, 1.0, 0.0], index=idx)
        ret = pd.Series([0.0, 0.01, np.nan, 0.01], index=idx)
        result = compute_pnl_components(pos, ret, cost_bps=10.0)
        # Bar 2 has NaN ret → all components zero
        assert result["gross_pnl"].iloc[2] == pytest.approx(0.0)
        assert result["trading_cost"].iloc[2] == pytest.approx(0.0)
        assert result["pnl"].iloc[2] == pytest.approx(0.0)

    def test_net_pnl_formula(self):
        """pnl = gross_pnl - trading_cost + funding_pnl - borrow_cost."""
        idx = _ts(3)
        pos = pd.Series([0.0, 1.0, 1.0], index=idx)
        ret = pd.Series([0.0, 0.0, 0.02], index=idx)
        funding = pd.Series([0.0, 0.0, 0.001], index=idx)
        borrow = pd.Series([0.0, 0.0, 0.0], index=idx)
        result = compute_pnl_components(pos, ret, cost_bps=10.0, funding_rate=funding, borrow_rate=borrow)
        gross = result["gross_pnl"].iloc[2]
        cost = result["trading_cost"].iloc[2]
        fp = result["funding_pnl"].iloc[2]
        bc = result["borrow_cost"].iloc[2]
        assert result["pnl"].iloc[2] == pytest.approx(gross - cost + fp - bc)


# ---------------------------------------------------------------------------
# compute_pnl (wrapper)
# ---------------------------------------------------------------------------

class TestComputePnl:
    def test_matches_components_pnl(self):
        idx = _ts(4)
        pos = pd.Series([0.0, 1.0, 1.0, 0.0], index=idx)
        ret = pd.Series([0.0, 0.01, 0.02, -0.01], index=idx)
        pnl = compute_pnl(pos, ret, cost_bps=5.0)
        components = compute_pnl_components(pos, ret, cost_bps=5.0)
        pd.testing.assert_series_equal(pnl, components["pnl"])
