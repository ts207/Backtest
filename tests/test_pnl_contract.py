import pandas as pd

from engine.pnl import compute_returns, compute_pnl_components


def test_next_bar_pnl_contract_no_same_bar_realization():
    # close-to-close returns with prior position (shifted) should realize PnL on next bar only.
    close = pd.Series([100.0, 110.0, 121.0], index=pd.to_datetime(["2022-01-01", "2022-01-02", "2022-01-03"], utc=True))
    ret = compute_returns(close)
    pos = pd.Series([1.0, 1.0, 1.0], index=close.index)
    out = compute_pnl_components(pos=pos, ret=ret, cost_bps=0.0)

    # First bar has NaN return -> forced 0
    assert out.loc[close.index[0], "gross_pnl"] == 0.0
    assert out.loc[close.index[0], "pnl"] == 0.0

    # Second bar gross pnl uses prior pos (1) times return from 100->110 (=0.1)
    assert abs(out.loc[close.index[1], "gross_pnl"] - 0.10) < 1e-12
    # Third bar gross pnl uses return 110->121 (=0.1)
    assert abs(out.loc[close.index[2], "gross_pnl"] - 0.10) < 1e-12


def test_turnover_cost_exactness_constant_cost():
    idx = pd.to_datetime(["2022-01-01", "2022-01-02", "2022-01-03", "2022-01-04"], utc=True)
    ret = pd.Series([0.0, 0.0, 0.0, 0.0], index=idx)
    pos = pd.Series([0.0, 1.0, 1.0, 0.0], index=idx)
    out = compute_pnl_components(pos=pos, ret=ret, cost_bps=10.0)

    expected = pd.Series([0.0, 10.0 / 10000.0, 0.0, 10.0 / 10000.0], index=idx)
    pd.testing.assert_series_equal(out["trading_cost"], expected, check_names=False)


def test_funding_alignment_uses_prior_position():
    idx = pd.to_datetime(["2022-01-01", "2022-01-02", "2022-01-03"], utc=True)
    ret = pd.Series([0.0, 0.0, 0.0], index=idx)
    # Long only during second bar exposure window.
    pos = pd.Series([0.0, 1.0, 0.0], index=idx)
    funding = pd.Series([0.0, 0.0, 0.01], index=idx)

    out = compute_pnl_components(pos=pos, ret=ret, cost_bps=0.0, funding_rate=funding)

    # funding_pnl is -prior_pos * funding_rate; prior_pos at idx[1] is 0, at idx[2] is 1.
    assert out.loc[idx[1], "funding_pnl"] == 0.0
    # Settlement at idx[2] should apply to prior_pos (which is 1.0 at idx[2])
    assert abs(out.loc[idx[2], "funding_pnl"] - (-0.01)) < 1e-12
