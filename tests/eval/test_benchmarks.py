import numpy as np
import pandas as pd
from eval.benchmarks import compute_buy_hold_sharpe, compute_btc_beta, compute_exposure_summary


def test_buy_hold_sharpe():
    ts = pd.date_range("2023-01-01", periods=1000, freq="5min", tz="UTC")
    close = pd.Series(100.0 * np.cumprod(1 + np.random.normal(0.0001, 0.01, 1000)), index=ts)
    result = compute_buy_hold_sharpe(close)
    assert "sharpe_annualized" in result
    assert isinstance(result["sharpe_annualized"], float)


def test_btc_beta_near_one_for_identical():
    rng = np.random.default_rng(0)
    ret = pd.Series(rng.normal(0.001, 0.01, 500))
    beta_result = compute_btc_beta(strategy_returns=ret, btc_returns=ret)
    assert abs(beta_result["beta"] - 1.0) < 0.05


def test_exposure_summary_keys():
    rng = np.random.default_rng(0)
    pos = pd.Series(rng.choice([-1, 0, 1], 500).astype(float))
    result = compute_exposure_summary(pos)
    assert "gross_exposure_mean" in result
    assert "net_exposure_mean" in result
    assert "turnover_mean" in result
