import numpy as np
import pandas as pd
from eval.selection_bias import probabilistic_sharpe_ratio, deflated_sharpe_ratio


def test_psr_high_for_strong_strategy():
    rng = np.random.default_rng(0)
    # Strong positive PnL → PSR should be high (close to 1)
    pnl = pd.Series(rng.normal(0.002, 0.01, 2000))
    psr = probabilistic_sharpe_ratio(pnl, benchmark_sr=0.0)
    assert psr > 0.90


def test_psr_low_for_weak_strategy():
    rng = np.random.default_rng(0)
    pnl = pd.Series(rng.normal(0.0001, 0.05, 500))
    psr = probabilistic_sharpe_ratio(pnl, benchmark_sr=0.0)
    assert psr < 0.70


def test_dsr_below_psr_when_trials_gt_1():
    rng = np.random.default_rng(0)
    pnl = pd.Series(rng.normal(0.002, 0.01, 2000))
    psr = probabilistic_sharpe_ratio(pnl, benchmark_sr=0.0)
    dsr = deflated_sharpe_ratio(pnl, n_trials=10)
    assert dsr < psr


def test_empty_series_returns_zero():
    psr = probabilistic_sharpe_ratio(pd.Series([], dtype=float), benchmark_sr=0.0)
    assert psr == 0.0
