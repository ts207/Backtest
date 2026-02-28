from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats


def probabilistic_sharpe_ratio(
    pnl: pd.Series,
    benchmark_sr: float = 0.0,
    periods_per_year: int = 105120,  # 5m bars per year; unused in raw-SR mode
) -> float:
    """
    PSR: probability that the true SR exceeds benchmark_sr, corrected for
    skewness, kurtosis, and finite sample length.
    Bailey & Lopez de Prado (2012).

    The SR is computed in raw (per-observation) units so that the z-score
    remains in a numerically precise range regardless of annualization factor.
    The benchmark_sr is interpreted in the same raw units.
    """
    pnl_arr = pd.to_numeric(pnl, errors="coerce").dropna().values
    n = len(pnl_arr)
    if n < 10:
        return 0.0
    sr = float(np.mean(pnl_arr) / np.std(pnl_arr, ddof=1))
    skew = float(stats.skew(pnl_arr))
    kurt = float(stats.kurtosis(pnl_arr, fisher=True))  # excess kurtosis
    # Standard error of SR (Lo 2002 adjusted for non-normality)
    se = np.sqrt(
        (1.0 + 0.5 * sr ** 2 - skew * sr + ((kurt + 3.0) / 4.0) * sr ** 2) / (n - 1)
    )
    if se <= 0.0:
        return 1.0 if sr > benchmark_sr else 0.0
    z = (sr - benchmark_sr) / se
    return float(stats.norm.cdf(z))


def deflated_sharpe_ratio(
    pnl: pd.Series,
    n_trials: int = 1,
    benchmark_sr: float = 0.0,
    periods_per_year: int = 105120,
) -> float:
    """
    DSR: PSR with benchmark SR replaced by the expected maximum SR from n_trials
    independent tests (assumes IID SR estimates across trials).
    Bailey & Lopez de Prado (2014).

    expected_max is a dimensionless z-score scaled by 1/sqrt(n), which is the
    standard deviation of the raw SR estimator (mean/std of pnl observations).
    """
    pnl_arr = pd.to_numeric(pnl, errors="coerce").dropna().values
    n = len(pnl_arr)
    if n < 10 or n_trials < 1:
        return 0.0
    # Expected maximum of n_trials standard normal draws (Euler–Mascheroni approximation)
    euler_mascheroni = 0.5772156649
    expected_max = (
        (1.0 - euler_mascheroni) * stats.norm.ppf(1.0 - 1.0 / n_trials)
        + euler_mascheroni * stats.norm.ppf(1.0 - 1.0 / (n_trials * np.e))
    ) if n_trials > 1 else 0.0
    # Std of the raw SR estimator across trials: 1/sqrt(n)
    sr_std = 1.0 / np.sqrt(n)
    # Deflated benchmark SR in raw units
    deflated_benchmark = max(benchmark_sr, expected_max * sr_std)
    return probabilistic_sharpe_ratio(pnl, benchmark_sr=deflated_benchmark, periods_per_year=periods_per_year)
