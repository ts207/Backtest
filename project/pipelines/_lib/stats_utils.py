from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats

def calculate_kendalls_tau(x: np.ndarray | pd.Series, y: np.ndarray | pd.Series) -> float:
    """
    Calculate Kendall's Tau rank correlation.
    """
    tau, _ = stats.kendalltau(x, y)
    return float(tau)

def test_cointegration(x: pd.Series, y: pd.Series) -> float:
    """
    Engle-Granger cointegration test (simplified).
    Returns the p-value of the ADF test on the residuals of OLS.
    Note: Requires statsmodels for a proper implementation. 
    Currently returns 0.0 if highly correlated as a proxy.
    """
    # Placeholder: in a real production system, we'd use statsmodels.tsa.stattools.coint
    # For now, we'll return a proxy based on correlation or suggest adding statsmodels.
    correlation = x.corr(y)
    if abs(correlation) > 0.95:
        return 0.01 # Proxy for "cointegrated"
    return 0.50 # Proxy for "not cointegrated"
