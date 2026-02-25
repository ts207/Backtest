from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats

def fit_gaussian_copula(u1: np.ndarray, u2: np.ndarray) -> float:
    """
    Estimate the correlation parameter rho for a Gaussian copula.
    Transforms uniforms to standard normal and calculates Pearson correlation.
    """
    # Inverse probability transform
    z1 = stats.norm.ppf(np.clip(u1, 1e-6, 1 - 1e-6))
    z2 = stats.norm.ppf(np.clip(u2, 1e-6, 1 - 1e-6))
    
    rho = np.corrcoef(z1, z2)[0, 1]
    return float(rho)

def calculate_gaussian_conditional_prob(u1: float, u2: float, rho: float) -> float:
    """
    Calculate P(U1 <= u1 | U2 = u2) for a Gaussian copula with correlation rho.
    """
    z1 = stats.norm.ppf(np.clip(u1, 1e-6, 1 - 1e-6))
    z2 = stats.norm.ppf(np.clip(u2, 1e-6, 1 - 1e-6))
    
    num = z1 - rho * z2
    denom = np.sqrt(1 - rho**2)
    
    prob = stats.norm.cdf(num / denom)
    return float(prob)

def get_empirical_uniforms(x: pd.Series) -> pd.Series:
    """
    Transform a series to empirical uniforms (ranks / N).
    """
    return x.rank(pct=True)
