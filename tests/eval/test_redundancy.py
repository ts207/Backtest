import numpy as np
import pandas as pd
from eval.redundancy import greedy_diversified_subset

def test_returns_all_when_uncorrelated():
    rng = np.random.default_rng(0)
    n = 5
    pnl_matrix = pd.DataFrame({f"s{i}": rng.normal(0, 1, 500) for i in range(n)})
    subset = greedy_diversified_subset(pnl_matrix, max_corr=0.90, max_n=10)
    assert len(subset) == n  # all pass since orthogonal

def test_removes_redundant_correlated():
    rng = np.random.default_rng(0)
    base = rng.normal(0, 1, 500)
    # s1 is nearly identical to s0 (high correlation)
    pnl_matrix = pd.DataFrame({
        "s0": base,
        "s1": base + rng.normal(0, 0.01, 500),  # clone of s0
        "s2": rng.normal(0, 1, 500),
    })
    subset = greedy_diversified_subset(pnl_matrix, max_corr=0.95, max_n=10)
    # s0 and s1 should collapse to one; s2 is independent
    assert len(subset) == 2

def test_respects_max_n():
    rng = np.random.default_rng(0)
    pnl_matrix = pd.DataFrame({f"s{i}": rng.normal(0, 1, 500) for i in range(10)})
    subset = greedy_diversified_subset(pnl_matrix, max_corr=0.99, max_n=3)
    assert len(subset) <= 3
