from __future__ import annotations

from typing import List

import pandas as pd


def greedy_diversified_subset(
    pnl_matrix: pd.DataFrame,
    max_corr: float = 0.70,
    max_n: int = 20,
) -> List[str]:
    """
    Greedy max-diversification: iteratively add strategies whose pairwise
    Pearson correlation with all already-selected strategies is below max_corr.

    Args:
        pnl_matrix: DataFrame where each column is a strategy's PnL series.
        max_corr: Maximum allowed pairwise absolute correlation with any selected strategy.
        max_n: Maximum number of strategies to select.

    Returns:
        List of selected column names.
    """
    cols = list(pnl_matrix.columns)
    if not cols:
        return []
    corr = pnl_matrix.corr().abs()
    selected: List[str] = []
    # Order by ascending mean correlation to prefer most independent strategies first
    mean_corr = corr.mean()
    ordered = [c for c in mean_corr.sort_values().index if c in cols]

    for candidate in ordered:
        if len(selected) >= max_n:
            break
        if not selected:
            selected.append(candidate)
            continue
        max_with_selected = max(
            float(corr.loc[candidate, s]) for s in selected
            if candidate != s and s in corr.columns
        )
        if max_with_selected < float(max_corr):
            selected.append(candidate)
    return selected
