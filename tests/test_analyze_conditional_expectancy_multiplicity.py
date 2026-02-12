import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import analyze_conditional_expectancy as ace


def test_bh_adjust_returns_expected_values() -> None:
    p = pd.Series([0.01, 0.04, 0.03, 0.002])
    adjusted = ace._bh_adjust(p)
    expected = [0.02, 0.04, 0.04, 0.008]
    assert adjusted.round(6).tolist() == expected


def test_select_expectancy_candidates_requires_adjusted_significance() -> None:
    df = pd.DataFrame(
        [
            {"condition": "a", "samples": 200, "mean_return": 0.01, "t_stat": 3.0, "significant_adj_bh": True},
            {"condition": "b", "samples": 200, "mean_return": 0.01, "t_stat": 3.0, "significant_adj_bh": False},
            {"condition": "c", "samples": 50, "mean_return": 0.01, "t_stat": 3.0, "significant_adj_bh": True},
        ]
    )
    out = ace._select_expectancy_candidates(df, min_samples=100, tstat_threshold=2.0)
    assert out["condition"].tolist() == ["a"]
