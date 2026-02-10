import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research._lib.metrics import symbol_positive_pnl_ratio, yearly_sign_consistency


def test_yearly_sign_consistency_computes_majority_sign(tmp_path: Path) -> None:
    path = tmp_path / "portfolio.csv"
    pd.DataFrame(
        {
            "timestamp": [
                "2021-01-01T00:00:00Z",
                "2022-01-01T00:00:00Z",
                "2023-01-01T00:00:00Z",
            ],
            "portfolio_pnl": [1.0, -0.5, 0.3],
        }
    ).to_csv(path, index=False)
    # signs: +, -, + => dominant + => 2/3
    assert abs(yearly_sign_consistency(path) - (2.0 / 3.0)) < 1e-9


def test_symbol_positive_pnl_ratio_counts_positive_symbols(tmp_path: Path) -> None:
    path = tmp_path / "symbol_contrib.csv"
    pd.DataFrame({"symbol": ["BTC", "ETH", "SOL"], "total_pnl": [1.0, -0.1, 0.0]}).to_csv(path, index=False)
    assert abs(symbol_positive_pnl_ratio(path) - (1.0 / 3.0)) < 1e-9
