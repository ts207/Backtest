import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from engine.execution_model import estimate_transaction_cost_bps


def test_execution_model_cost_increases_with_turnover() -> None:
    idx = pd.date_range("2024-01-01", periods=4, freq="15min", tz="UTC")
    frame = pd.DataFrame(
        {
            "spread_bps": [2.0, 2.0, 2.0, 2.0],
            "atr_14": [10.0, 10.0, 10.0, 10.0],
            "quote_volume": [1_000_000.0, 1_000_000.0, 1_000_000.0, 1_000_000.0],
            "close": [100.0, 100.0, 100.0, 100.0],
            "high": [101.0, 101.0, 101.0, 101.0],
            "low": [99.0, 99.0, 99.0, 99.0],
        },
        index=idx,
    )
    low_turnover = pd.Series([0.1, 0.1, 0.1, 0.1], index=idx)
    high_turnover = pd.Series([1.0, 1.0, 1.0, 1.0], index=idx)
    c1 = estimate_transaction_cost_bps(frame, low_turnover, {})
    c2 = estimate_transaction_cost_bps(frame, high_turnover, {})
    assert float(c2.mean()) >= float(c1.mean())
