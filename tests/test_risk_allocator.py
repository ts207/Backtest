import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from engine.risk_allocator import RiskLimits, allocate_position_scales


def test_allocate_position_scales_respects_caps() -> None:
    idx = pd.date_range("2024-01-01", periods=4, freq="15min", tz="UTC")
    pos = {
        "s1": pd.Series([1, 1, 1, 1], index=idx, dtype=float),
        "s2": pd.Series([1, 1, 1, 1], index=idx, dtype=float),
    }
    req_scale = {
        "s1": pd.Series([1.0, 1.0, 1.0, 1.0], index=idx, dtype=float),
        "s2": pd.Series([1.0, 1.0, 1.0, 1.0], index=idx, dtype=float),
    }
    scales, diag = allocate_position_scales(
        raw_positions_by_strategy=pos,
        requested_scale_by_strategy=req_scale,
        limits=RiskLimits(max_portfolio_gross=1.0, max_symbol_gross=1.0, max_strategy_gross=0.8, max_new_exposure_per_bar=1.0),
    )
    assert set(scales.keys()) == {"s1", "s2"}
    gross = (pos["s1"] * scales["s1"]).abs() + (pos["s2"] * scales["s2"]).abs()
    assert float(gross.max()) <= 1.000001
    assert float(diag["requested_gross"]) > float(diag["allocated_gross"])
