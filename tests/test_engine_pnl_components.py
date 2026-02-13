import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from engine.pnl import compute_pnl_components


def test_compute_pnl_components_reconciles_net_with_funding_and_borrow() -> None:
    idx = pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T00:15:00Z", "2024-01-01T00:30:00Z"])
    pos = pd.Series([0.0, -1.0, -1.0], index=idx)
    ret = pd.Series([0.0, 0.01, -0.02], index=idx)
    funding = pd.Series([0.0, 0.001, 0.001], index=idx)
    borrow = pd.Series([0.0, 0.0002, 0.0002], index=idx)

    out = compute_pnl_components(pos=pos, ret=ret, cost_bps=10.0, funding_rate=funding, borrow_rate=borrow)

    assert set(["gross_pnl", "trading_cost", "funding_pnl", "borrow_cost", "pnl"]).issubset(out.columns)
    reconciled = out["gross_pnl"] - out["trading_cost"] + out["funding_pnl"] - out["borrow_cost"]
    assert (out["pnl"].round(12) == reconciled.round(12)).all()
    assert float(out["borrow_cost"].sum()) > 0.0
