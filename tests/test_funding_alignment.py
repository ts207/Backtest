
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "project"))

import pandas as pd
from pipelines.engine.pnl import _assert_funding_alignment

def test_funding_alignment_contract():
    pos = pd.Series([0, 1, 1, 0])
    funding = pd.Series([0, 0, 0.01, 0])  # funding at bar 2 where prior pos (bar 1) is 1
    _assert_funding_alignment(pos, funding)
