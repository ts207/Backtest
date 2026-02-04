import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.clean.build_cleaned_15m import _detect_bad_bars


def test_bad_bar_detection():
    bars = pd.DataFrame(
        {
            "open": [100.0, 100.0, 101.0, 102.0],
            "high": [100.0, 100.0, 102.0, 103.0],
            "low": [100.0, 100.0, 100.0, 101.0],
            "close": [100.0, 100.0, 101.5, 102.5],
            "volume": [0.0, 0.0, 10.0, 5.0],
        }
    )

    mask = _detect_bad_bars(bars)
    assert mask.sum() == 2
    assert bool(mask.iloc[0]) is True
    assert bool(mask.iloc[1]) is True
    assert bool(mask.iloc[2]) is False
