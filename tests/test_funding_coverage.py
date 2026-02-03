import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.clean.build_cleaned_15m import _align_funding
from pipelines.report.make_report import _format_funding_section


def test_funding_coverage_stats():
    bars = pd.DataFrame(
        {
            "timestamp": pd.date_range(
                "2023-12-31 23:00",
                periods=96,
                freq="15min",
                tz="UTC",
            )
        }
    )
    funding = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-01 00:00", "2024-01-01 08:00", "2024-01-01 16:00"],
                utc=True,
            ),
            "funding_rate_scaled": [0.0001, 0.0002, 0.0003],
        }
    )

    aligned, missing_pct = _align_funding(bars, funding)
    funding_missing = float(aligned["funding_missing"].mean()) if len(aligned) else 0.0
    coverage = float(aligned["funding_event_ts"].notna().mean()) if len(aligned) else 0.0
    expected_missing = 4 / 96
    expected_coverage = 1.0 - expected_missing

    assert np.isclose(missing_pct, expected_missing)
    assert np.isclose(funding_missing, expected_missing)
    assert np.isclose(coverage, expected_coverage)


def test_report_uses_coverage():
    cleaned_stats = {
        "symbols": {
            "BTCUSDT": {
                "pct_missing_funding_event": {
                    "2024-01": {
                        "pct_missing_funding_event": 0.25,
                        "pct_funding_event_coverage": 0.75,
                    }
                },
                "pct_bars_with_funding_event": 0.75,
                "funding_rate_scaled_min": 0.0001,
                "funding_rate_scaled_max": 0.0003,
                "funding_rate_scaled_std": 0.00005,
            }
        }
    }
    lines = _format_funding_section(cleaned_stats)
    joined = "\n".join(lines)

    assert "Funding coverage (%) by month" in joined
    assert "2024-01: 75.00%" in joined
    assert "% bars with funding_event_ts: 75.00%" in joined
    assert "funding_rate_scaled min/max/std: 0.000100 / 0.000300 / 0.000050" in joined
