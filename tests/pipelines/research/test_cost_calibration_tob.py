from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research.cost_calibration import ToBRegimeCostCalibrator


def _write_tob_agg(root: Path, symbol: str) -> None:
    out_dir = root / "lake" / "cleaned" / "perp" / symbol / "tob_5m_agg" / "year=2024" / "month=01"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = pd.date_range("2024-01-01 00:00:00", periods=24, freq="5min", tz="UTC")
    frame = pd.DataFrame(
        {
            "timestamp": ts,
            "spread_bps_mean": [2.0] * 20 + [8.0] * 4,
            "bid_depth_usd_mean": [500_000.0] * 20 + [80_000.0] * 4,
            "ask_depth_usd_mean": [500_000.0] * 20 + [80_000.0] * 4,
        }
    )
    frame.to_parquet(out_dir / "tob_agg_BTCUSDT_5m_2024-01.parquet", index=False)


def test_tob_regime_calibration_increases_cost_in_wider_spread_regime(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_tob_agg(data_root, "BTCUSDT")

    calibrator = ToBRegimeCostCalibrator(
        run_id="r1",
        data_root=data_root,
        base_fee_bps=4.0,
        base_slippage_bps=2.0,
        static_cost_bps=6.0,
        mode="tob_regime",
        min_tob_coverage=0.6,
        tob_tolerance_minutes=10,
    )

    # Event timestamps intentionally land in the high-spread / low-depth tail.
    events = pd.DataFrame(
        {"enter_ts": pd.date_range("2024-01-01 01:40:00", periods=4, freq="5min", tz="UTC")}
    )
    est = calibrator.estimate(symbol="BTCUSDT", events_df=events)

    assert est.cost_model_valid is True
    assert est.cost_model_source == "tob_regime"
    assert est.cost_input_coverage >= 0.99
    assert est.regime_multiplier > 1.0
    assert est.cost_bps > 6.0


def test_tob_regime_calibration_falls_back_to_static_when_coverage_low(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_tob_agg(data_root, "BTCUSDT")

    calibrator = ToBRegimeCostCalibrator(
        run_id="r1",
        data_root=data_root,
        base_fee_bps=4.0,
        base_slippage_bps=2.0,
        static_cost_bps=6.0,
        mode="tob_regime",
        min_tob_coverage=0.9,
        tob_tolerance_minutes=1,
    )

    # 7-minute offsets with tight tolerance ensure no asof matches.
    events = pd.DataFrame(
        {"enter_ts": pd.date_range("2024-01-01 00:07:00", periods=6, freq="5min", tz="UTC")}
    )
    est = calibrator.estimate(symbol="BTCUSDT", events_df=events)

    assert est.cost_model_valid is False
    assert est.cost_model_source.startswith("fallback:")
    assert est.cost_bps == 6.0
    assert est.regime_multiplier == 1.0

