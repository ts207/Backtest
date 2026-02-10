import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines._lib.universe import compute_monthly_top_n_symbols


def _write_cleaned(root: Path, run_id: str, symbol: str, volume_scale: float) -> None:
    ts = pd.date_range("2024-01-01", periods=400, freq="15min", tz="UTC")
    bars = pd.DataFrame(
        {
            "timestamp": ts,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": volume_scale,
            "is_gap": False,
            "gap_len": 0,
        }
    )
    out = root / "lake" / "runs" / run_id / "cleaned" / "perp" / symbol / "bars_15m" / "year=2024" / "month=01"
    out.mkdir(parents=True, exist_ok=True)
    bars.to_parquet(out / f"bars_{symbol}_15m_2024-01.parquet", index=False)


def test_compute_monthly_top_n_symbols_ranks_by_dollar_volume(tmp_path: Path) -> None:
    run_id = "run_top"
    _write_cleaned(tmp_path, run_id, "BTCUSDT", 1000.0)
    _write_cleaned(tmp_path, run_id, "ETHUSDT", 200.0)

    mapping = compute_monthly_top_n_symbols(
        data_root=tmp_path,
        run_id=run_id,
        symbols=["BTCUSDT", "ETHUSDT"],
        top_n=1,
        lookback_days=30,
        start_ts=pd.Timestamp("2024-01-15", tz="UTC"),
        end_ts=pd.Timestamp("2024-02-15", tz="UTC"),
        fallback_seed=["ETHUSDT", "BTCUSDT"],
    )

    # For February rebalance, BTC should rank first due to larger dollar volume.
    assert mapping["2024-02"][0] == "BTCUSDT"
