import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from engine import runner
from pipelines._lib.io_utils import run_scoped_lake_path


def test_load_symbol_data_dedupes_feature_timestamps_before_context_merge(tmp_path: Path) -> None:
    run_id = "runner_dedupe"
    symbol = "BTCUSDT"
    ts = pd.date_range("2024-01-01", periods=6, freq="15min", tz="UTC")
    bars = pd.DataFrame(
        {
            "timestamp": ts,
            "open": [100.0] * len(ts),
            "high": [101.0] * len(ts),
            "low": [99.0] * len(ts),
            "close": [100.0] * len(ts),
        }
    )
    features = pd.DataFrame(
        {
            "timestamp": list(ts) + [ts[2], ts[4]],
            "high_96": [101.0] * (len(ts) + 2),
            "low_96": [99.0] * (len(ts) + 2),
            "spread_bps": [2.0] * (len(ts) + 2),
        }
    )

    bars_dir = run_scoped_lake_path(tmp_path, run_id, "cleaned", "perp", symbol, "bars_15m")
    features_dir = run_scoped_lake_path(tmp_path, run_id, "features", "perp", symbol, "15m", "features_v1")
    bars_dir.mkdir(parents=True, exist_ok=True)
    features_dir.mkdir(parents=True, exist_ok=True)
    bars.to_parquet(bars_dir / "part-00000.parquet", index=False)
    features.to_parquet(features_dir / "part-00000.parquet", index=False)

    loaded_bars, loaded_features = runner._load_symbol_data(
        data_root=tmp_path,
        symbol=symbol,
        run_id=run_id,
    )
    assert not loaded_bars["timestamp"].duplicated().any()
    assert not loaded_features["timestamp"].duplicated().any()
    assert len(loaded_features) == len(ts)
