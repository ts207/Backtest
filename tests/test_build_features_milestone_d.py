import os
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _seed_cleaned(tmp_path: Path, symbol: str) -> None:
    ts = pd.date_range("2024-01-01", periods=200, freq="15min", tz="UTC")
    bars = pd.DataFrame(
        {
            "timestamp": ts,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": [100.0 + (i * 0.01) for i in range(len(ts))],
            "funding_rate_scaled": 0.0,
            "funding_rate": 0.0,
        }
    )
    _write_csv(bars, tmp_path / "lake" / "cleaned" / "perp" / symbol / "bars_15m" / "part.csv")


def test_build_features_includes_oi_liquidation_and_revision_lag(tmp_path: Path) -> None:
    run_id = "feat_d"
    symbol = "BTCUSDT"
    _seed_cleaned(tmp_path, symbol)

    ts = pd.date_range("2024-01-01", periods=60, freq="5min", tz="UTC")
    oi = pd.DataFrame({"timestamp": ts, "sum_open_interest_value": [1000.0 + i for i in range(len(ts))]})
    _write_csv(oi, tmp_path / "lake" / "raw" / "binance" / "perp" / symbol / "open_interest" / "5m" / "part.csv")

    liq = pd.DataFrame({"timestamp": ts, "notional_usd": [0.0] * 59 + [2500.0], "event_count": [0] * 59 + [3]})
    _write_csv(liq, tmp_path / "lake" / "raw" / "binance" / "perp" / symbol / "liquidation_snapshot" / "part.csv")

    env = os.environ.copy()
    env["BACKTEST_DATA_ROOT"] = str(tmp_path)
    cmd = [
        sys.executable,
        str(ROOT / "project" / "pipelines" / "features" / "build_features_v1.py"),
        "--run_id",
        run_id,
        "--symbols",
        symbol,
        "--market",
        "perp",
        "--revision_lag_bars",
        "3",
        "--force",
        "1",
    ]
    subprocess.run(cmd, check=True, env=env)

    out_dir = tmp_path / "lake" / "features" / "perp" / symbol / "15m" / "features_v1"
    files = sorted(out_dir.rglob("*.csv")) + sorted(out_dir.rglob("*.parquet"))
    assert files
    if files[0].suffix == ".csv":
        frame = pd.read_csv(files[0])
    else:
        frame = pd.read_parquet(files[0])

    for col in [
        "oi_notional",
        "oi_delta_1h",
        "liquidation_notional",
        "liquidation_count",
        "basis_bps",
        "basis_zscore",
        "cross_exchange_spread_z",
        "spread_zscore",
        "revision_lag_bars",
        "revision_lag_minutes",
    ]:
        assert col in frame.columns
    assert int(frame["revision_lag_bars"].iloc[-1]) == 3
    assert int(frame["revision_lag_minutes"].iloc[-1]) == 45
    manifest = json.loads((tmp_path / "runs" / run_id / "build_features_v1.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "success"
    assert "feature_schema_version" in manifest["stats"]
    assert "feature_schema_hash" in manifest["stats"]


def test_build_features_prefers_run_scoped_cleaned_input(tmp_path: Path) -> None:
    run_id = "feat_run_scoped_priority"
    symbol = "BTCUSDT"
    ts = pd.date_range("2024-01-01", periods=100, freq="15min", tz="UTC")

    global_bars = pd.DataFrame(
        {
            "timestamp": ts,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 111.0,
            "funding_rate_scaled": 0.0,
            "funding_rate": 0.0,
        }
    )
    run_bars = global_bars.copy()
    run_bars["close"] = 222.0

    _write_csv(global_bars, tmp_path / "lake" / "cleaned" / "perp" / symbol / "bars_15m" / "part.csv")
    _write_csv(
        run_bars,
        tmp_path / "lake" / "runs" / run_id / "cleaned" / "perp" / symbol / "bars_15m" / "part.csv",
    )

    env = os.environ.copy()
    env["BACKTEST_DATA_ROOT"] = str(tmp_path)
    cmd = [
        sys.executable,
        str(ROOT / "project" / "pipelines" / "features" / "build_features_v1.py"),
        "--run_id",
        run_id,
        "--symbols",
        symbol,
        "--market",
        "perp",
        "--force",
        "1",
    ]
    subprocess.run(cmd, check=True, env=env)

    out_dir = tmp_path / "lake" / "features" / "perp" / symbol / "15m" / "features_v1"
    files = sorted(out_dir.rglob("*.csv")) + sorted(out_dir.rglob("*.parquet"))
    assert files
    frame = pd.read_csv(files[0]) if files[0].suffix == ".csv" else pd.read_parquet(files[0])
    assert float(frame["close"].iloc[0]) == 222.0

    compat_out_dir = tmp_path / "lake" / "features" / "perp" / symbol / "15m" / "features_v1"
    compat_files = sorted(compat_out_dir.rglob("*.csv")) + sorted(compat_out_dir.rglob("*.parquet"))
    assert compat_files
