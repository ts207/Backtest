import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.features import build_market_context


def _write_features_fixture(tmp_path: Path, run_id: str, symbol: str) -> None:
    ts = pd.date_range("2021-01-01 00:00", periods=192, freq="15min", tz="UTC")
    features = pd.DataFrame(
        {
            "timestamp": ts,
            "close": 100.0 + pd.Series(range(len(ts)), dtype=float) * 0.1,
            "rv_96": 0.001,
            "rv_pct_17280": 55.0,
            "range_96": 2.0,
            "range_med_2880": 3.0,
            "funding_rate_scaled": 0.0002,
        }
    )
    out_dir = (
        tmp_path
        / "lake"
        / "runs"
        / run_id
        / "features"
        / "perp"
        / symbol
        / "15m"
        / "features_v1"
        / "year=2021"
        / "month=01"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    features.to_parquet(out_dir / "features_BTCUSDT_v1_2021-01.parquet", index=False)


def test_build_market_context_writes_expected_output(monkeypatch, tmp_path: Path) -> None:
    run_id = "market_ctx_run"
    symbol = "BTCUSDT"
    _write_features_fixture(tmp_path, run_id=run_id, symbol=symbol)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_market_context, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_market_context.py",
            "--run_id",
            run_id,
            "--symbols",
            symbol,
            "--timeframe",
            "15m",
            "--start",
            "2021-01-01",
            "--end",
            "2021-01-02",
            "--force",
            "1",
        ],
    )

    assert build_market_context.main() == 0

    out_path = tmp_path / "lake" / "runs" / run_id / "context" / "market_state" / symbol / "15m.parquet"
    assert out_path.exists()
    out = pd.read_parquet(out_path)
    assert len(out) == 192
    assert {"trend_regime", "vol_regime", "compression_state", "funding_regime", "context_def_version"}.issubset(out.columns)

    manifest_path = tmp_path / "runs" / run_id / "build_market_context.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "success"
    assert manifest["stats"]["symbols"][symbol]["rows"] == 192
