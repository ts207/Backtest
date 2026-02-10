import json
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.backtest import backtest_multi_edge_portfolio as multi_stage


def _write_fixture(root: Path, run_id: str, symbol: str, price_step: float) -> None:
    periods = 160
    timestamps = pd.date_range("2024-01-01 00:00", periods=periods, freq="15min", tz="UTC")
    close = [100.0 + i * price_step for i in range(periods)]
    bars = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": close,
            "high": [x + 1.0 for x in close],
            "low": [x - 1.0 for x in close],
            "close": close,
            "volume": [1000.0] * periods,
            "is_gap": [False] * periods,
            "gap_len": [0] * periods,
        }
    )
    features = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": bars["open"],
            "high": bars["high"],
            "low": bars["low"],
            "close": bars["close"],
            "funding_event_ts": pd.NaT,
            "funding_rate_scaled": 0.0,
            "rv_pct_2880": [5.0] * periods,
            "high_96": [100.0] * periods,
            "low_96": [90.0] * periods,
            "range_96": [10.0] * periods,
            "range_med_480": [20.0] * periods,
        }
    )

    features_dir = root / "lake" / "runs" / run_id / "features" / "perp" / symbol / "15m" / "features_v1" / "year=2024" / "month=01"
    bars_dir = root / "lake" / "runs" / run_id / "cleaned" / "perp" / symbol / "bars_15m" / "year=2024" / "month=01"
    features_dir.mkdir(parents=True, exist_ok=True)
    bars_dir.mkdir(parents=True, exist_ok=True)
    features.to_parquet(features_dir / f"features_{symbol}_v1_2024-01.parquet", index=False)
    bars.to_parquet(bars_dir / f"bars_{symbol}_15m_2024-01.parquet", index=False)


def test_multi_edge_backtest_writes_metrics(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = "run_multi"
    _write_fixture(tmp_path, run_id, "BTCUSDT", 0.05)
    _write_fixture(tmp_path, run_id, "ETHUSDT", 0.03)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(multi_stage, "DATA_ROOT", tmp_path)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest_multi_edge_portfolio.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--modes",
            "equal_risk,score_weighted",
            "--force",
            "1",
        ],
    )
    assert multi_stage.main() == 0

    metrics_path = tmp_path / "lake" / "trades" / "backtests" / "multi_edge_portfolio" / run_id / "metrics.json"
    assert metrics_path.exists()

    payload = json.loads(metrics_path.read_text())
    assert payload["objective"]["target_metric"] == "net_total_return"
    assert payload["selected_mode"] in {"equal_risk", "score_weighted"}
    assert "modes" in payload and payload["modes"]
