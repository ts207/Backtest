import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.report.capital_allocation import AllocationConfig, build_allocation_weights
from pipelines.report import make_report


def test_build_allocation_weights_applies_caps_floors_and_vol_target() -> None:
    df = pd.DataFrame(
        [
            {
                "candidate_symbol": "BTCUSDT",
                "status": "PROMOTED",
                "expectancy_per_trade": 0.03,
                "variance": 0.0009,
                "n_events": 80,
            },
            {
                "candidate_symbol": "ETHUSDT",
                "status": "PROMOTED",
                "expectancy_per_trade": 0.02,
                "variance": 0.0004,
                "n_events": 50,
            },
            {
                "candidate_symbol": "SOLUSDT",
                "status": "PROMOTED",
                "expectancy_per_trade": 0.005,
                "variance": 0.0001,
                "n_events": 30,
            },
            {
                "candidate_symbol": "ALL",
                "status": "PROMOTED",
                "expectancy_per_trade": 0.5,
                "variance": 0.0001,
                "n_events": 10,
            },
        ]
    )

    out = build_allocation_weights(
        df,
        AllocationConfig(max_weight=0.5, min_weight=0.2, volatility_target=0.02, volatility_adjustment_cap=2.0),
    )

    assert set(out["symbol"]) == {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
    assert abs(float(out["final_weight"].sum()) - 1.0) < 1e-9
    assert (out["final_weight"] <= 0.5 + 1e-9).all()
    assert (out["final_weight"] >= 0.2 - 1e-9).all()


def test_make_report_writes_allocation_artifacts(tmp_path: Path, monkeypatch) -> None:
    run_id = "alloc_report"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(make_report, "DATA_ROOT", tmp_path)

    trades_dir = tmp_path / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / run_id
    trades_dir.mkdir(parents=True, exist_ok=True)
    (trades_dir / "metrics.json").write_text(
        json.dumps({"total_trades": 0, "avg_r": 0.0, "win_rate": 0.0, "ending_equity": 100000.0, "sharpe_annualized": 0.0}),
        encoding="utf-8",
    )

    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    edge_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"candidate_symbol": "BTCUSDT", "status": "PROMOTED", "expectancy_per_trade": 0.03, "variance": 0.0009, "n_events": 50},
            {"candidate_symbol": "ETHUSDT", "status": "PROMOTED", "expectancy_per_trade": 0.02, "variance": 0.0004, "n_events": 50},
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "make_report.py",
            "--run_id",
            run_id,
        ],
    )
    assert make_report.main() == 0

    out_dir = tmp_path / "reports" / "vol_compression_expansion_v1" / run_id
    alloc_csv = out_dir / "allocation_weights.csv"
    alloc_json = out_dir / "allocation_weights.json"
    assert alloc_csv.exists()
    assert alloc_json.exists()

    alloc_df = pd.read_csv(alloc_csv)
    assert not alloc_df.empty
    assert abs(float(alloc_df["final_weight"].sum()) - 1.0) < 1e-9
