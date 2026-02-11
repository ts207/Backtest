from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import run_promoted_edge_audits as mod


def test_run_promoted_edge_audits_generates_family_summary(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(mod, "DATA_ROOT", tmp_path)
    run_id = "r_prom"

    phase2_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    phase2_dir.mkdir(parents=True, exist_ok=True)
    (phase2_dir / "promoted_candidates.json").write_text(
        json.dumps(
            [
                {
                    "candidate_id": "vol_shock_relaxation_0",
                    "edge_score": 0.4,
                    "expected_return_proxy": 0.002,
                }
            ]
        ),
        encoding="utf-8",
    )

    phase1_dir = tmp_path / "reports" / "vol_shock_relaxation" / run_id
    phase1_dir.mkdir(parents=True, exist_ok=True)
    events = pd.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "enter_ts": ["2024-01-01T00:00:00Z"],
            "exit_ts": ["2024-01-01T00:00:00Z"],
        }
    )
    events.to_csv(phase1_dir / "vol_shock_relaxation_events.csv", index=False)

    bars_dir = tmp_path / "lake" / "runs" / run_id / "cleaned" / "perp" / "BTCUSDT" / "bars_15m" / "year=2024" / "month=01"
    bars_dir.mkdir(parents=True, exist_ok=True)
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01T00:15:00Z", "2024-01-01T00:30:00Z"], utc=True),
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [101.0, 100.0],
        }
    )
    bars.to_parquet(bars_dir / "bars_BTCUSDT_15m_2024-01.parquet", index=False)

    payload = mod.run_promoted_edge_audits(
        run_id=run_id,
        horizon_bars=1,
        top_n=1,
        fee_bps_per_side=0.0,
        spread_bps_per_side=0.0,
    )

    assert payload["family_count"] == 1
    out_dir = tmp_path / "reports" / "promotion_audits" / run_id / "vol_shock_relaxation"
    assert (out_dir / "summary.md").exists()
    summary = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["candidates"][0]["trades"] == 1
    assert summary["candidates"][0]["mean_net_return"] < 0
    assert summary["candidates"][0]["events_loaded"] == 1
    assert "fees_paid_return" in summary["candidates"][0]
    assert "independence" in payload
