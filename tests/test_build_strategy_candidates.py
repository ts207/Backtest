import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import build_strategy_candidates


def _write_edge_inputs(tmp_path: Path, run_id: str) -> None:
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    phase2_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    edge_dir.mkdir(parents=True, exist_ok=True)
    phase2_dir.mkdir(parents=True, exist_ok=True)

    source_json = phase2_dir / "promoted_candidates.json"
    source_json.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "event_type": "vol_shock_relaxation",
                "candidates": [
                    {
                        "condition": "age_bucket_0_8",
                        "action": "delay_30",
                        "sample_size": 120,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "vol_shock_relaxation_0",
                "status": "PROMOTED",
                "edge_score": 0.42,
                "expected_return_proxy": 0.01,
                "stability_proxy": 0.9,
                "n_events": 120,
                "source_path": str(source_json),
            }
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)


def test_build_strategy_candidates_from_promoted_edges(monkeypatch, tmp_path: Path) -> None:
    run_id = "strategy_builder_run"
    _write_edge_inputs(tmp_path, run_id=run_id)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--include_alpha_bundle",
            "0",
        ],
    )

    assert build_strategy_candidates.main() == 0

    out_json = tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json"
    assert out_json.exists()
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert len(payload) == 1
    assert payload[0]["base_strategy"] == "vol_compression_v1"
    assert payload[0]["action"] == "delay_30"
    assert payload[0]["risk_controls"]["entry_delay_bars"] == 30


def test_build_strategy_candidates_can_include_alpha_bundle(monkeypatch, tmp_path: Path) -> None:
    run_id = "strategy_builder_alpha"
    (tmp_path / "reports" / "edge_candidates" / run_id).mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        columns=[
            "run_id",
            "event",
            "candidate_id",
            "status",
            "edge_score",
            "expected_return_proxy",
            "stability_proxy",
            "n_events",
            "source_path",
        ]
    ).to_csv(tmp_path / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.csv", index=False)

    alpha_dir = tmp_path / "feature_store" / "alpha_bundle"
    alpha_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2021-01-01", periods=4, freq="1D", tz="UTC"),
            "symbol": ["BTCUSDT", "ETHUSDT", "BTCUSDT", "ETHUSDT"],
            "score": [0.1, -0.2, 0.3, -0.1],
        }
    ).to_csv(alpha_dir / "alpha_bundle_scores.csv", index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--include_alpha_bundle",
            "1",
        ],
    )

    assert build_strategy_candidates.main() == 0
    out_json = tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json"
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert len(payload) == 1
    assert payload[0]["source_type"] == "alpha_bundle"
