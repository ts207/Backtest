from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import evaluate_naive_entry


def _write_phase2_candidates(base: Path, run_id: str, event_type: str) -> None:
    out_dir = base / "reports" / "phase2" / run_id / event_type
    out_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"candidate_id": "all__delay_8", "condition": "all", "action": "delay_8", "expectancy_per_trade": 0.01},
            {"candidate_id": "session_eu__delay_8", "condition": "session_eu", "action": "delay_8", "expectancy_per_trade": 0.01},
        ]
    ).to_csv(out_dir / "phase2_candidates.csv", index=False)


def _write_phase1_events(base: Path, run_id: str, event_type: str) -> None:
    out_dir = base / "reports" / event_type / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"enter_ts": "2024-01-01T09:00:00Z", "forward_return_h": 0.02},
            {"enter_ts": "2024-01-01T10:00:00Z", "forward_return_h": 0.01},
            {"enter_ts": "2024-01-01T23:00:00Z", "forward_return_h": -0.01},
        ]
    ).to_csv(out_dir / f"{event_type}_events.csv", index=False)


def test_evaluate_naive_entry_writes_artifacts(monkeypatch, tmp_path: Path) -> None:
    run_id = "naive_eval_ok"
    event_type = "cross_venue_desync"
    _write_phase2_candidates(tmp_path, run_id, event_type)
    _write_phase1_events(tmp_path, run_id, event_type)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(evaluate_naive_entry, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "evaluate_naive_entry.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--min_trades",
            "1",
            "--min_expectancy_after_cost",
            "0.0",
            "--max_drawdown",
            "-1.0",
        ],
    )
    assert evaluate_naive_entry.main() == 0

    out_dir = tmp_path / "reports" / "naive_entry" / run_id
    csv_path = out_dir / "naive_entry_validation.csv"
    json_path = out_dir / "naive_entry_validation.json"
    assert csv_path.exists()
    assert json_path.exists()

    frame = pd.read_csv(csv_path)
    assert {"candidate_id", "naive_total_trades", "naive_expectancy_after_cost", "naive_pass"} <= set(frame.columns)
    assert set(frame["candidate_id"].astype(str)) == {"all__delay_8", "session_eu__delay_8"}
    assert bool(frame["naive_pass"].astype(bool).any()) is True

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert int(payload["tested_count"]) == 2
    assert int(payload["pass_count"]) >= 1


def test_evaluate_naive_entry_fails_without_phase1_events(monkeypatch, tmp_path: Path) -> None:
    run_id = "naive_eval_missing_phase1"
    event_type = "vol_shock_relaxation"
    _write_phase2_candidates(tmp_path, run_id, event_type)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(evaluate_naive_entry, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "evaluate_naive_entry.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
        ],
    )
    assert evaluate_naive_entry.main() == 1
    manifest_path = tmp_path / "runs" / run_id / "evaluate_naive_entry.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert "Missing Phase-1 events file" in str(payload.get("error", ""))
