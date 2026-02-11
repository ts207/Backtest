import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import generate_hypothesis_queue


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x", encoding="utf-8")


def test_generator_writes_empty_queue_when_no_datasets_available(tmp_path: Path, monkeypatch) -> None:
    run_id = "hgen_empty"
    monkeypatch.setattr(generate_hypothesis_queue, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "generate_hypothesis_queue.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--datasets",
            "auto",
        ],
    )
    assert generate_hypothesis_queue.main() == 0

    out_dir = tmp_path / "reports" / "hypothesis_generator" / run_id
    assert (out_dir / "dataset_introspection.json").exists()
    assert (out_dir / "phase1_hypothesis_queue.jsonl").exists()
    queue_lines = (out_dir / "phase1_hypothesis_queue.jsonl").read_text(encoding="utf-8").strip()
    assert queue_lines == ""


def test_generator_emits_template_and_fusion_candidates_with_anti_leak_pass(tmp_path: Path, monkeypatch) -> None:
    run_id = "hgen_full"
    # Forced-flow + crowding + liquidity-state + sync datasets available.
    _touch(tmp_path / "lake" / "raw" / "binance" / "perp" / "BTCUSDT" / "liquidation_snapshot" / "year=2024" / "month=01" / "part.parquet")
    _touch(tmp_path / "lake" / "raw" / "binance" / "perp" / "BTCUSDT" / "open_interest" / "5m" / "year=2024" / "month=01" / "part.parquet")
    _touch(tmp_path / "lake" / "raw" / "binance" / "perp" / "BTCUSDT" / "funding" / "year=2024" / "month=01" / "part.parquet")
    _touch(tmp_path / "lake" / "raw" / "binance" / "perp" / "BTCUSDT" / "ohlcv_15m" / "year=2024" / "month=01" / "part.parquet")
    _touch(tmp_path / "lake" / "raw" / "binance" / "spot" / "BTCUSDT" / "ohlcv_15m" / "year=2024" / "month=01" / "part.parquet")
    _touch(tmp_path / "lake" / "runs" / run_id / "microstructure" / "quotes" / "quotes_BTCUSDT.parquet")
    _touch(tmp_path / "lake" / "runs" / run_id / "microstructure" / "trades" / "trades_BTCUSDT.parquet")

    monkeypatch.setattr(generate_hypothesis_queue, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "generate_hypothesis_queue.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--datasets",
            "auto",
            "--max_fused",
            "12",
        ],
    )
    assert generate_hypothesis_queue.main() == 0

    out_dir = tmp_path / "reports" / "hypothesis_generator" / run_id
    all_candidates = json.loads((out_dir / "hypothesis_candidates.json").read_text(encoding="utf-8"))
    queue_df = pd.read_csv(out_dir / "phase1_hypothesis_queue.csv")

    assert all_candidates
    assert not queue_df.empty
    assert set(queue_df["source_kind"].unique()) >= {"template", "fusion"}
    assert set(queue_df["fixed_horizon_bucket"].unique()).issubset({"short", "medium"})
    assert (queue_df["priority_score"] > 0).all()

    for row in all_candidates:
        checks = row["anti_leak_checks"]
        assert "passed" in checks
        assert checks["fixed_horizon_bucket"] is True
        assert checks["negative_controls_predefined"] is True


def test_anti_leak_check_fails_on_outcome_reference() -> None:
    candidate = {
        "mechanism_sentence": "State predicts return quickly.",
        "event_family_spec": {"event_type": "x", "pre_event_conditioning_only": True},
        "fixed_horizon_bucket": "short",
        "negative_controls": ["a", "b"],
        "required_datasets": ["um_liquidation_snapshot"],
    }
    checks = generate_hypothesis_queue._anti_leak_checks(candidate)
    assert checks["event_defined_without_outcome_reference"] is True
    assert checks["passed"] is False
