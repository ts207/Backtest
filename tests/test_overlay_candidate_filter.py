import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import build_strategy_candidates


def _write_mixed_edge_inputs(tmp_path: Path, run_id: str) -> None:
    """Write edge CSV with both 'edge' and 'overlay' candidate_type rows."""
    event = "vol_shock_relaxation"
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    phase2_dir = tmp_path / "reports" / "phase2" / run_id / event
    edge_dir.mkdir(parents=True, exist_ok=True)
    phase2_dir.mkdir(parents=True, exist_ok=True)

    source_json = phase2_dir / "promoted_candidates.json"
    source_json.write_text(
        json.dumps({
            "run_id": run_id,
            "event_type": event,
            "candidates": [
                {
                    "candidate_id": f"{event}_edge",
                    "condition": "all",
                    "action": "delay_30",
                    "sample_size": 120,
                    "gate_oos_consistency_strict": True,
                    "gate_bridge_tradable": True,
                },
                {
                    "candidate_id": f"{event}_overlay",
                    "condition": "all",
                    "action": "risk_throttle_half",
                    "sample_size": 80,
                    "gate_oos_consistency_strict": True,
                    "gate_bridge_tradable": True,
                },
            ],
        }),
        encoding="utf-8",
    )

    pd.DataFrame([
        {
            "run_id": run_id,
            "event": event,
            "candidate_id": f"{event}_edge",
            "candidate_type": "edge",
            "overlay_base_candidate_id": "",
            "status": "PROMOTED",
            "edge_score": 0.55,
            "expected_return_proxy": 0.01,
            "expectancy_per_trade": 0.01,
            "stability_proxy": 0.9,
            "robustness_score": 0.9,
            "event_frequency": 0.5,
            "capacity_proxy": 1.0,
            "profit_density_score": 0.005,
            "n_events": 120,
            "source_path": str(source_json),
            "gate_oos_consistency_strict": True,
            "gate_bridge_tradable": True,
        },
        {
            "run_id": run_id,
            "event": event,
            "candidate_id": f"{event}_overlay",
            "candidate_type": "overlay",
            "overlay_base_candidate_id": f"{event}_edge",
            "status": "PROMOTED",
            "edge_score": 0.60,
            "expected_return_proxy": 0.02,
            "expectancy_per_trade": 0.02,
            "stability_proxy": 0.9,
            "robustness_score": 0.9,
            "event_frequency": 0.5,
            "capacity_proxy": 1.0,
            "profit_density_score": 0.006,
            "n_events": 80,
            "source_path": str(source_json),
            "gate_oos_consistency_strict": True,
            "gate_bridge_tradable": True,
        },
    ]).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)


def test_overlay_candidates_excluded_from_builder(tmp_path, monkeypatch):
    """Overlay candidates must never appear in strategy_candidates.json."""
    run_id = "overlay_filter_test"
    _write_mixed_edge_inputs(tmp_path, run_id)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys, "argv",
        [
            "build_strategy_candidates.py",
            "--run_id", run_id,
            "--symbols", "BTCUSDT",
            "--allow_non_promoted", "1",
            "--ignore_checklist", "1",
        ],
    )

    build_strategy_candidates.main()

    out = tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json"
    assert out.exists(), "strategy_candidates.json not written"
    candidates = json.loads(out.read_text(encoding="utf-8"))

    candidate_ids = [c.get("candidate_id", c.get("strategy_candidate_id", "")) for c in candidates]
    assert any("_edge" in cid for cid in candidate_ids), "edge candidate should be included"
    assert not any("_overlay" in cid for cid in candidate_ids), (
        f"overlay candidate must not appear in output; found: {candidate_ids}"
    )


def test_overlay_skipped_count_in_diagnostics(tmp_path, monkeypatch):
    """deployment_manifest.json builder_diagnostics must track skipped overlay count."""
    run_id = "overlay_diag_test"
    _write_mixed_edge_inputs(tmp_path, run_id)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys, "argv",
        [
            "build_strategy_candidates.py",
            "--run_id", run_id,
            "--symbols", "BTCUSDT",
            "--allow_non_promoted", "1",
            "--ignore_checklist", "1",
        ],
    )

    build_strategy_candidates.main()

    deploy = tmp_path / "reports" / "strategy_builder" / run_id / "deployment_manifest.json"
    diag = json.loads(deploy.read_text(encoding="utf-8"))["builder_diagnostics"]
    assert diag.get("skipped_overlay_count", -1) >= 1, (
        "builder_diagnostics.skipped_overlay_count must be >= 1"
    )
