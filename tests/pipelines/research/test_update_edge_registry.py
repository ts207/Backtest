from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.research.update_edge_registry as update_edge_registry


def _write_run_manifest(data_root: Path, run_id: str) -> None:
    run_dir = data_root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    payload = {"run_id": run_id, "ontology_spec_hash": "sha256:test"}
    (run_dir / "run_manifest.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_promotions(data_root: Path, run_id: str, effect: float) -> None:
    promo_dir = data_root / "reports" / "promotions" / run_id
    promo_dir.mkdir(parents=True, exist_ok=True)
    promoted = pd.DataFrame(
        [
            {
                "candidate_id": f"cand_{run_id}",
                "status": "PROMOTED",
                "event_type": "VOL_SHOCK",
                "template_id": "mean_reversion",
                "direction_rule": "contrarian",
                "signal_polarity_logic": "shock_up_short_shock_down_long",
                "promotion_score": 0.9,
                "promotion_decision": "promoted",
                "effect_shrunk_state": effect,
                "stability_score": 0.4,
            }
        ]
    )
    audit = pd.DataFrame(
        [
            {
                "candidate_id": f"cand_{run_id}",
                "event_type": "VOL_SHOCK",
                "template_id": "mean_reversion",
                "direction_rule": "contrarian",
                "signal_polarity_logic": "shock_up_short_shock_down_long",
                "promotion_score": 0.9,
                "promotion_decision": "promoted",
                "effect_shrunk_state": effect,
                "stability_score": 0.4,
            }
        ]
    )
    promoted.to_parquet(promo_dir / "promoted_candidates.parquet", index=False)
    audit.to_parquet(promo_dir / "promotion_audit.parquet", index=False)


def test_update_edge_registry_appends_and_aggregates(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    monkeypatch.setattr(update_edge_registry, "DATA_ROOT", data_root)

    for run_id, effect in (("r1", 0.02), ("r2", 0.01)):
        _write_run_manifest(data_root, run_id)
        _write_promotions(data_root, run_id, effect)
        rc = update_edge_registry.main(
            [
                "--run_id",
                run_id,
            ]
        )
        assert rc == 0

    latest_snapshot = data_root / "runs" / "r2" / "research" / "edge_registry.parquet"
    assert latest_snapshot.exists()
    snapshot_df = pd.read_parquet(latest_snapshot)
    assert len(snapshot_df) == 1
    row = snapshot_df.iloc[0]
    assert int(row["times_tested"]) == 2
    assert int(row["times_promoted"]) == 2
    assert row["first_seen_run"] == "r1"
    assert row["last_seen_run"] == "r2"
    assert abs(float(row["median_effect"]) - 0.015) < 1e-9
