import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.backtest import backtest_strategies as bts


def test_cost_components_from_frame_decomposes_gross_and_costs() -> None:
    frame = pd.DataFrame(
        {
            "pos": [0, 1, 1, 0],
            "ret": [0.0, 0.01, -0.02, 0.03],
        }
    )
    out = bts._cost_components_from_frame(frame, fee_bps=4.0, slippage_bps=2.0)
    assert round(out["gross_alpha"], 10) == round(0.01, 10)
    assert round(out["fees"], 10) == round(2.0 * 4.0 / 10000.0, 10)
    assert round(out["slippage"], 10) == round(2.0 * 2.0 / 10000.0, 10)
    assert out["impact"] == 0.0
    assert round(out["net_alpha"], 10) == round(0.01 - (2.0 * 6.0 / 10000.0), 10)


def test_build_reproducibility_metadata_includes_digest_revision_and_snapshots(monkeypatch, tmp_path: Path) -> None:
    run_id = "repro_run"
    runs_dir = tmp_path / "runs" / run_id
    runs_dir.mkdir(parents=True, exist_ok=True)
    (runs_dir / "build_cleaned_15m.json").write_text(json.dumps({"ok": 1}), encoding="utf-8")
    (runs_dir / "build_features_v1.json").write_text(json.dumps({"ok": 2}), encoding="utf-8")

    def _fake_run(*args, **kwargs):
        return SimpleNamespace(stdout="deadbeef\n")

    monkeypatch.setattr(bts.subprocess, "run", _fake_run)

    meta = bts._build_reproducibility_metadata(
        run_id=run_id,
        config={"fee_bps_per_side": 4},
        params={"symbols": ["BTCUSDT"], "strategies": ["vol_compression_v1"]},
        data_root=tmp_path,
        project_root=ROOT,
    )
    assert meta["code_revision"] == "deadbeef"
    assert len(meta["config_digest"]) == 64
    assert "build_cleaned_15m" in meta["data_snapshot_ids"]
    assert "build_features_v1" in meta["data_snapshot_ids"]
