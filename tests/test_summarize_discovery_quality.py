import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def test_summarize_discovery_quality_writes_expected_schema(tmp_path: Path) -> None:
    run_id = "qsum_case"
    phase2_root = tmp_path / "reports" / "phase2" / run_id

    vol_dir = phase2_root / "vol_shock_relaxation"
    vol_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "candidate_id": ["v1", "v2", "v3"],
            "gate_pass": [1, 0, 1],
            "fail_reasons": ["", "gate_c_regime,gate_ess", ""],
        }
    ).to_csv(vol_dir / "phase2_candidates.csv", index=False)

    liq_dir = phase2_root / "liquidity_vacuum"
    liq_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "candidate_id": ["l1", "l2"],
            "gate_pass": [0, 0],
            "fail_reasons": ["gate_c_regime", "gate_after_cost_positive,gate_c_regime"],
        }
    ).to_csv(liq_dir / "phase2_candidates.csv", index=False)

    env = os.environ.copy()
    env["BACKTEST_DATA_ROOT"] = str(tmp_path)
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "project" / "pipelines" / "research" / "summarize_discovery_quality.py"),
            "--run_id",
            run_id,
        ],
        check=True,
        env=env,
    )

    out_path = phase2_root / "discovery_quality_summary.json"
    assert out_path.exists()

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["run_id"] == run_id
    assert payload["total_candidates"] == 5
    assert payload["gate_pass_count"] == 2
    assert payload["gate_pass_rate"] == 0.4
    assert "top_fail_reasons" in payload
    assert payload["top_fail_reasons"][0]["reason"] == "gate_c_regime"
    assert payload["top_fail_reasons"][0]["count"] == 3
    assert "by_event_family" in payload
    assert payload["by_event_family"]["vol_shock_relaxation"]["total_candidates"] == 3
    assert payload["by_event_family"]["liquidity_vacuum"]["total_candidates"] == 2

