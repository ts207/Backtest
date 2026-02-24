from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.backtest.backtest_strategies import _write_trigger_coverage


def test_write_trigger_coverage_persists_strategy_metadata_diagnostics(tmp_path: Path):
    run_dir = tmp_path / "runs" / "r1"
    metrics = {
        "strategy_metadata": {
            "dsl_interpreter_v1__bp_a": {
                "blueprint_id": "bp_a",
                "trigger_coverage": {
                    "all_zero": True,
                    "missing": ["liquidity_vacuum_event"],
                    "triggers": {
                        "liquidity_vacuum_event": {
                            "resolved": "liquidity_vacuum_event",
                            "true_count": 0,
                            "true_rate": 0.0,
                        }
                    },
                },
            }
        }
    }

    _write_trigger_coverage(run_dir=run_dir, metrics=metrics, enabled=1)
    out_path = run_dir / "engine" / "trigger_coverage.json"
    payload = json.loads(out_path.read_text(encoding="utf-8"))

    assert "dsl_interpreter_v1__bp_a" in payload["by_strategy"]
    assert payload["all_zero_strategies"] == ["dsl_interpreter_v1__bp_a"]
    assert payload["missing_any"] == ["liquidity_vacuum_event"]
