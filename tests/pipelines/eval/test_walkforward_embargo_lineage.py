from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.eval.run_walkforward import _expected_blueprint_strategy_ids


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_expected_blueprints_reject_missing_bridge_embargo_lineage(tmp_path):
    path = tmp_path / "blueprints.jsonl"
    _write_jsonl(
        path,
        [
            {
                "id": "bp_001",
                "event_type": "VOL_SHOCK",
                "symbol_scope": {"mode": "single_symbol", "symbols": ["BTCUSDT"], "candidate_symbol": "BTCUSDT"},
                "lineage": {"promotion_track": "standard"},
            }
        ],
    )

    with pytest.raises(ValueError, match="bridge_embargo_days_used"):
        _expected_blueprint_strategy_ids(
            blueprints_path=path,
            event_type="all",
            top_k=10,
            cli_symbols=["BTCUSDT"],
            embargo_days=1,
        )


def test_expected_blueprints_reject_mismatched_bridge_embargo_lineage(tmp_path):
    path = tmp_path / "blueprints.jsonl"
    _write_jsonl(
        path,
        [
            {
                "id": "bp_001",
                "event_type": "VOL_SHOCK",
                "symbol_scope": {"mode": "single_symbol", "symbols": ["BTCUSDT"], "candidate_symbol": "BTCUSDT"},
                "lineage": {"promotion_track": "standard", "bridge_embargo_days_used": 0},
            }
        ],
    )

    with pytest.raises(ValueError, match="bridge_embargo_days_used"):
        _expected_blueprint_strategy_ids(
            blueprints_path=path,
            event_type="all",
            top_k=10,
            cli_symbols=["BTCUSDT"],
            embargo_days=1,
        )


def test_expected_blueprints_accept_matching_bridge_embargo_lineage(tmp_path):
    path = tmp_path / "blueprints.jsonl"
    _write_jsonl(
        path,
        [
            {
                "id": "bp_001",
                "event_type": "VOL_SHOCK",
                "symbol_scope": {"mode": "single_symbol", "symbols": ["BTCUSDT"], "candidate_symbol": "BTCUSDT"},
                "lineage": {"promotion_track": "standard", "bridge_embargo_days_used": 1},
            }
        ],
    )

    out, _ = _expected_blueprint_strategy_ids(
        blueprints_path=path,
        event_type="all",
        top_k=10,
        cli_symbols=["BTCUSDT"],
        embargo_days=1,
    )
    assert out == ["dsl_interpreter_v1__bp_001"]
