from __future__ import annotations

import json
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research.audit_promoted_candidates import AuditConfig, run_audit


def test_audit_promoted_candidates_flags_saturation_duplicates_and_collapse(tmp_path: Path) -> None:
    payload = [
        {
            "run_id": "20260211_031950",
            "event": "range_compression_breakout_window",
            "candidate_id": "range_compression_breakout_window_0",
            "status": "PROMOTED",
            "edge_score": 0.1159101913,
            "expected_return_proxy": 0.0001175412,
            "stability_proxy": 1.0,
            "n_events": 561,
        },
        {
            "run_id": "20260211_031950",
            "event": "range_compression_breakout_window",
            "candidate_id": "range_compression_breakout_window_1",
            "status": "PROMOTED",
            "edge_score": 0.1159101913,
            "expected_return_proxy": 0.0001175412,
            "stability_proxy": 1.0,
            "n_events": 561,
        },
        {
            "run_id": "20260211_031950",
            "event": "vol_shock_relaxation",
            "candidate_id": "vol_shock_relaxation_0",
            "status": "PROMOTED",
            "edge_score": 0.4519680413,
            "expected_return_proxy": -0.0036775852,
            "stability_proxy": 1.0,
            "n_events": 47,
        },
    ]

    in_path = tmp_path / "promoted_candidates.json"
    in_path.write_text(json.dumps(payload), encoding="utf-8")

    out_dir = tmp_path / "audit"
    result = run_audit(in_path, out_dir, AuditConfig())

    assert result["candidate_count"] == 3
    assert result["event_count"] == 2
    assert result["stability_saturation"]["available"] is True
    assert result["stability_saturation"]["saturated_ratio"] == 1.0

    dupes = result["duplicate_edge_groups"]
    assert len(dupes) == 1
    assert dupes[0]["event"] == "range_compression_breakout_window"
    assert dupes[0]["candidate_count"] == 2

    collapse = result["mechanism_collapse"]
    assert len(collapse) == 1
    assert collapse[0]["event"] == "range_compression_breakout_window"
    assert collapse[0]["count"] == 2

    collapsed = json.loads((out_dir / "collapsed_candidates.json").read_text(encoding="utf-8"))
    assert len(collapsed) == 2


def test_audit_promoted_candidates_computes_cofiring_when_timestamps_present(tmp_path: Path) -> None:
    payload = [
        {
            "event": "vol_shock_relaxation",
            "candidate_id": "vol_shock_relaxation_0",
            "edge_score": 0.4,
            "expected_return_proxy": -0.003,
            "stability_proxy": 1.0,
            "n_events": 10,
            "enter_ts": "2024-01-01T00:00:00Z",
        },
        {
            "event": "vol_shock_relaxation",
            "candidate_id": "vol_shock_relaxation_0",
            "edge_score": 0.4,
            "expected_return_proxy": -0.004,
            "stability_proxy": 1.0,
            "n_events": 10,
            "enter_ts": "2024-01-01T01:00:00Z",
        },
        {
            "event": "liquidity_refill_lag_window",
            "candidate_id": "liquidity_refill_lag_window_0",
            "edge_score": 0.2,
            "expected_return_proxy": -0.002,
            "stability_proxy": 1.0,
            "n_events": 10,
            "enter_ts": "2024-01-01T00:05:00Z",
        },
        {
            "event": "liquidity_refill_lag_window",
            "candidate_id": "liquidity_refill_lag_window_0",
            "edge_score": 0.2,
            "expected_return_proxy": -0.001,
            "stability_proxy": 1.0,
            "n_events": 10,
            "enter_ts": "2024-01-01T02:00:00Z",
        },
    ]

    in_path = tmp_path / "promoted_candidates_with_ts.json"
    in_path.write_text(json.dumps(payload), encoding="utf-8")

    out_dir = tmp_path / "audit_ts"
    result = run_audit(in_path, out_dir, AuditConfig(cofire_window_minutes=15))

    indep = result["independence"]
    assert indep["available"] is True
    assert len(indep["pairwise_overlap"]) == 1
    pair = indep["pairwise_overlap"][0]
    assert pair["event_a"] == "liquidity_refill_lag_window"
    assert pair["event_b"] == "vol_shock_relaxation"
    assert 0.0 <= pair["cofire_rate_15m"] <= 1.0
