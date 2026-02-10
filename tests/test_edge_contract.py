import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from portfolio.edge_contract import EdgeContractValidationError, load_approved_edge_contracts, validate_edge_contract


def _approved_edge() -> dict:
    return {
        "edge_id": "vc_core",
        "strategy": "vol_compression_v1",
        "status": "APPROVED",
        "family_id": "vol_compression",
        "relative_rank_within_family": 1,
        "dominated_by": [],
        "signal_fields": ["rv_pct_2880", "range_96"],
        "cost_assumptions": {"fee_bps_per_side": 4.0, "slippage_bps_per_fill": 2.0},
        "validity_window": {"start": "2020-01-01", "end": None},
        "required_evidence": [
            {
                "run_id": "run_a",
                "split": "promotion",
                "date_range": "2021-01-01..2021-12-31",
                "universe": "BTCUSDT,ETHUSDT",
                "config_hash": "cfg-a",
            }
        ],
        "params": {"one_trade_per_day": True},
    }


def test_validate_edge_contract_accepts_approved_contract() -> None:
    parsed = validate_edge_contract(_approved_edge(), require_approved=True)
    assert parsed["edge_id"] == "vc_core"


def test_validate_edge_contract_requires_evidence_for_approved() -> None:
    bad = _approved_edge()
    bad["required_evidence"] = []
    with pytest.raises(EdgeContractValidationError, match="non-empty required_evidence"):
        validate_edge_contract(bad, require_approved=True)


def test_load_approved_edge_contracts_filters_non_approved() -> None:
    draft = _approved_edge()
    draft["edge_id"] = "vc_draft"
    draft["status"] = "DRAFT"
    draft["required_evidence"] = []

    loaded = load_approved_edge_contracts([draft, _approved_edge()])
    assert len(loaded) == 1
    assert loaded[0]["edge_id"] == "vc_core"


def test_validate_edge_contract_requires_comparative_fields_for_approved() -> None:
    bad = _approved_edge()
    bad.pop("family_id")
    with pytest.raises(EdgeContractValidationError, match="comparative fields"):
        validate_edge_contract(bad, require_approved=True)
