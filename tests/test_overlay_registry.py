import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from strategies.overlay_registry import (
    PINNED_SPEC_VERSION,
    _load_overlay_specs,
    _validate_spec,
    apply_overlay,
    get_overlay,
    list_applicable_overlays,
    list_overlays,
)


def _approved_evidence() -> list[dict[str, str]]:
    return [
        {
            "run_id": "run_a",
            "split": "test",
            "date_range": "2021-01-01..2021-12-31",
            "universe": "BTCUSDT,ETHUSDT",
            "config_hash": "cfg-a",
        }
    ]


def test_delay30_overlay_registered() -> None:
    assert "vol_shock_relaxation_delay30_v1" in list_overlays()
    overlay = get_overlay("vol_shock_relaxation_delay30_v1")
    assert overlay["event"] == "vol_shock_relaxation"
    assert overlay["action"] == "delay_30"
    assert overlay["spec_version"] == PINNED_SPEC_VERSION


def test_apply_overlay_augments_params() -> None:
    base = {"trade_day_timezone": "UTC"}
    params = apply_overlay("vol_shock_relaxation_delay30_v1", base)
    assert params["trade_day_timezone"] == "UTC"
    assert params["overlays"] == ["vol_shock_relaxation_delay30_v1"]
    assert params["overlay_specs"]["vol_shock_relaxation_delay30_v1"]["cost_bps_validated"] == 6.0


def test_unknown_overlay_raises() -> None:
    with pytest.raises(ValueError, match="Unknown overlay"):
        get_overlay("does_not_exist")


def test_validate_spec_requires_fields_and_version() -> None:
    with pytest.raises(ValueError, match="missing required fields"):
        _validate_spec({"name": "x"}, Path("x.json"))

    bad = {
        "spec_version": "v2",
        "name": "x",
        "event": "e",
        "condition": "c",
        "action": "a",
        "status": "APPROVED",
        "tuning_allowed": False,
        "run_ids_evidence": _approved_evidence(),
        "cost_bps_validated": 6.0,
        "objective": {"target_metric": "net_total_return"},
        "constraints": {
            "max_drawdown_pct": 25.0,
            "tail_loss": {},
            "exposure_limits": {},
            "turnover_budget": {},
        },
        "stability": {
            "sign_consistency_min": 0.67,
            "effect_ci_excludes_0": True,
            "max_regime_flip_count": 0,
        },
        "notes": "n",
    }
    with pytest.raises(ValueError, match="version mismatch"):
        _validate_spec(bad, Path("x.json"))


def test_loader_fails_when_no_specs_present(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="No overlay specs found"):
        _load_overlay_specs(tmp_path)


def test_loader_reads_all_schema_valid_specs(tmp_path: Path) -> None:
    approved = {
        "spec_version": "v1",
        "name": "vol_shock_relaxation_delay30_v1",
        "event": "vol_shock_relaxation",
        "condition": "c",
        "action": "delay_30",
        "status": "APPROVED",
        "tuning_allowed": False,
        "run_ids_evidence": _approved_evidence(),
        "cost_bps_validated": 6.0,
        "objective": {"target_metric": "net_total_return"},
        "constraints": {
            "max_drawdown_pct": 25.0,
            "tail_loss": {},
            "exposure_limits": {},
            "turnover_budget": {},
        },
        "stability": {
            "sign_consistency_min": 0.67,
            "effect_ci_excludes_0": True,
            "max_regime_flip_count": 0,
        },
        "notes": "n",
    }
    draft = {
        "spec_version": "v1",
        "name": "vol_aftershock_window_rerisk_placeholder_v1",
        "event": "vol_aftershock_window",
        "condition": "true",
        "action": "PLACEHOLDER_NOOP",
        "status": "DRAFT",
        "tuning_allowed": False,
        "run_ids_evidence": [],
        "cost_bps_validated": None,
        "objective": {"target_metric": "net_total_return"},
        "constraints": {
            "max_drawdown_pct": None,
            "tail_loss": None,
            "exposure_limits": None,
            "turnover_budget": None,
        },
        "stability": {
            "sign_consistency_min": None,
            "effect_ci_excludes_0": None,
            "max_regime_flip_count": None,
        },
        "notes": "placeholder",
    }
    (tmp_path / "a.json").write_text(json.dumps(approved), encoding="utf-8")
    (tmp_path / "b.json").write_text(json.dumps(draft), encoding="utf-8")
    loaded = _load_overlay_specs(tmp_path)
    assert "vol_shock_relaxation_delay30_v1" in loaded
    assert "vol_aftershock_window_rerisk_placeholder_v1" in loaded


def test_draft_overlay_is_listed_but_not_applicable() -> None:
    assert "vol_aftershock_window_rerisk_placeholder_v1" in list_overlays()
    assert "vol_aftershock_window_rerisk_placeholder_v1" not in list_applicable_overlays()
    with pytest.raises(ValueError, match="not applicable"):
        apply_overlay("vol_aftershock_window_rerisk_placeholder_v1", params={})


def test_liquidity_refill_draft_overlay_listed_but_not_applicable() -> None:
    assert "liquidity_refill_lag_window_placeholder_v1" in list_overlays()
    assert "liquidity_refill_lag_window_placeholder_v1" not in list_applicable_overlays()
    with pytest.raises(ValueError, match="not applicable"):
        apply_overlay("liquidity_refill_lag_window_placeholder_v1", params={})


def test_approved_overlay_requires_structured_evidence_entries(tmp_path: Path) -> None:
    invalid_approved = {
        "spec_version": "v1",
        "name": "bad_approved",
        "event": "x",
        "condition": "true",
        "action": "a",
        "status": "APPROVED",
        "tuning_allowed": False,
        "run_ids_evidence": ["run_a"],
        "cost_bps_validated": 5.0,
        "objective": {"target_metric": "net_total_return"},
        "constraints": {
            "max_drawdown_pct": 10.0,
            "tail_loss": {},
            "exposure_limits": {},
            "turnover_budget": {},
        },
        "stability": {
            "sign_consistency_min": 0.67,
            "effect_ci_excludes_0": True,
            "max_regime_flip_count": 0,
        },
        "notes": "n",
    }
    (tmp_path / "bad.json").write_text(json.dumps(invalid_approved), encoding="utf-8")
    with pytest.raises(ValueError, match="must be an object"):
        _load_overlay_specs(tmp_path)


def test_approved_overlay_requires_falsifiable_stability_thresholds(tmp_path: Path) -> None:
    invalid_approved = {
        "spec_version": "v1",
        "name": "bad_approved",
        "event": "x",
        "condition": "true",
        "action": "a",
        "status": "APPROVED",
        "tuning_allowed": False,
        "run_ids_evidence": _approved_evidence(),
        "cost_bps_validated": 5.0,
        "objective": {"target_metric": "net_total_return"},
        "constraints": {
            "max_drawdown_pct": 10.0,
            "tail_loss": {},
            "exposure_limits": {},
            "turnover_budget": {},
        },
        "stability": {
            "time_split": "pass",
            "regime_split": "pass",
        },
        "notes": "n",
    }
    (tmp_path / "bad.json").write_text(json.dumps(invalid_approved), encoding="utf-8")
    with pytest.raises(ValueError, match="stability.sign_consistency_min"):
        _load_overlay_specs(tmp_path)
