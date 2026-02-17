from __future__ import annotations

import pytest
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from strategy_dsl.schema import (
    Blueprint,
    ConditionNodeSpec,
    EntrySpec,
    EvaluationSpec,
    ExitSpec,
    LineageSpec,
    OverlaySpec,
    SizingSpec,
    SymbolScopeSpec,
)


def _valid_blueprint() -> Blueprint:
    return Blueprint(
        id="bp_test",
        run_id="r1",
        event_type="vol_shock_relaxation",
        candidate_id="all__delay_8",
        symbol_scope=SymbolScopeSpec(mode="single_symbol", symbols=["BTCUSDT"], candidate_symbol="BTCUSDT"),
        direction="conditional",
        entry=EntrySpec(
            triggers=["event_detected"],
            conditions=["all"],
            confirmations=["oos_validation_pass"],
            delay_bars=8,
            cooldown_bars=16,
        ),
        exit=ExitSpec(
            time_stop_bars=24,
            invalidation={"metric": "adverse_proxy", "operator": ">", "value": 0.02},
            stop_type="range_pct",
            stop_value=0.02,
            target_type="range_pct",
            target_value=0.03,
        ),
        sizing=SizingSpec(mode="fixed_risk", risk_per_trade=0.004, target_vol=None, max_gross_leverage=1.0),
        overlays=[OverlaySpec(name="liquidity_guard", params={"min_notional": 0.0})],
        evaluation=EvaluationSpec(
            min_trades=20,
            cost_model={"fees_bps": 3.0, "slippage_bps": 1.0, "funding_included": True},
            robustness_flags={
                "oos_required": True,
                "multiplicity_required": True,
                "regime_stability_required": True,
            },
        ),
        lineage=LineageSpec(
            source_path="data/reports/phase2/r1/vol_shock_relaxation/phase2_candidates.csv",
            compiler_version="strategy_dsl_v1",
            generated_at_utc="1970-01-01T00:00:00Z",
        ),
    )


def test_blueprint_schema_accepts_valid_payload() -> None:
    bp = _valid_blueprint()
    payload = bp.to_dict()
    assert payload["id"] == "bp_test"
    assert payload["entry"]["delay_bars"] == 8


def test_blueprint_schema_rejects_invalid_delay() -> None:
    bp = _valid_blueprint()
    bad = Blueprint(
        id=bp.id,
        run_id=bp.run_id,
        event_type=bp.event_type,
        candidate_id=bp.candidate_id,
        symbol_scope=bp.symbol_scope,
        direction=bp.direction,
        entry=EntrySpec(
            triggers=bp.entry.triggers,
            conditions=bp.entry.conditions,
            confirmations=bp.entry.confirmations,
            delay_bars=-1,
            cooldown_bars=bp.entry.cooldown_bars,
        ),
        exit=bp.exit,
        sizing=bp.sizing,
        overlays=bp.overlays,
        evaluation=bp.evaluation,
        lineage=bp.lineage,
    )
    with pytest.raises(ValueError, match="entry.delay_bars"):
        bad.validate()


def test_blueprint_schema_rejects_empty_id() -> None:
    bp = _valid_blueprint()
    bad = Blueprint(
        id="",
        run_id=bp.run_id,
        event_type=bp.event_type,
        candidate_id=bp.candidate_id,
        symbol_scope=bp.symbol_scope,
        direction=bp.direction,
        entry=bp.entry,
        exit=bp.exit,
        sizing=bp.sizing,
        overlays=bp.overlays,
        evaluation=bp.evaluation,
        lineage=bp.lineage,
    )
    with pytest.raises(ValueError, match="id must be non-empty"):
        bad.validate()


def test_blueprint_schema_validates_condition_nodes() -> None:
    bp = _valid_blueprint()
    enriched = Blueprint(
        id=bp.id,
        run_id=bp.run_id,
        event_type=bp.event_type,
        candidate_id=bp.candidate_id,
        symbol_scope=bp.symbol_scope,
        direction=bp.direction,
        entry=EntrySpec(
            triggers=bp.entry.triggers,
            conditions=[],
            confirmations=bp.entry.confirmations,
            delay_bars=bp.entry.delay_bars,
            cooldown_bars=bp.entry.cooldown_bars,
            condition_logic="any",
            condition_nodes=[
                ConditionNodeSpec(
                    feature="spread_bps",
                    operator=">",
                    value=2.0,
                    lookback_bars=0,
                    window_bars=0,
                ),
            ],
        ),
        exit=bp.exit,
        sizing=bp.sizing,
        overlays=bp.overlays,
        evaluation=bp.evaluation,
        lineage=bp.lineage,
    )
    enriched.validate()


def test_blueprint_schema_rejects_invalid_condition_node() -> None:
    bp = _valid_blueprint()
    bad = Blueprint(
        id=bp.id,
        run_id=bp.run_id,
        event_type=bp.event_type,
        candidate_id=bp.candidate_id,
        symbol_scope=bp.symbol_scope,
        direction=bp.direction,
        entry=EntrySpec(
            triggers=bp.entry.triggers,
            conditions=[],
            confirmations=bp.entry.confirmations,
            delay_bars=bp.entry.delay_bars,
            cooldown_bars=bp.entry.cooldown_bars,
            condition_logic="all",
            condition_nodes=[
                ConditionNodeSpec(
                    feature="spread_bps",
                    operator="zscore_gt",
                    value=1.0,
                    window_bars=1,
                )
            ],
        ),
        exit=bp.exit,
        sizing=bp.sizing,
        overlays=bp.overlays,
        evaluation=bp.evaluation,
        lineage=bp.lineage,
    )
    with pytest.raises(ValueError, match="window_bars"):
        bad.validate()
