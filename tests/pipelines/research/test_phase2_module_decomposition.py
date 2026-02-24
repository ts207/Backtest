from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research import phase2_conditional_hypotheses as legacy
from pipelines.research import phase2_cost_integration as cost
from pipelines.research import phase2_event_analyzer as event_analyzer
from pipelines.research import phase2_statistical_gates as gates


def test_phase2_event_analyzer_wiring():
    assert legacy.ConditionSpec is event_analyzer.ConditionSpec
    assert legacy.ActionSpec is event_analyzer.ActionSpec
    assert legacy._build_conditions is event_analyzer.build_conditions
    assert legacy._build_actions is event_analyzer.build_actions
    assert legacy._attach_forward_opportunity is event_analyzer.attach_forward_opportunity


def test_phase2_cost_wiring():
    assert legacy._turnover_proxy_for_action is cost.turnover_proxy_for_action
    assert legacy._candidate_cost_fields is cost.candidate_cost_fields


def test_phase2_statistical_wiring():
    assert legacy._apply_multiplicity_adjustments is gates.apply_multiplicity_adjustments
    assert legacy._curvature_metrics is gates.curvature_metrics
    assert legacy._split_t_stat_and_p_value is gates.split_t_stat_and_p_value
