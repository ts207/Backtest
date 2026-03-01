"""
Thin shim preserving the original import path for ``phase2_conditional_hypotheses``.

The original 86 KB legacy file has been moved to ``archive/`` since it is not
part of the orchestrated pipeline.  All symbols the decomposition test verifies
are re-exported here directly from their canonical modules.
"""
from __future__ import annotations

# Re-export the symbols that test_phase2_module_decomposition.py verifies.
from pipelines.research.phase2_event_analyzer import (
    ConditionSpec,
    ActionSpec,
    build_conditions as _build_conditions,
    build_actions as _build_actions,
    attach_forward_opportunity as _attach_forward_opportunity,
)
from pipelines.research.phase2_cost_integration import (
    turnover_proxy_for_action as _turnover_proxy_for_action,
    candidate_cost_fields as _candidate_cost_fields,
)
from pipelines.research.phase2_statistical_gates import (
    apply_multiplicity_adjustments as _apply_multiplicity_adjustments,
    curvature_metrics as _curvature_metrics,
    split_t_stat_and_p_value as _split_t_stat_and_p_value,
)
