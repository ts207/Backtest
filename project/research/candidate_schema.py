import pandas as pd
from typing import List

CANONICAL_CANDIDATE_COLUMNS = [
    "candidate_id",
    "family_id",
    "run_id",
    "symbol",
    "event_type",
    "template_verb",
    "horizon",
    "state_id",
    "condition_label",
    "effect_raw",
    "effect_shrunk_state",
    "p_value",
    "q_value",
    "is_discovery",
    "n_events",
    "selection_score",
    "robustness_score",
    "effective_sample_size",
    "gate_phase2_final",
    "fail_reasons",
    "fail_gate_primary",
    "fail_reason_primary",
    "effective_lag_bars",
    "gate_bridge_tradable",
    "bridge_fail_gate_primary",
    "bridge_fail_reason_primary",
    "promotion_fail_gate_primary",
    "promotion_fail_reason_primary",
    "gate_promo_statistical",
    "gate_promo_stability",
    "gate_promo_cost_survival",
    "gate_promo_negative_control",
    "gate_promo_hypothesis_audit",
]

def ensure_candidate_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure DataFrame matches the canonical candidate schema.
    Missing columns are filled with sensible defaults.
    """
    out = df.copy()
    for col in CANONICAL_CANDIDATE_COLUMNS:
        if col not in out.columns:
            if col.startswith("gate_"):
                out[col] = False
            elif "effect" in col or "p_value" in col or "q_value" in col:
                out[col] = 1.0 if "p_value" in col or "q_value" in col else 0.0
            elif "n_events" in col or "effective" in col:
                out[col] = 0
            elif col == "is_discovery":
                out[col] = False
            else:
                out[col] = ""
    
    return out[CANONICAL_CANDIDATE_COLUMNS + [c for c in out.columns if c not in CANONICAL_CANDIDATE_COLUMNS]]
