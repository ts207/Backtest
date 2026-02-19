import numpy as np
import pandas as pd
from typing import List, Dict, Any, Tuple

def benjamini_hochberg(p_values: List[float], alpha: float = 0.05) -> Tuple[List[bool], List[float]]:
    """
    Apply Benjamini-Hochberg procedure for FDR control.
    Returns (reject_null, q_values)
    """
    p_values = np.array(p_values)
    n = len(p_values)
    
    # Sort p-values
    sorted_indices = np.argsort(p_values)
    sorted_p = p_values[sorted_indices]
    
    # Calculate q-values
    # q[i] = p[i] * n / (i + 1)
    q_values = sorted_p * n / (np.arange(n) + 1)
    
    # Enforce monotonicity: q[i] = min(q[i], q[i+1])
    q_values = np.minimum.accumulate(q_values[::-1])[::-1]
    
    # Reject where q <= alpha
    reject = q_values <= alpha
    
    # Restore original order
    original_order_reject = np.zeros(n, dtype=bool)
    original_order_reject[sorted_indices] = reject
    
    original_order_q = np.zeros(n, dtype=float)
    original_order_q[sorted_indices] = q_values
    
    return original_order_reject.tolist(), original_order_q.tolist()

def apply_multiplicity_control(candidates: pd.DataFrame, alpha: float = 0.05) -> pd.DataFrame:
    """
    Apply BH-FDR to a dataframe of candidates with 'p_value' column.
    """
    if "p_value" not in candidates.columns:
        raise ValueError("DataFrame must contain 'p_value' column")
        
    pvals = candidates["p_value"].fillna(1.0).tolist()
    rejected, qvals = benjamini_hochberg(pvals, alpha)
    
    candidates["pass_fdr"] = rejected
    candidates["q_value_bh"] = qvals
    
    return candidates

def report_discoveries(candidates: pd.DataFrame, alpha: float = 0.05) -> Dict[str, Any]:
    """
    Generate summary stats for discoveries.
    """
    total = len(candidates)
    passed = candidates["pass_fdr"].sum() if "pass_fdr" in candidates.columns else 0
    
    return {
        "total_hypotheses": int(total),
        "fdr_target": alpha,
        "discoveries": int(passed),
        "discovery_rate": float(passed / total) if total > 0 else 0.0
    }
