import pandas as pd
import json
from typing import Dict, Any

def run_state_acceptance(df: pd.DataFrame, state_col: str, target_metric_col: str) -> Dict[str, Any]:
    """
    Verify monotonicity and separation of a state.
    Example: High vol state should have higher realized volatility than low vol state.
    """
    if state_col not in df.columns or target_metric_col not in df.columns:
        return {"ok": False, "error": f"Columns {state_col} or {target_metric_col} missing"}
    
    # Calculate group means
    group_means = df.groupby(state_col)[target_metric_col].mean()
    
    # Basic monotonicity check (placeholder logic)
    # For vol_regime (0, 1, 2), group_means should be increasing
    is_monotonic = group_means.is_monotonic_increasing
    
    # Calculate join rate
    join_rate = 1.0 - df[state_col].isna().mean()
    
    report = {
        "ok": True,
        "state_col": state_col,
        "target_metric_col": target_metric_col,
        "group_means": group_means.to_dict(),
        "is_monotonic": is_monotonic,
        "join_rate": join_rate,
        "passed": is_monotonic and join_rate > 0.99
    }
    
    return report

if __name__ == "__main__":
    # Shell implementation for CLI
    print("State acceptance module ready.")
