import pandas as pd

def join_candidates_with_features(candidates: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    """
    Join candidates with Point-in-Time (PIT) features.
    
    Args:
        candidates: DataFrame containing 'timestamp' and 'symbol'
        features: DataFrame containing 'timestamp', 'symbol' and feature columns
        
    Returns:
        DataFrame with features joined to candidates
    """
    if "timestamp" not in candidates.columns or "symbol" not in candidates.columns:
        raise ValueError("Candidates DataFrame must contain 'timestamp' and 'symbol'")
        
    if "timestamp" not in features.columns or "symbol" not in features.columns:
        raise ValueError("Features DataFrame must contain 'timestamp' and 'symbol'")
        
    # Standard join on exact matches. 
    # In a real PIT system, this might use pd.merge_asof for better robustness,
    # but exact match is preferred for clean research pipelines where features 
    # are aligned with event timestamps.
    return pd.merge(candidates, features, on=["timestamp", "symbol"], how="left")
