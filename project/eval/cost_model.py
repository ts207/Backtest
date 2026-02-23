import pandas as pd
import yaml
from pathlib import Path

def apply_cost_model(candidates: pd.DataFrame, config_path: str, pnl_col: str = "pnl_bps") -> pd.DataFrame:
    """
    Apply fees and slippage to a DataFrame of trading candidates.
    
    Round-trip cost = (fee_bps_per_side + slippage_bps_per_fill) * 2
    Net PnL = Gross PnL - Round-trip cost
    
    Args:
        candidates: DataFrame with a P&L column (in bps)
        config_path: Path to YAML config containing 'fee_bps_per_side' 
                    and 'slippage_bps_per_fill'
        pnl_col: Name of the column containing gross P&L in bps
        
    Returns:
        DataFrame with an added 'net_pnl_bps' column
    """
    if pnl_col not in candidates.columns:
        raise ValueError(f"Column '{pnl_col}' not found in DataFrame")
        
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Cost config not found at: {config_path}")
        
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
        
    fee = float(config.get("fee_bps_per_side", 0.0))
    slippage = float(config.get("slippage_bps_per_fill", 0.0))
    
    # 2 * (fee + slippage) for round trip
    round_trip_cost = (fee + slippage) * 2.0
    
    candidates["net_pnl_bps"] = candidates[pnl_col] - round_trip_cost
    candidates["round_trip_cost_bps"] = round_trip_cost
    
    return candidates
