import pandas as pd
from pathlib import Path

def generate_attribution_report(metrics_df: pd.DataFrame, output_path: str) -> None:
    """
    Export the performance attribution metrics to a Parquet file.
    
    Args:
        metrics_df: DataFrame containing the calculated metrics per regime
        output_path: Target file path for the Parquet report
        
    Raises:
        ValueError: If metrics_df is empty or missing expected columns
    """
    if metrics_df.empty:
        raise ValueError("Cannot generate report from empty DataFrame")
        
    # We expect at least net_pnl_bps or sharpe_ratio to be present
    if not any(col in metrics_df.columns for col in ["net_pnl_bps", "sharpe_ratio", "total_pnl"]):
        raise ValueError("DataFrame missing key performance metrics")
        
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    metrics_df.to_parquet(output_file, index=True)
    print(f"Attribution report exported to {output_path}")
