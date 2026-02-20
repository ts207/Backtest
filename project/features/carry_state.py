import pandas as pd

def calculate_funding_rate_bps(funding_rate_series: pd.Series) -> pd.Series:
    return funding_rate_series * 10000
