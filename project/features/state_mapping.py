import pandas as pd
import numpy as np

def map_vol_regime(rv_pct: pd.Series) -> pd.Series:
    bins = [0.0, 0.33, 0.66, 1.0]
    labels = [0.0, 1.0, 2.0] # low, mid, high
    return pd.cut(rv_pct, bins=bins, labels=labels, include_lowest=True).astype(float)

def map_carry_state(fr_bps: pd.Series) -> pd.Series:
    # funding_neg: -1, neutral: 0, funding_pos: 1
    state = pd.Series(0.0, index=fr_bps.index)
    state[fr_bps < -0.1] = -1.0
    state[fr_bps > 0.1] = 1.0
    return state.astype(float)
