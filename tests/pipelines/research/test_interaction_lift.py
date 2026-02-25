import numpy as np
import pandas as pd
import pytest
from pipelines.research.analyze_interaction_lift import calculate_lift

def test_calculate_lift_basic():
    # Synthetic data
    # (Event, Hyp, Symbol, Horizon)
    df = pd.DataFrame([
        {
            "event_type": "EVENT_A",
            "hypothesis_id": "H1",
            "symbol": "BTC",
            "horizon": "5m",
            "condition_signature": "all",
            "expectancy": 0.0010, # 10 bps
            "std_return": 0.01,
            "sample_size": 100
        },
        {
            "event_type": "EVENT_A",
            "hypothesis_id": "H1",
            "symbol": "BTC",
            "horizon": "5m",
            "condition_signature": "state_X",
            "expectancy": 0.0015, # 15 bps
            "std_return": 0.01,
            "sample_size": 50
        }
    ])
    
    lifts = calculate_lift(df, min_trades=30)
    assert len(lifts) == 1
    assert lifts.iloc[0]["expectancy_lift_bps"] == pytest.approx(5.0)
    assert lifts.iloc[0]["condition"] == "state_X"

def test_calculate_lift_significance():
    # Huge difference, should be significant
    df = pd.DataFrame([
        {
            "event_type": "EVENT_A",
            "hypothesis_id": "H1",
            "symbol": "BTC",
            "horizon": "5m",
            "condition_signature": "all",
            "expectancy": 0.0010,
            "std_return": 0.001,
            "sample_size": 1000
        },
        {
            "event_type": "EVENT_A",
            "hypothesis_id": "H1",
            "symbol": "BTC",
            "horizon": "5m",
            "condition_signature": "state_X",
            "expectancy": 0.0050,
            "std_return": 0.001,
            "sample_size": 500
        }
    ])
    
    lifts = calculate_lift(df, min_trades=30)
    assert lifts.iloc[0]["is_significant"] == True
    assert lifts.iloc[0]["p_value"] < 0.01

def test_calculate_lift_insufficient_samples():
    df = pd.DataFrame([
        {
            "event_type": "EVENT_A",
            "hypothesis_id": "H1",
            "symbol": "BTC",
            "horizon": "5m",
            "condition_signature": "all",
            "expectancy": 0.0010,
            "std_return": 0.01,
            "sample_size": 100
        },
        {
            "event_type": "EVENT_A",
            "hypothesis_id": "H1",
            "symbol": "BTC",
            "horizon": "5m",
            "condition_signature": "state_X",
            "expectancy": 0.0015,
            "std_return": 0.01,
            "sample_size": 20 # Below 30
        }
    ])
    
    lifts = calculate_lift(df, min_trades=30)
    assert len(lifts) == 0
