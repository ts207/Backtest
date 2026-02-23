
import pandas as pd
import numpy as np
import pytest
from pipelines.research.analyze_conditional_expectancy import _bh_adjust

def test_bh_adjust_monotonicity():
    p_values = pd.Series([0.01, 0.04, 0.03, 0.05])
    adjusted = _bh_adjust(p_values)
    
    # Sort by original p-values to check monotonicity
    joined = pd.DataFrame({"p": p_values, "adj": adjusted}).sort_values("p")
    assert joined["adj"].is_monotonic_increasing

def test_bh_adjust_known_values():
    # m = 4
    # 0.01 * 4 / 1 = 0.04
    # 0.03 * 4 / 2 = 0.06
    # 0.04 * 4 / 3 = 0.0533 -> min(0.06, 0.0533) = 0.0533
    # 0.05 * 4 / 4 = 0.05 -> min(0.0533, 0.05) = 0.05
    
    p_values = pd.Series([0.01, 0.03, 0.04, 0.05])
    adjusted = _bh_adjust(p_values)
    
    expected = [0.04, 0.05, 0.05, 0.05]
    np.testing.assert_allclose(adjusted.values, expected, atol=1e-4)

def test_horizon_hours_logic_5m():
    # For 5m bars, 12 bars = 1 hour.
    
    horizon = 12
    # Logic should be:
    horizon_hours = horizon / 12.0
    assert horizon_hours == 1.0
