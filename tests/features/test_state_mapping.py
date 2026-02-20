import pandas as pd
import pytest
from project.features.state_mapping import map_vol_regime, map_carry_state

def test_map_vol_regime():
    rv = pd.Series([0.1, 0.5, 0.9])
    states = map_vol_regime(rv)
    assert list(states) == [0.0, 1.0, 2.0]

def test_map_carry_state():
    fr = pd.Series([-0.2, 0.0, 0.2])
    states = map_carry_state(fr)
    assert list(states) == [-1.0, 0.0, 1.0]
