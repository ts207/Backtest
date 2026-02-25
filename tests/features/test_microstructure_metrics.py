import numpy as np
import pandas as pd
import pytest
from features.microstructure import calculate_vpin, calculate_roll

def test_calculate_vpin_basic():
    # Synthetic volume and buy volume
    # If buy == sell, VPIN should be 0 (if we only have one bucket)
    # Actually VPIN is usually calculated over a window of volume buckets.
    # In a bar context, we might treat each bar as a bucket or part of a bucket.
    
    vol = pd.Series([100, 100, 100, 100])
    buy_vol = pd.Series([50, 50, 50, 50])
    # |50 - 50| = 0. VPIN = 0 / 400 = 0.
    vpin = calculate_vpin(vol, buy_vol, window=4)
    assert vpin.iloc[-1] == 0.0

def test_calculate_vpin_maximum_toxicity():
    vol = pd.Series([100, 100, 100, 100])
    buy_vol = pd.Series([100, 100, 100, 100])
    # |100 - 0| = 100. VPIN = 400 / 400 = 1.0
    vpin = calculate_vpin(vol, buy_vol, window=4)
    assert vpin.iloc[-1] == 1.0

def test_calculate_roll_basic():
    # Roll's measure: 2 * sqrt(-cov(dp_t, dp_{t-1}))
    # If price alternates: 10, 11, 10, 11
    # dp: 1, -1, 1, -1
    
    prices = pd.Series([10.0, 11.0, 10.0, 11.0, 10.0, 11.0, 10.0, 11.0, 10.0, 11.0])
    roll = calculate_roll(prices, window=5)
    assert roll.iloc[-1] > 0

def test_calculate_roll_zero_for_trending():
    # If price is trending: 10, 11, 12, 13, 14...
    prices = pd.Series([float(10 + i) for i in range(20)])
    roll = calculate_roll(prices, window=5)
    assert roll.iloc[-1] == 0.0
