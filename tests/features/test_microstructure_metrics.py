import numpy as np
import pandas as pd
import pytest
from features.microstructure import calculate_vpin_score as calculate_vpin, calculate_roll, calculate_amihud_illiquidity as calculate_amihud, calculate_kyle_lambda

def test_calculate_amihud_basic():
    # Amihud = |Ret| / (Close * Vol)
    prices = pd.Series([100.0, 101.0])
    vol = pd.Series([0.0, 1000.0])
    # Ret = log(101/100) approx 0.01
    # Amihud approx 0.01 / (101 * 1000)
    amihud = calculate_amihud(prices, vol, window=1)
    assert amihud.iloc[-1] > 0
    assert not np.isnan(amihud.iloc[-1])

def test_calculate_kyle_lambda_basic():
    # price change = lambda * net_order_flow + error
    # If dp = 1.0 when flow = 100.0
    # lambda should be approx 0.01
    ts = pd.date_range("2024-09-01", periods=100, freq="5min")
    # Make net flow vary
    x = np.random.normal(100, 10, 100)
    # y = 0.01 * x
    y = 0.01 * x
    
    close = pd.Series(np.cumsum(y) + 1000.0, index=ts)
    buy_vol = pd.Series(x + 500.0, index=ts)
    sell_vol = pd.Series([500.0] * 100, index=ts)
    
    kyle = calculate_kyle_lambda(close, buy_vol, sell_vol, window=24)
    assert kyle.iloc[-1] > 0
    # Since it's a perfect regression, it should be very close to 0.01
    assert kyle.iloc[-1] == pytest.approx(0.01, rel=1e-2)

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
