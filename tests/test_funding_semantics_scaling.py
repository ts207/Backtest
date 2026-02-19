
def test_funding_scaling():
    interval = 0.008
    bars = 32
    per_bar = interval / bars
    assert abs(per_bar - 0.00025) < 1e-9
