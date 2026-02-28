from engine.exchange_constraints import SymbolConstraints, apply_constraints

def test_lot_rounding():
    c = SymbolConstraints(tick_size=0.01, step_size=0.001, min_notional=10.0)
    # Raw qty 1.2345 should round DOWN to nearest step_size
    assert c.round_qty(1.2345) == 1.234

def test_min_notional_zero_out():
    c = SymbolConstraints(tick_size=0.01, step_size=0.001, min_notional=100.0)
    # Notional = qty * price = 0.001 * 50.0 = 0.05 < 100 → qty becomes 0
    assert c.enforce_min_notional(qty=0.001, price=50.0) == 0.0

def test_apply_constraints_clips_trade():
    c = SymbolConstraints(tick_size=0.01, step_size=0.1, min_notional=5.0)
    result = apply_constraints(requested_qty=0.05, price=50.0, constraints=c)
    assert result == 0.0  # 0.05 * 50 = 2.5 < 5.0 → zeroed

def test_no_constraints_passthrough():
    c = SymbolConstraints(tick_size=None, step_size=None, min_notional=None)
    assert apply_constraints(requested_qty=1.2345, price=100.0, constraints=c) == 1.2345

def test_negative_qty_sign_preserved():
    import pytest
    c = SymbolConstraints(tick_size=0.01, step_size=0.001, min_notional=10.0)
    result = apply_constraints(requested_qty=-1.2345, price=100.0, constraints=c)
    assert result == pytest.approx(-1.234)

def test_negative_qty_min_notional_zero():
    c = SymbolConstraints(tick_size=0.01, step_size=0.001, min_notional=100.0)
    result = apply_constraints(requested_qty=-0.001, price=50.0, constraints=c)
    assert result == 0.0
