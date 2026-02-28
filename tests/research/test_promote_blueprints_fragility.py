import numpy as np
import pandas as pd
from eval.robustness import simulate_parameter_perturbation
from pipelines.research.promote_blueprints import _fragility_gate


def test_fragility_gate_passes_stable_strategy():
    rng = np.random.default_rng(42)
    # Strong consistent positive PnL — most perturbations stay positive
    pnl = pd.Series(rng.normal(0.001, 0.002, 1000))
    assert _fragility_gate(pnl, min_pass_rate=0.60, n_iterations=200) is True


def test_fragility_gate_fails_fragile_strategy():
    rng = np.random.default_rng(42)
    # Tiny mean relative to std — perturbations flip sign frequently
    pnl = pd.Series(rng.normal(0.0001, 0.05, 500))
    assert _fragility_gate(pnl, min_pass_rate=0.60, n_iterations=200) is False


def test_fragility_gate_empty_series():
    assert _fragility_gate(pd.Series([], dtype=float), min_pass_rate=0.60) is False
