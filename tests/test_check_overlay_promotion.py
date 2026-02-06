import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research.check_overlay_promotion import _check_single_run


def test_check_single_run_passes_when_constraints_and_stability_met() -> None:
    summary = {
        "max_drawdown": -0.2,
        "fee_sensitivity": [{"fee_bps_per_side": 6.0, "net_return": 0.15}],
        "overlay_bindings": {
            "vol_compression_v1": {"applied_overlays": ["vol_shock_relaxation_delay30_v1"]}
        },
        "risk_metrics": {
            "cvar_99_1d": 0.04,
            "bps_per_day": 10.0,
            "trades_per_day": 5.0,
            "max_gross": 0.9,
            "max_per_symbol": 0.7,
            "max_time_in_market": 0.8,
        },
        "stability_checks": {
            "sign_consistency": 0.8,
            "effect_ci_excludes_0": True,
            "regime_flip_count": 0,
        },
    }
    constraints = {
        "max_drawdown_pct": 30.0,
        "tail_loss": {"metric": "cvar_99_1d", "max_abs_return": 0.05},
        "turnover_budget": {"max_bps_per_day": 25.0, "max_trades_per_day": 20},
        "exposure_limits": {"max_gross": 1.0, "max_per_symbol": 1.0, "max_time_in_market": 1.0},
    }
    stability = {"sign_consistency_min": 0.67, "effect_ci_excludes_0": True, "max_regime_flip_count": 0}
    reasons = _check_single_run(
        summary=summary,
        overlay_name="vol_shock_relaxation_delay30_v1",
        objective_metric="net_total_return",
        cost_bps=6.0,
        constraints=constraints,
        stability=stability,
    )
    assert reasons == []


def test_check_single_run_returns_reasons_on_contract_violations() -> None:
    summary = {
        "max_drawdown": -0.5,
        "fee_sensitivity": [{"fee_bps_per_side": 2.0, "net_return": 0.15}],
        "overlay_bindings": {},
        "risk_metrics": {},
        "stability_checks": {"sign_consistency": 0.4, "effect_ci_excludes_0": False, "regime_flip_count": 3},
    }
    constraints = {
        "max_drawdown_pct": 30.0,
        "tail_loss": {"metric": "cvar_99_1d", "max_abs_return": 0.05},
        "turnover_budget": {"max_bps_per_day": 25.0, "max_trades_per_day": 20},
        "exposure_limits": {"max_gross": 1.0, "max_per_symbol": 1.0, "max_time_in_market": 1.0},
    }
    stability = {"sign_consistency_min": 0.67, "effect_ci_excludes_0": True, "max_regime_flip_count": 0}
    reasons = _check_single_run(
        summary=summary,
        overlay_name="vol_shock_relaxation_delay30_v1",
        objective_metric="net_total_return",
        cost_bps=6.0,
        constraints=constraints,
        stability=stability,
    )
    assert any("overlay not found" in r for r in reasons)
    assert any("max_drawdown_pct violated" in r for r in reasons)
    assert any("sign_consistency below threshold" in r for r in reasons)
