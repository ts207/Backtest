import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines._lib.validation import validate_strategy_family_params
from pipelines.backtest import backtest_strategies as bts


def test_validate_strategy_family_params_rejects_invalid_carry_percentiles() -> None:
    config = {
        "strategy_family_params": {
            "Carry": {
                "funding_percentile_entry_min": 99.0,
                "funding_percentile_entry_max": 98.0,
            }
        }
    }
    with pytest.raises(ValueError, match="funding_percentile_entry_min"):
        validate_strategy_family_params(config)


def test_build_strategy_params_by_name_keeps_only_relevant_family_keys() -> None:
    config = {
        "strategy_family_params": {
            "Carry": {
                "funding_percentile_entry_min": 95.0,
                "funding_percentile_entry_max": 99.0,
                "normalization_exit_percentile": 60.0,
                "normalization_exit_consecutive_bars": 2,
                "sizing_curve": "linear",
            },
            "Spread": {
                "spread_zscore_entry_abs": 2.1,
                "dislocation_threshold_bps": 10.0,
                "convergence_target_zscore": 0.3,
                "max_hold_bars": 12,
            },
        },
        "strategy_families": {
            "funding_extreme_reversal_v1": "Carry",
            "cross_venue_desync_v1": "Spread",
        },
    }

    _, params_by_name = bts._build_strategy_params_by_name(
        strategies=["funding_extreme_reversal_v1", "cross_venue_desync_v1"],
        config=config,
        trade_day_timezone="UTC",
        overlays=[],
    )

    carry_params = params_by_name["funding_extreme_reversal_v1"]
    spread_params = params_by_name["cross_venue_desync_v1"]

    assert "funding_percentile_entry_min" in carry_params
    assert "spread_zscore_entry_abs" not in carry_params
    assert "spread_zscore_entry_abs" in spread_params
    assert "funding_percentile_entry_min" not in spread_params
