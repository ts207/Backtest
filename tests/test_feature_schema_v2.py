import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "project"))

import os
import pytest
import pandas as pd
from pipelines._lib.run_manifest import validate_feature_schema_columns

def test_feature_schema_v2_enforcement():
    # Mock columns matching a typical v1 output (missing v2 specific fields)
    v1_cols = [
        "timestamp", "open", "high", "low", "close", "volume",
        "funding_rate_scaled", "funding_rate", "oi_notional", "oi_delta_1h",
        "liquidation_notional", "liquidation_count", "basis_bps", "basis_zscore",
        "cross_exchange_spread_z", "spread_zscore", "revision_lag_bars",
        "revision_lag_minutes", "logret_1", "rv_96", "rv_pct_17280",
        "high_96", "low_96", "range_96", "range_med_2880",
        "ms_vpin_24", "ms_roll_24", "ms_amihud_24", "ms_kyle_24"
    ]
    
    # 1. Under v1, it should pass for v1 dataset
    os.environ["BACKTEST_FEATURE_SCHEMA_VERSION"] = "v1"
    validate_feature_schema_columns(dataset_key="features_v1_5m_v1", columns=v1_cols)
    
    # 2. Under v2, it should fail for v2 dataset because critical columns are missing
    os.environ["BACKTEST_FEATURE_SCHEMA_VERSION"] = "v2"
    with pytest.raises(ValueError, match="Feature schema contract violated for features_v2_5m_v1; missing columns"):
        validate_feature_schema_columns(dataset_key="features_v2_5m_v1", columns=v1_cols)
        
    # 3. Verify exactly which columns are reported as missing
    try:
        validate_feature_schema_columns(dataset_key="features_v2_5m_v1", columns=v1_cols)
    except ValueError as e:
        error_msg = str(e)
        assert "funding_rate_realized" in error_msg
        assert "is_gap" in error_msg
        assert "quote_volume" in error_msg
        # spread_bps etc are now optional_gated, so not strictly required by validate_feature_schema_columns
        assert "spread_bps" not in error_msg

    # 4. Adding hard-required critical columns should make it pass for v2
    v2_cols = v1_cols + ["funding_rate_realized", "is_gap", "quote_volume"]
    validate_feature_schema_columns(dataset_key="features_v2_5m_v1", columns=v2_cols)

if __name__ == "__main__":
    pytest.main([__file__])
