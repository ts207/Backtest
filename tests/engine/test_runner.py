"""
Unit tests for engine/runner.py

Covers:
- _load_symbol_data: merging, start/end slicing, and explicit ffill limits
- _strategy_returns: lag gating, position generation bounds
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from engine.runner import _load_symbol_data, _strategy_returns


def _ts(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")


@pytest.fixture
def mock_pq_read():
    with patch("engine.runner.read_parquet") as m:
        yield m


@pytest.fixture
def mock_dirs():
    with patch("engine.runner.choose_partition_dir") as m_dir, \
         patch("engine.runner.list_parquet_files") as m_list:
        m_dir.return_value = Path("/tmp/fake")
        m_list.return_value = ["fake.parquet"]
        yield


class TestLoadSymbolData:
    def test_load_symbol_data_applies_ffill_limit(self, mock_dirs, mock_pq_read):
        """Ensures the explicitly passed event_feature_ffill_bars parameter is used."""
        idx = _ts(3)
        # Mock features and bars returning from read_parquet
        features_df = pd.DataFrame({"timestamp": idx, "base_feat": [1.0, 2.0, 3.0]})
        bars_df = pd.DataFrame({"timestamp": idx, "close": [100.0, 101.0, 102.0]})
        
        # read_parquet is called twice: once for features, once for bars
        mock_pq_read.side_effect = [features_df, bars_df]

        # Mock context to be empty
        with patch("engine.runner._load_context_data") as m_ctx, \
             patch("engine.runner._dedupe_timestamp_rows", side_effect=lambda df, label: (df, False)) as m_dedup, \
             patch("engine.runner._merge_event_features") as m_merge_ef, \
             patch("engine.runner._merge_event_flags") as m_merge_flags:
            
            m_ctx.return_value = pd.DataFrame(columns=["timestamp"])
            m_merge_flags.return_value = features_df.copy()
            m_merge_ef.return_value = features_df.copy()

            ef_df = pd.DataFrame({"timestamp": idx, "ef": [1, 2, 3]})
            _load_symbol_data(
                data_root=Path("/tmp/root"),
                symbol="BTCUSDT",
                run_id="test_run",
                event_features=ef_df,
                event_feature_ffill_bars=5,
            )

            # Assert _merge_event_features was called with the ffill limit
            # It's called with (features, ef, ffill_limit=5)
            m_merge_ef.assert_called_once()
            _, kwargs = m_merge_ef.call_args
            assert kwargs["ffill_limit"] == 5

    def test_load_symbol_data_bounds(self, mock_dirs, mock_pq_read):
        """Ensures timestamp clipping works correctly."""
        idx = _ts(5)
        features_df = pd.DataFrame({"timestamp": idx, "base_feat": [1, 2, 3, 4, 5]})
        bars_df = pd.DataFrame({"timestamp": idx, "close": [1, 2, 3, 4, 5]})
        mock_pq_read.side_effect = [features_df, bars_df]

        start = idx[1]
        end = idx[3]

        with patch("engine.runner._load_context_data") as m_ctx, \
             patch("engine.runner._dedupe_timestamp_rows", side_effect=lambda df, label: (df, False)) as m_dedup:
            m_ctx.return_value = pd.DataFrame(columns=["timestamp"])
            
            bars_out, feats_out = _load_symbol_data(
                data_root=Path("/tmp/root"),
                symbol="BTCUSDT",
                run_id="test_run",
                start_ts=start,
                end_ts=end,
                # Explicit argument testing to verify no `params` NameError
                event_feature_ffill_bars=12,
            )

            assert len(bars_out) == 3
            assert bars_out["timestamp"].min() == start
            assert bars_out["timestamp"].max() == end


class TestStrategyReturns:
    def test_double_lag_invariant(self):
        """Ensures 'double lag' throws ValueError unless allow_double_lag=1."""
        idx = _ts(3)
        bars = pd.DataFrame({
            "timestamp": idx, 
            "open": [1, 2, 3], "high": [1, 2, 3], "low": [1, 2, 3], "close": [1, 2, 3], 
            "volume": [10, 10, 10], "trades": [10, 10, 10], "target_volume": [10, 10, 10]
        })
        features = pd.DataFrame({"timestamp": idx})
        
        mock_strategy = Mock()
        mock_strategy.required_features = []
        pos = pd.Series([0, 1, 0], index=idx)
        pos.attrs["signal_events"] = []
        mock_strategy.generate_positions.return_value = pos
        
        with patch("engine.runner.get_strategy", return_value=mock_strategy):
            # DSL strategy implies execution_lag=0, blueprint_delay>0 is fine
            with pytest.raises(ValueError, match="Double lag detected"):
                _strategy_returns(
                    "BTCUSDT",
                    bars,
                    features,
                    "python_strat",  # Not DSL -> default exec lag = 1
                    params={"delay_bars": 1}, # Also has blueprint lag
                    cost_bps=5.0
                )
            
            # Allow double lag bypasses the error
            _strategy_returns(
                "BTCUSDT",
                bars,
                features,
                "python_strat",
                params={"delay_bars": 1, "allow_double_lag": 1},
                cost_bps=5.0
            )

    def test_positions_validation(self):
        """Ensures positions must be strictly -1, 0, or 1 and tz-aware."""
        from engine.runner import _validate_positions
        
        # Test tz-naive rejection
        bad_idx = pd.date_range("2024-01-01", periods=3, freq="5min")
        pos = pd.Series([0, 1, 0], index=bad_idx)
        with pytest.raises(ValueError, match="Positions index must be tz-aware"):
            _validate_positions(pos)

        # Test invalid bounds
        idx = _ts(3)
        pos = pd.Series([0, 2, 0], index=idx)
        with pytest.raises(ValueError, match="Positions must be in \\{-1,0,1\\}"):
            _validate_positions(pos)

