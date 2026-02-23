import sys
from pathlib import Path
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from eval.attribution_joiner import join_candidates_with_features

class TestAttributionJoiner:
    def test_join_basic(self):
        """Test exact timestamp/symbol join."""
        candidates = pd.DataFrame([
            {"timestamp": "2024-01-01 10:00:00", "symbol": "BTCUSDT", "p_value": 0.01},
            {"timestamp": "2024-01-01 10:05:00", "symbol": "BTCUSDT", "p_value": 0.05},
        ])
        candidates["timestamp"] = pd.to_datetime(candidates["timestamp"])
        
        features = pd.DataFrame([
            {"timestamp": "2024-01-01 10:00:00", "symbol": "BTCUSDT", "vol_regime": "high"},
            {"timestamp": "2024-01-01 10:05:00", "symbol": "BTCUSDT", "vol_regime": "low"},
        ])
        features["timestamp"] = pd.to_datetime(features["timestamp"])
        
        joined = join_candidates_with_features(candidates, features)
        
        assert "vol_regime" in joined.columns
        assert len(joined) == 2
        assert joined.iloc[0]["vol_regime"] == "high"
        assert joined.iloc[1]["vol_regime"] == "low"

    def test_join_missing_feature(self):
        """Test that missing features result in NaN (outer or left join behavior)."""
        candidates = pd.DataFrame([
            {"timestamp": "2024-01-01 10:00:00", "symbol": "BTCUSDT", "p_value": 0.01},
            {"timestamp": "2024-01-01 10:10:00", "symbol": "BTCUSDT", "p_value": 0.05},
        ])
        candidates["timestamp"] = pd.to_datetime(candidates["timestamp"])
        
        features = pd.DataFrame([
            {"timestamp": "2024-01-01 10:00:00", "symbol": "BTCUSDT", "vol_regime": "high"},
        ])
        features["timestamp"] = pd.to_datetime(features["timestamp"])
        
        joined = join_candidates_with_features(candidates, features)
        
        assert len(joined) == 2
        assert joined.iloc[0]["vol_regime"] == "high"
        assert pd.isna(joined.iloc[1]["vol_regime"])

    def test_merge_fails_if_no_timestamp(self):
        """Should raise ValueError if timestamp or symbol missing in candidates."""
        candidates = pd.DataFrame([{"p_value": 0.01}])
        features = pd.DataFrame([{"timestamp": "2024-01-01", "symbol": "BTC", "vol_regime": "high"}])
        
        with pytest.raises(ValueError, match="Candidates"):
            join_candidates_with_features(candidates, features)

    def test_merge_fails_if_features_missing_cols(self):
        """Should raise ValueError if timestamp or symbol missing in features."""
        candidates = pd.DataFrame([{"timestamp": "2024-01-01", "symbol": "BTC", "p_value": 0.01}])
        features = pd.DataFrame([{"vol_regime": "high"}])
        
        with pytest.raises(ValueError, match="Features"):
            join_candidates_with_features(candidates, features)
