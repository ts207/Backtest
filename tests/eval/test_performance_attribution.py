import sys
from pathlib import Path
import pandas as pd
import numpy as np
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from eval.performance_attribution import calculate_regime_metrics

class TestPerformanceAttribution:
    def test_calculate_regime_metrics_basic(self):
        """Test basic P&L aggregation by regime."""
        df = pd.DataFrame([
            {"vol_regime": "high", "pnl": 0.01},
            {"vol_regime": "high", "pnl": 0.02},
            {"vol_regime": "low", "pnl": -0.01},
            {"vol_regime": "low", "pnl": 0.03},
        ])
        
        metrics = calculate_regime_metrics(df)
        
        assert "high" in metrics.index
        assert "low" in metrics.index
        assert pytest.approx(metrics.loc["high", "total_pnl"]) == 0.03
        assert pytest.approx(metrics.loc["low", "total_pnl"]) == 0.02
        assert metrics.loc["high", "count"] == 2
        assert metrics.loc["low", "count"] == 2

    def test_calculate_regime_metrics_sharpe(self):
        """Test that sharpe_ratio is calculated (non-zero)."""
        # Create a series with some variance
        df = pd.DataFrame({
            "vol_regime": ["high"] * 10,
            "pnl": [0.01, 0.02, 0.01, 0.03, 0.01, 0.02, 0.01, 0.03, 0.01, 0.02]
        })
        
        metrics = calculate_regime_metrics(df)
        assert "sharpe_ratio" in metrics.columns
        assert metrics.loc["high", "sharpe_ratio"] > 0

    def test_calculate_regime_metrics_drawdown(self):
        """Test that max_drawdown is calculated correctly."""
        # Cum PNL: [1, 3, 2, 5, 4] -> Peak: [1, 3, 3, 5, 5] -> DD: [0, 0, 1, 0, 1]
        df = pd.DataFrame({
            "vol_regime": ["high"] * 5,
            "pnl": [1.0, 2.0, -1.0, 3.0, -1.0]
        })
        metrics = calculate_regime_metrics(df)
        assert "max_drawdown" in metrics.columns
        assert metrics.loc["high", "max_drawdown"] == 1.0

    def test_missing_columns(self):
        """Should raise ValueError if pnl or regime column missing."""
        df = pd.DataFrame({"pnl": [0.01]})
        with pytest.raises(ValueError, match="regime"):
            calculate_regime_metrics(df, regime_col="vol_regime")
            
        df = pd.DataFrame({"vol_regime": ["high"]})
        with pytest.raises(ValueError, match="pnl"):
            calculate_regime_metrics(df, pnl_col="pnl")
