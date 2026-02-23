import sys
from pathlib import Path
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.report.performance_attribution_report import generate_attribution_report

class TestPerformanceAttributionReport:
    def test_generate_report_creates_file(self, tmp_path):
        """Test that the report file is actually created."""
        df = pd.DataFrame([
            {"vol_regime": "high", "net_pnl_bps": 5.0, "sharpe_ratio": 0.5},
            {"vol_regime": "low", "net_pnl_bps": 2.0, "sharpe_ratio": 1.2},
        ])
        
        output_file = tmp_path / "attribution_report.parquet"
        generate_attribution_report(df, str(output_file))
        
        assert output_file.exists()
        # Verify content
        loaded = pd.read_parquet(output_file)
        assert len(loaded) == 2
        assert "net_pnl_bps" in loaded.columns

    def test_generate_report_fails_invalid_df(self, tmp_path):
        """Should fail if DataFrame is not as expected."""
        df = pd.DataFrame({"wrong_col": [1, 2, 3]})
        output_file = tmp_path / "fail.parquet"
        
        with pytest.raises(ValueError):
            generate_attribution_report(df, str(output_file))
