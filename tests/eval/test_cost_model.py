import sys
from pathlib import Path
import pandas as pd
import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from eval.cost_model import apply_cost_model

class TestCostModel:
    def test_apply_cost_model_basic(self, tmp_path):
        """Test basic cost application from a config file."""
        df = pd.DataFrame([
            {"pnl_bps": 20.0},
            {"pnl_bps": -5.0},
        ])
        
        # Create a temporary config file
        config_data = {
            "fee_bps_per_side": 5.0,
            "slippage_bps_per_fill": 2.5
        }
        config_file = tmp_path / "fees.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_data, f)
            
        result = apply_cost_model(df, str(config_file))
        
        # Round trip cost = (5.0 + 2.5) * 2 = 15.0 bps
        assert "net_pnl_bps" in result.columns
        assert pytest.approx(result.iloc[0]["net_pnl_bps"]) == 5.0
        assert pytest.approx(result.iloc[1]["net_pnl_bps"]) == -20.0

    def test_apply_cost_model_missing_config(self):
        """Should raise FileNotFoundError if config doesn't exist."""
        df = pd.DataFrame({"pnl_bps": [10.0]})
        with pytest.raises(FileNotFoundError):
            apply_cost_model(df, "non_existent_config.yaml")

    def test_apply_cost_model_missing_pnl_col(self, tmp_path):
        """Should raise ValueError if pnl_bps column missing."""
        df = pd.DataFrame({"some_other_col": [10.0]})
        config_file = tmp_path / "fees.yaml"
        with open(config_file, "w") as f:
            yaml.dump({"fee_bps_per_side": 4}, f)
            
        with pytest.raises(ValueError, match="pnl_bps"):
            apply_cost_model(df, str(config_file))
