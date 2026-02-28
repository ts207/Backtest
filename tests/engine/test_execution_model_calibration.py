import json
from pathlib import Path
from engine.execution_model import load_calibration_config

def test_load_calibration_falls_back_to_defaults(tmp_path):
    # No calibration file → use defaults from base config unchanged
    config = {"cost_model": "static", "base_fee_bps": 5.0, "base_slippage_bps": 3.0}
    result = load_calibration_config("BTCUSDT", calibration_dir=tmp_path, base_config=config)
    assert result["base_fee_bps"] == 5.0
    assert result["cost_model"] == "static"

def test_load_calibration_overrides_from_file(tmp_path):
    calib = {"spread_weight": 0.8, "base_slippage_bps": 1.5}
    (tmp_path / "BTCUSDT.json").write_text(json.dumps(calib))
    base = {"cost_model": "dynamic", "base_fee_bps": 5.0, "base_slippage_bps": 3.0, "spread_weight": 0.5}
    result = load_calibration_config("BTCUSDT", calibration_dir=tmp_path, base_config=base)
    assert result["spread_weight"] == 0.8        # overridden
    assert result["base_slippage_bps"] == 1.5    # overridden
    assert result["base_fee_bps"] == 5.0         # preserved from base

def test_load_calibration_ignores_none_values(tmp_path):
    calib = {"spread_weight": None, "base_slippage_bps": 2.0}
    (tmp_path / "ETHUSDT.json").write_text(json.dumps(calib))
    base = {"spread_weight": 0.5, "base_slippage_bps": 3.0}
    result = load_calibration_config("ETHUSDT", calibration_dir=tmp_path, base_config=base)
    assert result["spread_weight"] == 0.5   # None in calib should NOT override
    assert result["base_slippage_bps"] == 2.0

def test_load_calibration_ignores_malformed_json(tmp_path):
    (tmp_path / "BTCUSDT.json").write_text("{not valid json", encoding="utf-8")
    base = {"base_fee_bps": 5.0}
    result = load_calibration_config("BTCUSDT", calibration_dir=tmp_path, base_config=base)
    assert result["base_fee_bps"] == 5.0

def test_load_calibration_ignores_non_dict_json(tmp_path):
    (tmp_path / "BTCUSDT.json").write_text("[1, 2, 3]", encoding="utf-8")
    base = {"base_fee_bps": 5.0}
    result = load_calibration_config("BTCUSDT", calibration_dir=tmp_path, base_config=base)
    assert result["base_fee_bps"] == 5.0
