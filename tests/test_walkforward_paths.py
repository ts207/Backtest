from pipelines.eval.run_walkforward import _infer_backtest_family_dir


def test_infer_family_dir_blueprint_mode_is_dsl():
    assert _infer_backtest_family_dir(strategies="", blueprints_path="/tmp/blueprints.jsonl") == "dsl"


def test_infer_family_dir_strategy_mode_breakout():
    assert _infer_backtest_family_dir(strategies="vol_compression_v1", blueprints_path=None) == "breakout"


def test_infer_family_dir_strategy_mode_hybrid():
    fam = _infer_backtest_family_dir(strategies="vol_compression_v1,funding_extreme_reversal_v1", blueprints_path=None)
    assert fam == "hybrid"
