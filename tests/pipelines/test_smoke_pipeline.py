import pytest
import pandas as pd
import dataclasses
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "project"))

from engine.runner import run_engine

@pytest.fixture
def mock_data_root(tmp_path):
    # Write a tiny fake feature/bar slice
    lake = tmp_path / "lake"
    feat_dir = lake / "features" / "perp" / "BTCUSDT" / "5m" / "features_v1"
    feat_dir.mkdir(parents=True)
    pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=10, freq="5min", tz="UTC"),
        "open": [100.0] * 10,
        "close": [100.0] * 10,
        "high": [101.0] * 10,
        "low": [99.0] * 10,
        "volume": [10.0] * 10,
        "direction_score": [0.5] * 10,
    }).to_parquet(feat_dir / "slice.parquet")
    
    bar_dir = lake / "cleaned" / "perp" / "BTCUSDT" / "bars_5m"
    bar_dir.mkdir(parents=True)
    pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=10, freq="5min", tz="UTC"),
        "open": [100.0] * 10,
        "close": [100.0] * 10,
        "high": [101.0] * 10,
        "low": [99.0] * 10,
        "volume": [10.0] * 10,
    }).to_parquet(bar_dir / "slice.parquet")
    
    univ_dir = lake / "metadata" / "universe_snapshots"
    univ_dir.mkdir(parents=True)
    pd.DataFrame({
        "symbol": ["BTCUSDT"],
        "listing_start": [pd.Timestamp("2020-01-01", tz="UTC")],
        "listing_end": [pd.Timestamp("2025-01-01", tz="UTC")]
    }).to_parquet(univ_dir / "univ.parquet")
    
    return tmp_path

def test_engine_smoke_test(mock_data_root):
    from strategy_dsl.schema import Blueprint, SymbolScopeSpec, EntrySpec, ExitSpec, SizingSpec, LineageSpec, EvaluationSpec
    bp = Blueprint(
        id="smoke_dsl", run_id="test", event_type="mock_event", candidate_id="mock_candidate", 
        direction="long",
        symbol_scope=SymbolScopeSpec(mode="single_symbol", symbols=["BTCUSDT"], candidate_symbol="BTCUSDT"),
        entry=EntrySpec(triggers=["event_detected"], conditions=[], confirmations=[], delay_bars=0, cooldown_bars=0, condition_logic="all", condition_nodes=[], arm_bars=0, reentry_lockout_bars=0),
        exit=ExitSpec(time_stop_bars=5, invalidation={"metric": "test", "operator": "==", "value": 0.0}, stop_type="percent", stop_value=0.05, target_type="percent", target_value=0.05, trailing_stop_type="none", trailing_stop_value=0.0, break_even_r=0.0),
        sizing=SizingSpec(mode="fixed_risk", risk_per_trade=0.01, target_vol=None, max_gross_leverage=1.0, max_position_scale=1.0, portfolio_risk_budget=1.0, symbol_risk_budget=1.0),
        overlays=[], evaluation=EvaluationSpec(min_trades=0, cost_model={"fees_bps": 5.0, "slippage_bps": 0.0, "funding_included": True}, robustness_flags={"oos_required": False, "multiplicity_required": False, "regime_stability_required": False}), lineage=LineageSpec(source_path="mock", compiler_version="mock", generated_at_utc="mock")
    )
    
    result = run_engine(
        data_root=mock_data_root,
        run_id="test_run",
        symbols=["BTCUSDT"],
        strategies=["dsl_interpreter_v1__smoke_dsl"],
        params_by_strategy={"dsl_interpreter_v1__smoke_dsl": {"dsl_blueprint": bp.model_dump()}},
        params={},
        cost_bps=5.0,
        start_ts=pd.Timestamp("2024-01-01", tz="UTC"),
        end_ts=pd.Timestamp("2024-01-02", tz="UTC"),
    )
    
    assert "metrics" in result
    assert "strategies" in result["metrics"]
    assert "dsl_interpreter_v1__smoke_dsl" in result["metrics"]["strategies"]
