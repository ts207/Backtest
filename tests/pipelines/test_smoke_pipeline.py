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
        symbol_scope=SymbolScopeSpec("single_symbol", ["BTCUSDT"], "BTCUSDT"),
        entry=EntrySpec(["event_detected"], [], [], 0, 0, "all", [], 0, 0),
        exit=ExitSpec(5, {"metric": "", "operator": "", "value": 0.0}, "percent", 0.05, "percent", 0.05, "none", 0.0, 0.0),
        sizing=SizingSpec("fixed_risk", 0.01, None, 1.0, 1.0, 1.0, 1.0),
        overlays=[], evaluation=EvaluationSpec(0, {"fees_bps": 5.0, "slippage_bps": 0.0, "funding_included": True}, {"oos_required": False, "multiplicity_required": False, "regime_stability_required": False}), lineage=LineageSpec("mock", "mock", "mock")
    )
    
    result = run_engine(
        data_root=mock_data_root,
        run_id="test_run",
        symbols=["BTCUSDT"],
        strategies=["dsl_interpreter_v1__smoke_dsl"],
        params_by_strategy={"dsl_interpreter_v1__smoke_dsl": {"dsl_blueprint": dataclasses.asdict(bp)}},
        params={},
        cost_bps=5.0,
        start_ts=pd.Timestamp("2024-01-01", tz="UTC"),
        end_ts=pd.Timestamp("2024-01-02", tz="UTC"),
    )
    
    assert "metrics" in result
    assert "strategies" in result["metrics"]
    assert "dsl_interpreter_v1__smoke_dsl" in result["metrics"]["strategies"]
