import os
import subprocess
import sys
import json
from pathlib import Path

import pandas as pd
import numpy as np
import pytest

# Add project to path
PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import HAS_PYARROW, write_parquet


def _read_table(path: Path) -> pd.DataFrame:
    """Read a parquet/csv table, tolerating parquet engine absence."""
    if path.exists():
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path)
        if HAS_PYARROW:
            return pd.read_parquet(path)
        alt = path.with_suffix(".csv")
        if alt.exists():
            return pd.read_csv(alt)
        raise ImportError("pyarrow/fastparquet required to read parquet")
    # Try alternate suffix.
    alt = path.with_suffix(".csv") if path.suffix.lower() == ".parquet" else path.with_suffix(".parquet")
    if alt.exists():
        return _read_table(alt)
    return pd.DataFrame()

@pytest.fixture
def mock_data_root(tmp_path):
    """
    Sets up a deterministic mock data environment for contract testing.
    """
    np.random.seed(42)
    data_root = tmp_path / "data"
    data_root.mkdir()
    
    symbols = ["BTCUSDT"]
    start_ts = pd.Timestamp("2024-01-01", tz="UTC")
    n_bars = 500
    
    # 1. Create fake features and bars
    for sym in symbols:
        feat_dir = data_root / "lake" / "features" / "perp" / sym / "5m" / "features_v1"
        feat_dir.mkdir(parents=True)
        bar_dir = data_root / "lake" / "cleaned" / "perp" / sym / "bars_5m"
        bar_dir.mkdir(parents=True)
        
        timestamps = pd.date_range(start_ts, periods=n_bars, freq="5min", tz="UTC")
        price = np.linspace(100.0, 150.0, n_bars)
        
        df = pd.DataFrame({
            "timestamp": timestamps,
            "open": price,
            "high": price + 0.1,
            "low": price - 0.1,
            "close": price,
            "volume": 100.0,
            "quote_volume": 1000.0,
            "is_gap": False,
        })
        
        write_parquet(df, feat_dir / "slice.parquet")
        write_parquet(df[["timestamp", "open", "high", "low", "close", "volume", "quote_volume", "is_gap"]], bar_dir / "slice.parquet")

        # 2. Market state
        ms_dir = data_root / "lake" / "context" / "market_state" / sym
        ms_dir.mkdir(parents=True)
        write_parquet(pd.DataFrame({
            "timestamp": timestamps,
            "vol_regime_code": [0] * n_bars,
            "vol_regime": ["high"] * n_bars,
        }), ms_dir / "5m.parquet")

    # 3. Universe
    univ_dir = data_root / "lake" / "metadata" / "universe_snapshots"
    univ_dir.mkdir(parents=True)
    write_parquet(pd.DataFrame({
        "symbol": symbols,
        "listing_start": [pd.Timestamp("2020-01-01", tz="UTC")] * len(symbols),
        "listing_end": [pd.Timestamp("2025-01-01", tz="UTC")] * len(symbols)
    }), univ_dir / "univ.parquet")
    
    # 4. Mock fees
    (data_root / "lake" / "metadata").mkdir(parents=True, exist_ok=True)
    with open(data_root / "lake" / "metadata" / "fees.yaml", "w") as f:
        f.write("standard:\n  taker_fee_bps: 2.0\n  maker_fee_bps: 1.0\n")

    return data_root

def run_script(script_path, args, data_root):
    env = os.environ.copy()
    env["BACKTEST_DATA_ROOT"] = str(data_root)
    env["PYTHONPATH"] = str(PROJECT_ROOT)
    env["TZ"] = "UTC"
    
    cmd = [sys.executable, str(script_path)] + args
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.stdout:
        print(f"--- STDOUT ({script_path.name}) ---\n{result.stdout}")
    if result.stderr:
        print(f"--- STDERR ({script_path.name}) ---\n{result.stderr}", file=sys.stderr)
    return result

@pytest.mark.contract
def test_e2e_artifact_contract_directional_and_multi(mock_data_root, tmp_path):
    """
    Contract test for directional events and multi-type analyzers.
    """
    run_id = "test_contract_complex"
    symbols = "BTCUSDT"
    registry_script = PROJECT_ROOT / "pipelines" / "research" / "build_event_registry.py"
    discovery_script = PROJECT_ROOT / "pipelines" / "research" / "phase2_candidate_discovery.py"
    
    custom_analyzer_content = f"""
import pandas as pd
from pathlib import Path
import sys
import os

PROJECT_ROOT = Path("{PROJECT_ROOT}")
sys.path.insert(0, str(PROJECT_ROOT))
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.io_utils import HAS_PYARROW, write_parquet


def _read_table(path: Path) -> pd.DataFrame:
    if path.exists() and path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.exists() and path.suffix.lower() == ".parquet" and HAS_PYARROW:
        return pd.read_parquet(path)
    if path.with_suffix(".csv").exists():
        return pd.read_csv(path.with_suffix(".csv"))
    if path.with_suffix(".parquet").exists() and HAS_PYARROW:
        return pd.read_parquet(path.with_suffix(".parquet"))
    return pd.DataFrame()

def main():
    data_root = Path(os.getenv("BACKTEST_DATA_ROOT"))
    run_id = "test_contract_complex"
    feat_path = data_root / "lake" / "features" / "perp" / "BTCUSDT" / "5m" / "features_v1" / "slice.parquet"
    df = _read_table(feat_path)
    
    events = []
    # Event 1: LIQUIDITY_VACUUM
    events.append({{
        "event_id": "ev_1",
        "symbol": "BTCUSDT",
        "timestamp": df["timestamp"].iat[10],
        "event_type": "LIQUIDITY_VACUUM",
        "direction": 1.0,
        "severity": 10.0,
        "severity_bucket": "extreme_5pct"
    }})
    # Event 2: VOL_SHOCK
    events.append({{
        "event_id": "ev_2",
        "symbol": "BTCUSDT",
        "timestamp": df["timestamp"].iat[20],
        "event_type": "VOL_SHOCK",
        "direction": 1.0,
        "severity": 10.0,
        "severity_bucket": "extreme_5pct"
    }})
    
    events_df = pd.DataFrame(events)
    out_dir = data_root / "reports" / "liquidity_vacuum" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(events_df, out_dir / "liquidity_vacuum_events.parquet")
    
    manifest = start_manifest("custom_multi", run_id, {{}}, [], [])
    finalize_manifest(manifest, "success", stats={{}})

if __name__ == "__main__":
    main()
"""
    custom_analyzer_path = tmp_path / "custom_analyzer.py"
    custom_analyzer_path.write_text(custom_analyzer_content)
    
    res = run_script(custom_analyzer_path, [], mock_data_root)
    assert res.returncode == 0
    
    # 1. Multi-type filtering
    res = run_script(registry_script, ["--run_id", run_id, "--symbols", symbols, "--event_type", "LIQUIDITY_VACUUM"], mock_data_root)
    assert res.returncode == 0
    
    df_reg = _read_table(mock_data_root / "events" / run_id / "events.parquet")
    assert (df_reg["event_type"] == "LIQUIDITY_VACUUM").all()
    assert "direction" in df_reg.columns
    
    # 2. Directional Sign Flip
    def run_directional_test(sign_val, run_suffix):
        rid = f"run_dir_{run_suffix}"
        events = [
            {
                "event_id": "ev_train",
                "symbol": "BTCUSDT",
                "timestamp": pd.Timestamp("2024-01-01 00:50:00", tz="UTC"),
                "event_type": "LIQUIDITY_VACUUM",
                "direction": float(sign_val),
                "severity": 10.0,
                "severity_bucket": "extreme_5pct",
                "split_label": "train"
            },
            {
                "event_id": "ev_val",
                "symbol": "BTCUSDT",
                "timestamp": pd.Timestamp("2024-01-01 10:00:00", tz="UTC"),
                "event_type": "LIQUIDITY_VACUUM",
                "direction": float(sign_val),
                "severity": 10.0,
                "severity_bucket": "extreme_5pct",
                "split_label": "validation"
            }
        ]
        out_dir = mock_data_root / "reports" / "liquidity_vacuum" / rid
        out_dir.mkdir(parents=True, exist_ok=True)
        write_parquet(pd.DataFrame(events), out_dir / "liquidity_vacuum_events.parquet")
        
        run_script(registry_script, ["--run_id", rid, "--symbols", symbols, "--event_type", "LIQUIDITY_VACUUM"], mock_data_root)
        
        res_p2 = run_script(discovery_script, [
            "--run_id", rid, 
            "--event_type", "LIQUIDITY_VACUUM", 
            "--symbols", symbols, 
            "--mode", "research",
            "--min_samples", "1",
            "--adaptive_lambda_max", "1.0",
            "--adaptive_shrinkage_lambda", "0",
            "--gate_profile", "discovery"
        ], mock_data_root)
        assert res_p2.returncode == 0
        
        df = _read_table(mock_data_root / "reports" / "phase2" / rid / "LIQUIDITY_VACUUM" / "phase2_candidates_raw.parquet")
        return df[df["rule_template"] == "continuation"]["expectancy"].iloc[0]

    exp_pos = run_directional_test(1.0, "pos")
    exp_neg = run_directional_test(-1.0, "neg")
    
    assert exp_pos > 0
    assert exp_neg < 0
    assert np.sign(exp_pos) != np.sign(exp_neg)

@pytest.mark.contract
def test_e2e_artifact_contract_deterministic(mock_data_root, tmp_path):
    """
    Full pipeline contract test including Promotion and OOS split verification.
    """
    run_id = "test_contract_e2e"
    symbols = "BTCUSDT"
    entry_lag_bars = 2
    
    registry_script = PROJECT_ROOT / "pipelines" / "research" / "build_event_registry.py"
    discovery_script = PROJECT_ROOT / "pipelines" / "research" / "phase2_candidate_discovery.py"
    bridge_script = PROJECT_ROOT / "pipelines" / "research" / "bridge_evaluate_phase2.py"
    promote_script = PROJECT_ROOT / "pipelines" / "research" / "promote_candidates.py"
    compile_script = PROJECT_ROOT / "pipelines" / "research" / "compile_strategy_blueprints.py"

    # Ensure events in both train and validation
    events = [
        {
            "event_id": "ev_train",
            "symbol": "BTCUSDT",
            "timestamp": pd.Timestamp("2024-01-01 01:00:00", tz="UTC"),
            "event_type": "LIQUIDITY_VACUUM",
            "direction": 1.0,
            "severity": 10.0,
            "severity_bucket": "extreme_5pct",
            "split_label": "train"
        },
        {
            "event_id": "ev_val",
            "symbol": "BTCUSDT",
            "timestamp": pd.Timestamp("2024-01-01 12:00:00", tz="UTC"),
            "event_type": "LIQUIDITY_VACUUM",
            "direction": 1.0,
            "severity": 10.0,
            "severity_bucket": "extreme_5pct",
            "split_label": "validation"
        }
    ]
    out_dir = mock_data_root / "reports" / "liquidity_vacuum" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(pd.DataFrame(events), out_dir / "liquidity_vacuum_events.parquet")

    # 1. Build Registry
    res = run_script(registry_script, ["--run_id", run_id, "--symbols", symbols, "--event_type", "LIQUIDITY_VACUUM"], mock_data_root)
    assert res.returncode == 0
    
    # 2. Run Phase 2
    res = run_script(discovery_script, [
        "--run_id", run_id, 
        "--event_type", "LIQUIDITY_VACUUM", 
        "--symbols", symbols, 
        "--mode", "research",
        "--min_samples", "1",
        "--entry_lag_bars", str(entry_lag_bars),
        "--adaptive_lambda_max", "1.0",
        "--adaptive_shrinkage_lambda", "0",
        "--gate_profile", "discovery"
    ], mock_data_root)
    assert res.returncode == 0
    
    # 3. Bridge Evaluation
    res = run_script(bridge_script, [
        "--run_id", run_id,
        "--event_type", "LIQUIDITY_VACUUM",
        "--symbols", symbols,
        "--start", "2024-01-01",
        "--end", "2024-01-02",
        "--mode", "research",
        "--min_validation_trades", "1"
    ], mock_data_root)
    assert res.returncode == 0
    
    # 4. Promote Candidates
    res = run_script(promote_script, [
        "--run_id", run_id,
        "--min_events", "1",
        "--min_stability_score", "0.0",
        "--min_sign_consistency", "0.0",
        "--min_cost_survival_ratio", "0.0",
        "--min_tob_coverage", "0.0",
        "--allow_discovery_promotion", "1",
        "--max_q_value", "1.0"
    ], mock_data_root)
    assert res.returncode == 0
    
    promoted_path = mock_data_root / "reports" / "promotions" / run_id / "promoted_candidates.parquet"
    if not promoted_path.exists():
        promoted_path = promoted_path.with_suffix(".csv")
    assert promoted_path.exists()
    df_promoted = _read_table(promoted_path)
    assert not df_promoted.empty
    
    # ASSERT: Authoritative OOS Split usage
    for _, row in df_promoted.iterrows():
        assert int(row["validation_samples"]) > 0
        assert pd.notna(row["selection_score"])

    # 5. Compile Strategy Blueprints
    res = run_script(compile_script, [
        "--run_id", run_id,
        "--symbols", symbols,
        "--ignore_checklist", "1",
        "--allow_naive_entry_fail", "1",
        "--allow_fallback_blueprints", "1",
        "--strict_cost_fields", "0",
        "--quality_floor_fallback", "0.0"
    ], mock_data_root)
    assert res.returncode == 0

    blueprint_path = mock_data_root / "runs" / run_id / "blueprints.jsonl"
    if blueprint_path.exists():
        print("Blueprints successfully generated.")
    else:
        print("Pipeline completed successfully, but no blueprints generated due to quality gates.")
