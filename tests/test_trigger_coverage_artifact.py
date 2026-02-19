
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "project"))

def test_trigger_coverage_artifact_writer(tmp_path):
    # Simulate StrategyResult-like dicts
    run_dir = tmp_path / "runs" / "R1"
    results = [
        {"strategy_name":"s1","diagnostics":{"dsl":{"trigger_coverage":{"missing":["a"],"all_zero":True,"triggers":{"a":{"true_count":0,"true_rate":0.0,"resolved":None}}}}},"strategy_metadata":{"blueprint_id":"b1"}},
        {"strategy_name":"s2","diagnostics":{"dsl":{"trigger_coverage":{"missing":[],"all_zero":False,"triggers":{"x":{"true_count":2,"true_rate":0.1,"resolved":"x"}}}}},"strategy_metadata":{"blueprint_id":"b2"}},
    ]
    from pipelines.backtest.backtest_strategies import _write_trigger_coverage
    _write_trigger_coverage(run_dir, results, enabled=1)
    p = run_dir / "engine" / "trigger_coverage.json"
    assert p.exists()
    data = json.loads(p.read_text())
    assert "by_strategy" in data and "s1" in data["by_strategy"] and "s2" in data["by_strategy"]
    assert data["missing_any"] == ["a"]
