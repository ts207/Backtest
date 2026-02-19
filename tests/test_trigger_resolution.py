
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "project"))

from strategy_dsl.contract_v1 import resolve_trigger_column

def test_resolve_trigger_variants():
    cols = ["vol_shock_relaxation_flag", "event_range_compression_breakout_window", "flag_liquidity_absence_window"]
    assert resolve_trigger_column("vol_shock_relaxation", cols) == "vol_shock_relaxation_flag"
    assert resolve_trigger_column("event_range_compression_breakout_window", cols) == "event_range_compression_breakout_window"
    assert resolve_trigger_column("liquidity_absence_window", cols) == "flag_liquidity_absence_window"
    assert resolve_trigger_column("missing_trigger", cols) is None
