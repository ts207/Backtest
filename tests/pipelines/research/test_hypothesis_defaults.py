from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research._hypothesis_defaults import (
    extract_event_type,
    load_hypothesis_defaults,
    parse_symbols_filter,
)


def test_load_hypothesis_defaults_reads_global_defaults():
    defaults = load_hypothesis_defaults(project_root=PROJECT_ROOT)
    assert "5m" in defaults["horizons"]
    assert "mean_reversion" in defaults["rule_templates"]
    assert "vol_regime" in defaults["conditioning"]
    assert "carry_state" in defaults["conditioning"]
    assert "funding_bps" in defaults["conditioning"]
    assert "severity_bucket" not in defaults["conditioning"]


def test_parse_symbols_filter_restricts_to_run_universe():
    selected = parse_symbols_filter("BTC|SOL", universe=["BTCUSDT", "ETHUSDT"])
    assert selected == ["BTCUSDT"]


def test_extract_event_type_from_statement():
    statement = 'Some payload {""event_type"": ""LIQUIDITY_VACUUM""}'
    assert extract_event_type(statement) == "LIQUIDITY_VACUUM"
