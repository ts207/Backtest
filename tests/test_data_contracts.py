import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "project"))

import pytest

# These tests validate Pandera schemas. In minimal environments (e.g. contract-only
# test runs) Pandera may be intentionally absent.
pytest.importorskip("pandera")

import pandas as pd
from pandera.errors import SchemaError
from schemas.data_contracts import Cleaned5mBarsSchema, EventRegistrySchema

def test_cleaned_schema_valid():
    df = pd.DataFrame([{
        "symbol": "BTCUSDT",
        "timestamp": pd.Timestamp("2026-01-01T00:00:00Z"),
        "open": 10000.0,
        "high": 10100.0,
        "low": 9900.0,
        "close": 10050.0,
        "volume": 500.0,
        "quote_volume": 5000000.0,
        "is_gap": False,
        "funding_rate_realized": 0.0001
    }])
    Cleaned5mBarsSchema.validate(df)

def test_cleaned_schema_invalid_low_high():
    df = pd.DataFrame([{
        "symbol": "BTCUSDT",
        "timestamp": pd.Timestamp("2026-01-01T00:00:00Z"),
        "open": 10000.0,
        "high": 9900.0,
        "low": 10100.0,
        "close": 10050.0,
        "volume": 500.0
    }])
    with pytest.raises(SchemaError):
        Cleaned5mBarsSchema.validate(df)

def test_registry_schema_valid():
    df = pd.DataFrame([{
        "symbol": "BTCUSDT",
        "phenom_enter_ts": 1600000000000,
        "enter_ts": 1600000000000,
        "detected_ts": 1600000000000,
        "signal_ts": 1600000000000,
        "exit_ts": 1600000000000 + 300000,
        "event_id": "ev_1",
        "signal_column": "sig"
    }])
    EventRegistrySchema.validate(df)
