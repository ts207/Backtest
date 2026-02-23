import pytest
from datetime import datetime, timezone
import subprocess
import sys

from pipelines.ingest.ingest_binance_um_ohlcv_1m import _expected_bars as expected_bars_1m
from pipelines.ingest.ingest_binance_um_ohlcv_5m import _expected_bars as expected_bars_5m

def test_expected_bars_logic():
    start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
    
    # 1 hour = 60 minutes
    # 1m script should expect 60 bars
    # 5m script should expect 12 bars
    
    assert expected_bars_1m(start, end) == 60
    assert expected_bars_5m(start, end) == 12

def test_description_and_manifest_names():
    # Verify that the 1m script has 1m in its manifest and description
    with open("project/pipelines/ingest/ingest_binance_um_ohlcv_1m.py", "r") as f:
        content = f.read()
        assert 'Ingest Binance USD-M OHLCV 1m from archives' in content
        assert 'start_manifest("ingest_binance_um_ohlcv_1m"' in content
        assert '"1m"' in content # Should have 1m URLs
        
    with open("project/pipelines/ingest/ingest_binance_um_mark_price_1m.py", "r") as f:
        content = f.read()
        assert 'Ingest Binance USD-M mark price 1m from archives' in content
        assert 'start_manifest("ingest_binance_um_mark_price_1m"' in content

    with open("project/pipelines/ingest/ingest_binance_um_mark_price_5m.py", "r") as f:
        content = f.read()
        assert 'Ingest Binance USD-M mark price 5m from archives' in content
        assert 'start_manifest("ingest_binance_um_mark_price_5m"' in content
