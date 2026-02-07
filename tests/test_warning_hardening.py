import sys
import warnings
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from engine.runner import _apply_context_defaults
from pipelines._lib.validation import ensure_utc_timestamp
from pipelines.ingest import ingest_binance_um_funding


def test_ensure_utc_timestamp_requires_tz_aware_utc() -> None:
    aware = pd.Series(pd.date_range("2024-01-01", periods=2, freq="h", tz="UTC"))
    out = ensure_utc_timestamp(aware, "ts")
    assert out.equals(aware)

    naive = pd.Series(pd.date_range("2024-01-01", periods=2, freq="h"))
    with pytest.raises(ValueError, match="timezone-aware UTC"):
        ensure_utc_timestamp(naive, "ts")


def test_apply_context_defaults_avoids_futurewarning_downcast() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="15min", tz="UTC"),
            "fp_active": pd.Series([1, None, "0"], dtype="object"),
            "fp_age_bars": pd.Series(["2", None, 0], dtype="object"),
            "fp_norm_due": pd.Series([None, 1, "0"], dtype="object"),
            "fp_severity": pd.Series(["0.3", None, "0.0"], dtype="object"),
        }
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("error", FutureWarning)
        out = _apply_context_defaults(frame)

    assert caught == []
    assert out["fp_active"].dtype == "int64"
    assert out["fp_age_bars"].dtype == "int64"
    assert out["fp_norm_due"].dtype == "int64"
    assert out["fp_severity"].dtype == "float64"


def test_funding_ingest_empty_merge_path_avoids_concat_futurewarning(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(ingest_binance_um_funding, "DATA_ROOT", tmp_path)

    def _fake_download(*args, **kwargs):
        return SimpleNamespace(status="not_found", path=None, error=None)

    def _fake_fetch_api(*args, **kwargs):
        return pd.DataFrame(columns=["timestamp", "funding_rate", "symbol", "source"]), 0

    monkeypatch.setattr(ingest_binance_um_funding, "download_with_retries", _fake_download)
    monkeypatch.setattr(ingest_binance_um_funding, "_fetch_funding_api", _fake_fetch_api)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ingest_binance_um_funding.py",
            "--run_id",
            "funding_empty_merge",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-01",
            "--max_retries",
            "0",
            "--retry_backoff_sec",
            "0",
            "--use_api_fallback",
            "1",
        ],
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        code = ingest_binance_um_funding.main()

    future_warnings = [w for w in caught if issubclass(w.category, FutureWarning)]
    assert code == 0
    assert future_warnings == []
