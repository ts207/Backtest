from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.ingest.ingest_binance_um_book_ticker as ingest_book_ticker


class _FakeFuture:
    def __init__(self, *, result_payload=None, error: Exception | None = None):
        self._result_payload = result_payload
        self._error = error

    def result(self):
        if self._error is not None:
            raise self._error
        return self._result_payload


def _patch_common_runtime(monkeypatch, capture: list[dict], future_factory):
    class _FakeExecutor:
        def __init__(self, max_workers):
            self.max_workers = max_workers

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, symbol, effective_start, effective_end, out_root, args):
            return future_factory(symbol)

    monkeypatch.setattr(ingest_book_ticker, "HAS_PYARROW", True)
    monkeypatch.setattr(ingest_book_ticker, "ProcessPoolExecutor", _FakeExecutor)
    monkeypatch.setattr(ingest_book_ticker, "as_completed", lambda futures: list(futures))
    monkeypatch.setattr(
        ingest_book_ticker,
        "start_manifest",
        lambda stage, run_id, params, inputs, outputs: {"stage": stage, "run_id": run_id},
    )

    def fake_finalize(manifest, status, error=None, stats=None):
        capture.append({"status": status, "error": error, "stats": stats})
        return manifest

    monkeypatch.setattr(ingest_book_ticker, "finalize_manifest", fake_finalize)


def test_book_ticker_collects_all_symbol_outcomes_before_manifest(monkeypatch):
    capture: list[dict] = []

    def future_factory(symbol: str):
        if symbol == "ETHUSDT":
            return _FakeFuture(error=RuntimeError("eth failed"))
        return _FakeFuture(
            result_payload={
                "symbol": symbol,
                "rows_written": 123,
                "missing_archive_files": [],
                "partitions_written": ["x"],
                "partitions_skipped": [],
            }
        )

    _patch_common_runtime(monkeypatch, capture, future_factory)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ingest_binance_um_book_ticker.py",
            "--run_id",
            "r_book_ticker",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start",
            "2026-01-01",
            "--end",
            "2026-01-02",
            "--concurrency",
            "2",
        ],
    )

    rc = ingest_book_ticker.main()
    assert rc == 1
    assert capture
    assert capture[-1]["status"] == "failed"
    stats = capture[-1]["stats"]
    assert "symbols" in stats
    assert "BTCUSDT" in stats["symbols"]
    assert "ETHUSDT" in stats["symbols"]
    assert stats["symbols"]["ETHUSDT"]["status"] == "failed"
    assert "eth failed" in stats["symbols"]["ETHUSDT"]["error_message"]


def test_book_ticker_success_when_all_symbols_succeed(monkeypatch):
    capture: list[dict] = []

    def future_factory(symbol: str):
        return _FakeFuture(
            result_payload={
                "symbol": symbol,
                "rows_written": 10,
                "missing_archive_files": [],
                "partitions_written": ["x"],
                "partitions_skipped": [],
            }
        )

    _patch_common_runtime(monkeypatch, capture, future_factory)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ingest_binance_um_book_ticker.py",
            "--run_id",
            "r_book_ticker_ok",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start",
            "2026-01-01",
            "--end",
            "2026-01-02",
            "--concurrency",
            "2",
        ],
    )

    rc = ingest_book_ticker.main()
    assert rc == 0
    assert capture
    assert capture[-1]["status"] == "success"
    stats = capture[-1]["stats"]
    assert set(stats["symbols"]) == {"BTCUSDT", "ETHUSDT"}
