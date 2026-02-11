import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.ingest import ingest_binance_um_open_interest_hist as oi


class _Resp:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload


class _Session:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = 0

    def get(self, url, params, timeout):  # noqa: ARG002
        self.calls += 1
        if self.pages:
            payload = self.pages.pop(0)
        else:
            payload = []
        return _Resp(200, payload)


def test_fetch_open_interest_hist_pages_and_parses() -> None:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, tzinfo=timezone.utc)
    pages = [
        [
            {"timestamp": 1704067200000, "sumOpenInterest": "100.0", "sumOpenInterestValue": "200.0"},
            {"timestamp": 1704067500000, "sumOpenInterest": "101.0", "sumOpenInterestValue": "202.0"},
        ],
        [
            {"timestamp": 1704067800000, "sumOpenInterest": "102.0", "sumOpenInterestValue": "204.0"},
        ],
        [],
    ]
    session = _Session(pages=pages)
    out, calls = oi._fetch_open_interest_hist(
        session=session,
        api_base="https://fapi.binance.com",
        symbol="BTCUSDT",
        period="5m",
        start=start,
        end_exclusive=end,
        limit=500,
        sleep_sec=0.0,
    )
    assert calls >= 2
    assert len(out) == 3
    assert list(out.columns) == [
        "timestamp",
        "symbol",
        "sum_open_interest",
        "sum_open_interest_value",
        "cmc_circulating_supply",
        "source",
    ]
    assert out["symbol"].nunique() == 1
