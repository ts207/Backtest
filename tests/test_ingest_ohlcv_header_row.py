import sys
from pathlib import Path
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.ingest.ingest_binance_um_ohlcv_15m import _read_ohlcv_from_zip


def _write_zip(path: Path, payload: str) -> None:
    with ZipFile(path, "w") as zf:
        zf.writestr("BTCUSDT-15m-2020-01.csv", payload)


def test_read_ohlcv_handles_archive_with_header_row(tmp_path: Path) -> None:
    zip_path = tmp_path / "ohlcv_header.zip"
    _write_zip(
        zip_path,
        "\n".join(
            [
                "open_time,open,high,low,close,volume,close_time,quote_volume,trade_count,taker_base_volume,taker_quote_volume,ignore",
                "1577836800000,7200,7210,7190,7205,100,1577837699999,0,0,0,0,0",
            ]
        )
        + "\n",
    )

    out = _read_ohlcv_from_zip(zip_path, symbol="BTCUSDT", source="archive_monthly")
    assert len(out) == 1
    assert out.iloc[0]["timestamp"].isoformat() == "2020-01-01T00:00:00+00:00"
    assert float(out.iloc[0]["close"]) == 7205.0
    assert out.iloc[0]["symbol"] == "BTCUSDT"
    assert out.iloc[0]["source"] == "archive_monthly"
