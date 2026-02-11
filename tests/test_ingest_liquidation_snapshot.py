import sys
import zipfile
from pathlib import Path

import pandas as pd
from pandas import DatetimeTZDtype

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.ingest import ingest_binance_um_liquidation_snapshot as liq


def _write_zip_csv(path: Path, csv_text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("data.csv", csv_text)


def test_parse_liquidation_snapshot_zip_basic(tmp_path: Path) -> None:
    zip_path = tmp_path / "liq.zip"
    _write_zip_csv(
        zip_path,
        "time,side,price,qty\n1704067200000,BUY,43000.5,1.2\n1704067260000,SELL,42990.0,0.8\n",
    )
    out = liq._parse_liquidation_from_zip(zip_path, "BTCUSDT")
    assert not out.empty
    assert list(out.columns) == ["ts", "symbol", "side", "price", "qty", "notional", "source"]
    assert out["symbol"].nunique() == 1
    assert set(out["side"].unique()) == {"BUY", "SELL"}
    assert isinstance(out["ts"].dtype, DatetimeTZDtype)


def test_to_cm_contract_btc_eth_aliases() -> None:
    assert liq._to_cm_contract("BTCUSDT") == "BTCUSD_PERP"
    assert liq._to_cm_contract("ETHUSDT") == "ETHUSD_PERP"
