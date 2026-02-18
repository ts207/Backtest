from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.http_utils import download_with_retries
from pipelines._lib.io_utils import ensure_dir, read_parquet, write_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.url_utils import join_url
from pipelines._lib.validation import ensure_utc_timestamp

ARCHIVE_BASE = "https://data.binance.vision/data/futures/cm"
ARCHIVE_LIST_BASE = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"

CM_SYMBOL_MAP = {
    "BTC": "BTCUSD_PERP",
    "BTCUSDT": "BTCUSD_PERP",
    "BTCUSD": "BTCUSD_PERP",
    "BTCUSD_PERP": "BTCUSD_PERP",
    "ETH": "ETHUSD_PERP",
    "ETHUSDT": "ETHUSD_PERP",
    "ETHUSD": "ETHUSD_PERP",
    "ETHUSD_PERP": "ETHUSD_PERP",
}


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _month_start(ts: datetime) -> datetime:
    ts = ts.astimezone(timezone.utc)
    return ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _next_month(ts: datetime) -> datetime:
    ts = ts.astimezone(timezone.utc)
    y, m = ts.year, ts.month
    if m == 12:
        y += 1
        m = 1
    else:
        m += 1
    return ts.replace(year=y, month=m, day=1, hour=0, minute=0, second=0, microsecond=0)


def _iter_months(start: datetime, end: datetime) -> List[datetime]:
    out: List[datetime] = []
    cur = _month_start(start)
    while cur <= end:
        out.append(cur)
        cur = _next_month(cur)
    return out


def _iter_days(start: datetime, end_exclusive: datetime) -> Iterable[datetime]:
    cur = start.astimezone(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end_exclusive = end_exclusive.astimezone(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    while cur < end_exclusive:
        yield cur
        cur += timedelta(days=1)


def _to_cm_contract(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if not s:
        return s
    if s in CM_SYMBOL_MAP:
        return CM_SYMBOL_MAP[s]
    if s.endswith("_PERP"):
        return s
    if re.search(r"_\d{6}$", s):
        return s
    if s.endswith("USDT") and len(s) > 4:
        return f"{s[:-4]}USD_PERP"
    if s.endswith("BUSD") and len(s) > 4:
        return f"{s[:-4]}USD_PERP"
    if s.endswith("USD") and len(s) > 3:
        return f"{s}_PERP"
    return s


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        out = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(out):
            return None
        return float(out)
    if pd.isna(out):
        return None
    return float(out)


def _pick_first_numeric(df: pd.DataFrame, keys: List[str]) -> pd.Series:
    out = pd.Series([pd.NA] * len(df), index=df.index, dtype="Float64")
    for key in keys:
        if key in df.columns:
            vals = pd.to_numeric(df[key], errors="coerce").astype("Float64")
            out = out.fillna(vals)
    return out


def _normalize_side(value: object) -> str:
    s = str(value or "").strip().upper()
    if s in {"BUY", "SELL"}:
        return s
    return "UNKNOWN"


def _read_csv_from_zip(path: Path) -> pd.DataFrame:
    with ZipFile(path) as zf:
        names = zf.namelist()
        if not names:
            return pd.DataFrame()
        csv_name = names[0]
        with zf.open(csv_name) as f:
            df = pd.read_csv(f)
            # If file has no header row, re-read with expected column names.
            lowered = {str(col).lower(): col for col in df.columns}
            if "time" not in lowered and "timestamp" not in lowered:
                f.seek(0)
                df = pd.read_csv(
                    f,
                    header=None,
                    names=[
                        "time",
                        "side",
                        "order_type",
                        "time_in_force",
                        "original_quantity",
                        "price",
                        "average_price",
                        "order_status",
                        "last_fill_quantity",
                        "accumulated_fill_quantity",
                    ],
                )
    return df


def _parse_liquidation_from_zip(
    path: Path, symbol: str, source: str = "binance_data_vision_cm_liquidation_snapshot"
) -> pd.DataFrame:
    raw = _read_csv_from_zip(path)
    if raw.empty:
        return pd.DataFrame(
            columns=["ts", "symbol", "side", "price", "qty", "notional", "source"]
        )

    # Normalize to lowercase columns so historical name variants are handled.
    raw.columns = [str(c).strip().lower() for c in raw.columns]

    ts_col = None
    for candidate in ("time", "timestamp", "create_time", "createtime", "ts"):
        if candidate in raw.columns:
            ts_col = candidate
            break
    if ts_col is None:
        return pd.DataFrame(
            columns=["ts", "symbol", "side", "price", "qty", "notional", "source"]
        )

    ts_numeric = pd.to_numeric(raw[ts_col], errors="coerce")
    ts_unit = "ms" if ts_numeric.dropna().abs().median() >= 1_000_000_000_000 else "s"
    ts = pd.to_datetime(ts_numeric, unit=ts_unit, utc=True, errors="coerce")

    side_col = "side" if "side" in raw.columns else None
    side = (
        raw[side_col].map(_normalize_side)
        if side_col
        else pd.Series(["UNKNOWN"] * len(raw), index=raw.index)
    )

    price = _pick_first_numeric(
        raw, ["average_price", "price", "avgprice", "avg_price"]
    )
    qty = _pick_first_numeric(
        raw,
        [
            "last_fill_quantity",
            "accumulated_fill_quantity",
            "original_quantity",
            "qty",
            "quantity",
        ],
    )
    notional = _pick_first_numeric(raw, ["notional", "quote_qty", "quote_quantity"])

    # Compute USD proxy notional if explicit notional is unavailable.
    computed_notional = (qty.astype(float) * price.astype(float)).abs()
    computed_notional = pd.Series(computed_notional, index=raw.index)
    notional = notional.fillna(computed_notional)

    out = pd.DataFrame(
        {
            "ts": ts,
            "symbol": str(symbol).upper(),
            "side": side,
            "price": price,
            "qty": qty,
            "notional": notional,
            "source": source,
        }
    )

    out = out.dropna(subset=["ts", "notional"]).copy()
    if out.empty:
        return pd.DataFrame(
            columns=["ts", "symbol", "side", "price", "qty", "notional", "source"]
        )

    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out["qty"] = pd.to_numeric(out["qty"], errors="coerce")
    out["notional"] = pd.to_numeric(out["notional"], errors="coerce").abs()
    out = out.dropna(subset=["notional"]).copy()

    ensure_utc_timestamp(out["ts"], "ts")
    out = (
        out.sort_values("ts")
        .drop_duplicates(subset=["ts", "side", "price", "qty", "notional"])
        .reset_index(drop=True)
    )
    return out[["ts", "symbol", "side", "price", "qty", "notional", "source"]]


def _list_available_day_set(
    cm_contract: str,
    list_base: str,
    session: requests.Session,
    *,
    timeout_sec: int,
    max_retries: int,
    retry_backoff_sec: float,
) -> Set[str]:
    prefix = f"data/futures/cm/daily/liquidationSnapshot/{cm_contract}/{cm_contract}-liquidationSnapshot-"
    marker: str | None = None
    out: Set[str] = set()
    key_re = re.compile(
        rf"{re.escape(cm_contract)}-liquidationSnapshot-(\d{{4}}-\d{{2}}-\d{{2}})\.zip$"
    )

    while True:
        params: Dict[str, str] = {"prefix": prefix, "max-keys": "1000"}
        if marker:
            params["marker"] = marker

        payload_text: str | None = None
        last_error = ""
        for attempt in range(max_retries + 1):
            try:
                resp = session.get(list_base, params=params, timeout=timeout_sec)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:400]}")
                if resp.status_code != 200:
                    raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:400]}")
                payload_text = resp.text
                break
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                if attempt >= max_retries:
                    raise RuntimeError(
                        f"Failed listing archive keys for {cm_contract}: {last_error}"
                    )
                time.sleep(retry_backoff_sec * (2**attempt))

        if payload_text is None:
            raise RuntimeError(
                f"Failed listing archive keys for {cm_contract}: {last_error}"
            )

        root = ET.fromstring(payload_text)
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

        contents = root.findall("s3:Contents", ns)
        for item in contents:
            key_el = item.find("s3:Key", ns)
            if key_el is None or not key_el.text:
                continue
            m = key_re.search(key_el.text)
            if m:
                out.add(m.group(1))

        is_truncated = (
            root.findtext("s3:IsTruncated", default="false", namespaces=ns) or ""
        ).lower() == "true"
        if not is_truncated:
            break

        next_marker = root.findtext("s3:NextMarker", default="", namespaces=ns)
        if next_marker:
            marker = next_marker
        elif contents:
            tail_key = contents[-1].find("s3:Key", ns)
            marker = tail_key.text if tail_key is not None else None
        else:
            break

    return out


def _fetch_symbol_binance_daily(
    session: requests.Session,
    *,
    requested_symbol: str,
    cm_contract: str,
    start: datetime,
    end: datetime,
    archive_base: str,
    archive_list_base: str,
    prelist_available: int,
    timeout_sec: int,
    max_retries: int,
    retry_backoff_sec: float,
) -> Tuple[pd.DataFrame, Dict[str, int], List[str]]:
    end_exclusive = (end + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    available_days: Set[str] = set()
    if int(prelist_available):
        available_days = _list_available_day_set(
            cm_contract=cm_contract,
            list_base=archive_list_base,
            session=session,
            timeout_sec=timeout_sec,
            max_retries=max_retries,
            retry_backoff_sec=retry_backoff_sec,
        )

    frames: List[pd.DataFrame] = []
    missing_files: List[str] = []
    stats = {
        "requested_days": 0,
        "listed_days": int(len(available_days)),
        "days_considered": 0,
        "downloads_ok": 0,
        "downloads_not_found": 0,
        "parsed_rows": 0,
    }

    for day in _iter_days(start, end_exclusive):
        stats["requested_days"] += 1
        day_str = day.strftime("%Y-%m-%d")

        if available_days and day_str not in available_days:
            continue

        stats["days_considered"] += 1
        filename = f"{cm_contract}-liquidationSnapshot-{day_str}.zip"
        url = join_url(
            archive_base, "daily", "liquidationSnapshot", cm_contract, filename
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            out_zip = Path(tmpdir) / filename
            result = download_with_retries(
                url,
                out_zip,
                max_retries=max_retries,
                backoff_sec=retry_backoff_sec,
                timeout=timeout_sec,
                session=session,
            )

            if result.status == "ok":
                parsed = _parse_liquidation_from_zip(out_zip, symbol=requested_symbol)
                if not parsed.empty:
                    frames.append(parsed)
                stats["downloads_ok"] += 1
                stats["parsed_rows"] += int(len(parsed))
            elif result.status == "not_found":
                stats["downloads_not_found"] += 1
                missing_files.append(url)
            else:
                raise RuntimeError(f"Failed downloading {url}: {result.error}")

    data = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(
            columns=["ts", "symbol", "side", "price", "qty", "notional", "source"]
        )
    )
    if not data.empty:
        data = data[(data["ts"] >= start) & (data["ts"] < end_exclusive)].copy()
        data = (
            data.sort_values("ts")
            .drop_duplicates(subset=["ts", "side", "price", "qty", "notional"])
            .reset_index(drop=True)
        )
    return data, stats, missing_files


def _partition_has_rows(path: Path) -> bool:
    target = path
    if not target.exists():
        csv_path = path.with_suffix(".csv")
        if csv_path.exists():
            target = csv_path
        else:
            return False
    try:
        df = read_parquet([target]) if target.suffix != ".csv" else pd.read_csv(target)
    except Exception:
        return False
    return int(len(df)) > 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ingest liquidation snapshots from Binance Data Vision CM daily archives"
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--archive_base", default=ARCHIVE_BASE)
    parser.add_argument("--archive_list_base", default=ARCHIVE_LIST_BASE)
    parser.add_argument("--prelist_available", type=int, default=1)
    parser.add_argument("--max_retries", type=int, default=5)
    parser.add_argument("--retry_backoff_sec", type=float, default=2.0)
    parser.add_argument("--request_timeout_sec", type=int, default=30)
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--fail_if_no_data", type=int, default=1)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(
        level=logging.INFO,
        handlers=log_handlers,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    run_id = args.run_id
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    if end < start:
        raise ValueError("end must be >= start")

    params = {
        "run_id": run_id,
        "symbols": symbols,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "provider": "binance_data_vision_cm",
        "archive_base": str(args.archive_base),
        "archive_list_base": str(args.archive_list_base),
        "prelist_available": int(args.prelist_available),
        "max_retries": int(args.max_retries),
        "retry_backoff_sec": float(args.retry_backoff_sec),
        "request_timeout_sec": int(args.request_timeout_sec),
        "force": int(args.force),
        "fail_if_no_data": int(args.fail_if_no_data),
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest(
        "ingest_binance_um_liquidation_snapshot", run_id, params, inputs, outputs
    )

    stats: Dict[str, object] = {"provider": "binance_data_vision_cm", "symbols": {}}

    try:
        session = requests.Session()
        total_rows = 0
        total_written_parts = 0

        for symbol in symbols:
            cm_contract = _to_cm_contract(symbol)
            data, fetch_stats, missing_files = _fetch_symbol_binance_daily(
                session=session,
                requested_symbol=symbol,
                cm_contract=cm_contract,
                start=start,
                end=end,
                archive_base=args.archive_base,
                archive_list_base=args.archive_list_base,
                prelist_available=int(args.prelist_available),
                timeout_sec=int(args.request_timeout_sec),
                max_retries=int(args.max_retries),
                retry_backoff_sec=float(args.retry_backoff_sec),
            )

            written_rows = 0
            written_parts = 0
            for month_start in _iter_months(start, end):
                month_end = _next_month(month_start)
                if data.empty:
                    part = pd.DataFrame(
                        columns=[
                            "ts",
                            "symbol",
                            "side",
                            "price",
                            "qty",
                            "notional",
                            "source",
                        ]
                    )
                else:
                    part = data[
                        (data["ts"] >= month_start) & (data["ts"] < month_end)
                    ].copy()

                if part.empty and not int(args.force):
                    continue

                out_dir = (
                    DATA_ROOT
                    / "lake"
                    / "raw"
                    / "binance"
                    / "perp"
                    / symbol
                    / "liquidation_snapshot"
                    / f"year={month_start.year}"
                    / f"month={month_start.month:02d}"
                )
                out_path = (
                    out_dir
                    / f"liquidation_snapshot_{symbol}_{month_start.year}-{month_start.month:02d}.parquet"
                )
                if not int(args.force) and _partition_has_rows(out_path):
                    continue
                path_written, storage = write_parquet(part, out_path)
                outputs.append(
                    {
                        "path": str(path_written),
                        "rows": int(len(part)),
                        "storage": storage,
                    }
                )
                written_rows += int(len(part))
                written_parts += 1

            logging.info(
                "Liquidation ingest symbol=%s cm_contract=%s provider=binance_data_vision_cm rows=%d written_rows=%d written_partitions=%d requested_days=%d considered_days=%d downloads_ok=%d not_found=%d parsed_rows=%d",
                symbol,
                cm_contract,
                int(len(data)),
                int(written_rows),
                int(written_parts),
                int(fetch_stats.get("requested_days", 0)),
                int(fetch_stats.get("days_considered", 0)),
                int(fetch_stats.get("downloads_ok", 0)),
                int(fetch_stats.get("downloads_not_found", 0)),
                int(fetch_stats.get("parsed_rows", 0)),
            )

            stats["symbols"][symbol] = {
                "cm_contract": cm_contract,
                "rows": int(len(data)),
                "written_rows": int(written_rows),
                "written_partitions": int(written_parts),
                "requested_days": int(fetch_stats.get("requested_days", 0)),
                "listed_days": int(fetch_stats.get("listed_days", 0)),
                "days_considered": int(fetch_stats.get("days_considered", 0)),
                "downloads_ok": int(fetch_stats.get("downloads_ok", 0)),
                "downloads_not_found": int(fetch_stats.get("downloads_not_found", 0)),
                "parsed_rows": int(fetch_stats.get("parsed_rows", 0)),
                "missing_archive_files": missing_files,
            }

            total_rows += int(len(data))
            total_written_parts += int(written_parts)

        if int(args.fail_if_no_data) and total_rows == 0 and total_written_parts == 0:
            raise RuntimeError(
                "No liquidation rows were ingested from Binance Data Vision CM archives. "
                "Check symbol mapping (UM->CM), date range, and archive availability. "
                "Re-run with --fail_if_no_data 0 to treat this stage as optional."
            )

        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:  # pragma: no cover
        logging.exception("Liquidation snapshot ingest failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
