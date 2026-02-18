from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from features.funding_persistence import DEFAULT_FP_CONFIG, build_funding_persistence_state
from pipelines._lib.io_utils import (
    choose_partition_dir,
    ensure_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
    write_parquet,
)
from pipelines._lib.run_manifest import finalize_manifest, schema_hash_from_columns, start_manifest, validate_input_provenance


def _collect_stats(df: pd.DataFrame) -> Dict[str, object]:
    if df.empty:
        return {"rows": 0, "start_ts": None, "end_ts": None}
    return {
        "rows": int(len(df)),
        "start_ts": df["timestamp"].min().isoformat(),
        "end_ts": df["timestamp"].max().isoformat(),
    }


def _dedupe_timestamp_rows(df: pd.DataFrame, label: str) -> tuple[pd.DataFrame, int]:
    if "timestamp" not in df.columns or df.empty:
        return df, 0
    out = df.sort_values("timestamp").copy()
    dupes = int(out["timestamp"].duplicated(keep="last").sum())
    if dupes > 0:
        logging.warning("Dropping %s duplicate timestamp rows for %s (keeping last).", dupes, label)
        out = out.drop_duplicates(subset=["timestamp"], keep="last")
    return out.reset_index(drop=True), dupes


def main() -> int:
    parser = argparse.ArgumentParser(description="Build read-only context features")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--timeframe", default="15m")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    start = pd.Timestamp(args.start, tz="UTC").floor("D")
    end_exclusive = pd.Timestamp(args.end, tz="UTC").floor("D") + pd.Timedelta(days=1)

    output_root = (
        Path(args.out_dir)
        if args.out_dir
        else run_scoped_lake_path(DATA_ROOT, args.run_id, "context", "funding_persistence")
    )

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    params = {
        "symbols": symbols,
        "timeframe": args.timeframe,
        "start": start.isoformat(),
        "end": pd.Timestamp(args.end, tz="UTC").isoformat(),
        "end_exclusive": end_exclusive.isoformat(),
        "out_dir": str(output_root),
        "force": int(args.force),
        "fp_def_version": DEFAULT_FP_CONFIG.def_version,
    }
    manifest = start_manifest("build_context_features", args.run_id, params, inputs, outputs)
    stats: Dict[str, object] = {"symbols": {}}

    try:
        def _skip_existing(existing_df: pd.DataFrame, expected: pd.DataFrame) -> bool:
            if existing_df.empty or len(existing_df) != len(expected):
                return False
            if "timestamp" not in existing_df.columns or "fp_def_version" not in existing_df.columns:
                return False
            existing_ts = pd.to_datetime(existing_df["timestamp"], utc=True, errors="coerce")
            expected_ts = pd.to_datetime(expected["timestamp"], utc=True, errors="coerce")
            if existing_ts.isna().any() or expected_ts.isna().any():
                return False
            if list(existing_ts) != list(expected_ts):
                return False
            versions = set(existing_df["fp_def_version"].astype(str).dropna().unique().tolist())
            return versions == {DEFAULT_FP_CONFIG.def_version}

        for symbol in symbols:
            bars_candidates = [
                run_scoped_lake_path(DATA_ROOT, args.run_id, "cleaned", "perp", symbol, f"bars_{args.timeframe}"),
                DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / f"bars_{args.timeframe}",
            ]
            bars_dir = choose_partition_dir(bars_candidates)
            bars = read_parquet(list_parquet_files(bars_dir)) if bars_dir else pd.DataFrame()
            if bars.empty:
                raise ValueError(f"No cleaned bars found for {symbol} at timeframe={args.timeframe}")
            bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
            bars, bars_dupes = _dedupe_timestamp_rows(bars, label=f"bars:{symbol}:{args.timeframe}")
            bars = bars[(bars["timestamp"] >= start) & (bars["timestamp"] < end_exclusive)].copy()
            if bars.empty:
                raise ValueError(f"No bars in requested range for {symbol}")

            if "funding_rate_scaled" not in bars.columns:
                funding_candidates = [
                    run_scoped_lake_path(
                        DATA_ROOT,
                        args.run_id,
                        "cleaned",
                        "perp",
                        symbol,
                        f"funding_{args.timeframe}",
                    ),
                    DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / f"funding_{args.timeframe}",
                ]
                funding_dir = choose_partition_dir(funding_candidates)
                funding = read_parquet(list_parquet_files(funding_dir)) if funding_dir else pd.DataFrame()
                if funding.empty:
                    raise ValueError(f"Missing funding_rate_scaled for {symbol} at timeframe={args.timeframe}")
                funding["timestamp"] = pd.to_datetime(funding["timestamp"], utc=True)
                funding = (
                    funding[["timestamp", "funding_rate_scaled"]]
                    .dropna(subset=["timestamp"])
                )
                funding, funding_dupes = _dedupe_timestamp_rows(
                    funding, label=f"funding:{symbol}:{args.timeframe}"
                )
                bars = bars.merge(funding, on="timestamp", how="left", validate="one_to_one")
                bars["funding_rate_scaled"] = pd.to_numeric(bars["funding_rate_scaled"], errors="coerce")
                if bars["funding_rate_scaled"].isna().all():
                    raise ValueError(f"Unable to align funding_rate_scaled for {symbol}")
                bars["funding_rate_scaled"] = bars["funding_rate_scaled"].ffill().fillna(0.0)
            else:
                funding_dupes = 0

            inputs.append(
                {
                    "path": str(bars_dir),
                    **_collect_stats(bars),
                    "provenance": {
                        "vendor": "binance",
                        "exchange": "binance",
                        "schema_version": "cleaned_bars_15m_v1",
                        "schema_hash": schema_hash_from_columns(bars.columns.tolist()),
                        "extraction_start": bars["timestamp"].min().isoformat(),
                        "extraction_end": bars["timestamp"].max().isoformat(),
                    },
                }
            )
            if "funding_rate_scaled" in bars.columns:
                funding_non_null = bars[["timestamp", "funding_rate_scaled"]].dropna(subset=["funding_rate_scaled"]).copy()
                if not funding_non_null.empty:
                    inputs.append(
                        {
                            "path": str(bars_dir),
                            **_collect_stats(funding_non_null),
                            "provenance": {
                                "vendor": "binance",
                                "exchange": "binance",
                                "schema_version": "funding_15m_v1",
                                "schema_hash": schema_hash_from_columns(funding_non_null.columns.tolist()),
                                "extraction_start": funding_non_null["timestamp"].min().isoformat(),
                                "extraction_end": funding_non_null["timestamp"].max().isoformat(),
                            },
                        }
                    )
            fp = build_funding_persistence_state(bars, symbol=symbol, config=DEFAULT_FP_CONFIG)

            out_path = output_root / symbol / f"{args.timeframe}.parquet"
            if out_path.exists() and not args.force:
                existing = read_parquet([out_path])
                if _skip_existing(existing, fp):
                    logging.info("Skipping existing context file for %s: %s", symbol, out_path)
                    outputs.append({"path": str(out_path), "rows": int(len(existing)), "storage_format": out_path.suffix})
                    continue

            written_path, storage = write_parquet(fp, out_path)
            outputs.append({"path": str(written_path), "rows": int(len(fp)), "storage_format": storage})
            stats["symbols"][symbol] = {
                "rows": int(len(fp)),
                "active_bars": int(fp["fp_active"].sum()),
                "event_count": int(fp["fp_event_id"].dropna().nunique()),
                "fp_def_version": DEFAULT_FP_CONFIG.def_version,
                "bars_duplicate_rows_dropped": int(bars_dupes),
                "funding_duplicate_rows_dropped": int(funding_dupes),
            }

        validate_input_provenance(inputs)
        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Context feature build failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
