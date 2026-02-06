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
from pipelines._lib.io_utils import ensure_dir, list_parquet_files, read_parquet, write_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest


def _collect_stats(df: pd.DataFrame) -> Dict[str, object]:
    if df.empty:
        return {"rows": 0, "start_ts": None, "end_ts": None}
    return {
        "rows": int(len(df)),
        "start_ts": df["timestamp"].min().isoformat(),
        "end_ts": df["timestamp"].max().isoformat(),
    }


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
    start = pd.Timestamp(args.start, tz="UTC")
    end = pd.Timestamp(args.end, tz="UTC")

    output_root = (
        Path(args.out_dir)
        if args.out_dir
        else DATA_ROOT / "features" / "context" / "funding_persistence"
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
        "end": end.isoformat(),
        "out_dir": str(output_root),
        "force": int(args.force),
        "fp_def_version": DEFAULT_FP_CONFIG.def_version,
    }
    manifest = start_manifest("build_context_features", args.run_id, params, inputs, outputs)
    stats: Dict[str, object] = {
        "fp_def_version": DEFAULT_FP_CONFIG.def_version,
        "timeframe": args.timeframe,
        "output_files": [],
        "symbols": {},
    }

    try:
        for symbol in symbols:
            bars_dir = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / f"bars_{args.timeframe}"
            bars = read_parquet(list_parquet_files(bars_dir))
            if bars.empty:
                raise ValueError(f"No cleaned bars found for {symbol} at timeframe={args.timeframe}")
            bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
            bars = bars.sort_values("timestamp").reset_index(drop=True)
            bars = bars[(bars["timestamp"] >= start) & (bars["timestamp"] <= end)].copy()
            if bars.empty:
                raise ValueError(f"No bars in requested range for {symbol}")

            if "funding_rate_scaled" not in bars.columns:
                funding_dir = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / f"funding_{args.timeframe}"
                funding = read_parquet(list_parquet_files(funding_dir))
                if funding.empty:
                    raise ValueError(f"No cleaned funding found for {symbol} at timeframe={args.timeframe}")
                funding["timestamp"] = pd.to_datetime(funding["timestamp"], utc=True)
                funding = funding.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last")
                funding = funding[(funding["timestamp"] >= start) & (funding["timestamp"] <= end)].copy()
                if funding.empty:
                    raise ValueError(f"No funding rows in requested range for {symbol}")
                bars = bars.merge(
                    funding[["timestamp", "funding_rate_scaled"]],
                    on="timestamp",
                    how="left",
                )
                if bars["funding_rate_scaled"].isna().all():
                    raise ValueError(f"Unable to align funding_rate_scaled onto bars for {symbol}")

            inputs.append({"path": str(bars_dir), **_collect_stats(bars)})
            fp = build_funding_persistence_state(bars, symbol=symbol, config=DEFAULT_FP_CONFIG)

            out_path = output_root / symbol / f"{args.timeframe}.parquet"
            output_file = str(out_path)
            if out_path.exists() and not args.force:
                existing = read_parquet([out_path])
                if len(existing) == len(fp):
                    logging.info("Skipping existing context file for %s: %s", symbol, out_path)
                    outputs.append(
                        {
                            "path": output_file,
                            "symbol": symbol,
                            "timeframe": args.timeframe,
                            "fp_def_version": DEFAULT_FP_CONFIG.def_version,
                            "rows": int(len(existing)),
                            "active_bars": int(fp["fp_active"].sum()),
                            "event_count": int(fp["fp_event_id"].dropna().nunique()),
                            "storage_format": out_path.suffix,
                        }
                    )
                    stats["output_files"].append(output_file)
                    stats["symbols"][symbol] = {
                        "symbol": symbol,
                        "timeframe": args.timeframe,
                        "output_file": output_file,
                        "rows": int(len(existing)),
                        "active_bars": int(fp["fp_active"].sum()),
                        "event_count": int(fp["fp_event_id"].dropna().nunique()),
                        "fp_def_version": DEFAULT_FP_CONFIG.def_version,
                    }
                    continue

            written_path, storage = write_parquet(fp, out_path)
            output_file = str(written_path)
            outputs.append(
                {
                    "path": output_file,
                    "symbol": symbol,
                    "timeframe": args.timeframe,
                    "fp_def_version": DEFAULT_FP_CONFIG.def_version,
                    "rows": int(len(fp)),
                    "active_bars": int(fp["fp_active"].sum()),
                    "event_count": int(fp["fp_event_id"].dropna().nunique()),
                    "storage_format": storage,
                }
            )
            stats["output_files"].append(output_file)
            stats["symbols"][symbol] = {
                "symbol": symbol,
                "timeframe": args.timeframe,
                "output_file": output_file,
                "rows": int(len(fp)),
                "active_bars": int(fp["fp_active"].sum()),
                "event_count": int(fp["fp_event_id"].dropna().nunique()),
                "fp_def_version": DEFAULT_FP_CONFIG.def_version,
            }

        stats["output_files"] = sorted(set(stats["output_files"]))
        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Context feature build failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
