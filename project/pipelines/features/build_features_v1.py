from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.config import load_configs
from pipelines._lib.io_utils import ensure_dir, list_parquet_files, read_parquet, write_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.validation import ensure_utc_timestamp, validate_columns


def _collect_stats(df: pd.DataFrame) -> Dict[str, object]:
    if df.empty:
        return {"rows": 0, "start_ts": None, "end_ts": None}
    return {
        "rows": int(len(df)),
        "start_ts": df["timestamp"].min().isoformat(),
        "end_ts": df["timestamp"].max().isoformat(),
    }


def _rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    def pct_rank(values: np.ndarray) -> float:
        last = values[-1]
        return float(np.sum(values <= last) / len(values) * 100.0)

    return series.rolling(window=window, min_periods=window).apply(pct_rank, raw=True)


def _partition_complete(path: Path, expected_rows: int) -> bool:
    if not path.exists():
        csv_path = path.with_suffix(".csv")
        if csv_path.exists():
            path = csv_path
        else:
            return False
    try:
        if expected_rows == 0:
            return True
        data = read_parquet([path])
        return len(data) >= expected_rows
    except Exception:
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Build features v1")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--market", choices=["perp", "spot"], default="perp")
    parser.add_argument("--force", type=int, default=0)
    # Compatibility flag from run_all; feature build already tolerates missing funding.
    parser.add_argument("--allow_missing_funding", type=int, default=0)
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    run_id = args.run_id
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    market = str(args.market).strip().lower()

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    config_paths = [str(PROJECT_ROOT / "configs" / "pipeline.yaml")]
    config_paths.extend(args.config)
    config = load_configs(config_paths)

    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    params = {
        "symbols": symbols,
        "market": market,
        "trade_day_timezone": config.get("trade_day_timezone", "UTC"),
        "force": int(args.force),
        "allow_missing_funding": int(args.allow_missing_funding),
    }
    manifest = start_manifest("build_features_v1", run_id, params, inputs, outputs)
    stats: Dict[str, object] = {"symbols": {}}

    try:
        for symbol in symbols:
            cleaned_dir = DATA_ROOT / "lake" / "cleaned" / market / symbol / "bars_15m"
            funding_dir = DATA_ROOT / "lake" / "cleaned" / market / symbol / "funding_15m"
            cleaned_files = list_parquet_files(cleaned_dir)
            funding_files = list_parquet_files(funding_dir)
            bars = read_parquet(cleaned_files)
            funding = read_parquet(funding_files) if market == "perp" else pd.DataFrame()
            if bars.empty:
                raise ValueError(f"No cleaned bars for {symbol}")

            validate_columns(bars, ["timestamp", "open", "high", "low", "close"])
            bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
            ensure_utc_timestamp(bars["timestamp"], "timestamp")
            bars = bars.sort_values("timestamp").reset_index(drop=True)

            inputs.append({"path": str(cleaned_dir), **_collect_stats(bars)})
            if market == "perp" and not funding.empty:
                inputs.append({"path": str(funding_dir), **_collect_stats(funding)})

            if market == "perp" and not funding.empty:
                funding["timestamp"] = pd.to_datetime(funding["timestamp"], utc=True)
                funding = funding.sort_values("timestamp").reset_index(drop=True)
                if "funding_rate_scaled" in funding.columns:
                    rate_col = "funding_rate_scaled"
                elif "funding_rate" in funding.columns:
                    rate_col = "funding_rate"
                else:
                    raise ValueError(
                        f"Funding schema for {symbol} missing rate column; expected funding_rate_scaled or funding_rate."
                    )
                funding_rates = funding[["timestamp", rate_col]].rename(columns={rate_col: "funding_rate_scaled"})
                bars = bars.merge(funding_rates, on="timestamp", how="left")
            else:
                bars["funding_rate_scaled"] = 0.0

            # Backward-compatible alias for any code still reading funding_rate.
            bars["funding_rate"] = bars["funding_rate_scaled"]

            features = bars[["timestamp", "open", "high", "low", "close", "funding_rate_scaled", "funding_rate"]].copy()
            features["logret_1"] = np.log(features["close"]).diff()
            features["rv_96"] = features["logret_1"].rolling(window=96, min_periods=96).std()
            features["rv_pct_17280"] = _rolling_percentile(features["rv_96"], window=17280)
            features["high_96"] = features["high"].rolling(window=96, min_periods=96).max()
            features["low_96"] = features["low"].rolling(window=96, min_periods=96).min()
            features["range_96"] = features["high_96"] - features["low_96"]
            features["range_med_2880"] = features["range_96"].rolling(window=2880, min_periods=2880).median()

            features["year"] = features["timestamp"].dt.year
            features["month"] = features["timestamp"].dt.month
            out_dir = DATA_ROOT / "lake" / "features" / market / symbol / "15m" / "features_v1"

            partitions_written: List[str] = []
            partitions_skipped: List[str] = []

            for (year, month), group in features.groupby(["year", "month"], sort=True):
                group_out = group.drop(columns=["year", "month"]).reset_index(drop=True)
                out_path = out_dir / f"year={year}" / f"month={month:02d}" / f"features_{symbol}_v1_{year}-{month:02d}.parquet"
                expected_rows = len(group_out)
                if not args.force and _partition_complete(out_path, expected_rows):
                    partitions_skipped.append(str(out_path))
                    continue
                ensure_dir(out_path.parent)
                written_path, storage = write_parquet(group_out, out_path)
                outputs.append({"path": str(written_path), **_collect_stats(group_out), "storage": storage})
                partitions_written.append(str(written_path))

            stats["symbols"][symbol] = {
                "rows_written": int(len(features)),
                "coverage_start": features["timestamp"].min().isoformat(),
                "coverage_end": features["timestamp"].max().isoformat(),
                "partitions_written": partitions_written,
                "partitions_skipped": partitions_skipped,
            }

        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Feature build failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
