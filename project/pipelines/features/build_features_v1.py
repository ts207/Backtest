from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import list_parquet_files, read_parquet, write_parquet
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Build features v1")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    args = parser.parse_args()

    run_id = args.run_id
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    manifest = start_manifest(run_id, "build_features_v1", ["project/configs/pipeline.yaml"])
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []

    try:
        for symbol in symbols:
            cleaned_dir = Path("project") / "lake" / "cleaned" / "perp" / symbol / "bars_15m"
            cleaned_files = list_parquet_files(cleaned_dir)
            bars = read_parquet(cleaned_files)
            if bars.empty:
                raise ValueError(f"No cleaned bars for {symbol}")

            validate_columns(bars, ["timestamp", "open", "high", "low", "close"])
            bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
            ensure_utc_timestamp(bars["timestamp"], "timestamp")
            bars = bars.sort_values("timestamp").reset_index(drop=True)

            inputs.append({"path": str(cleaned_dir), **_collect_stats(bars)})

            features = bars[["timestamp", "open", "high", "low", "close"]].copy()
            features["logret_1"] = np.log(features["close"]).diff()
            features["rv_96"] = features["logret_1"].rolling(window=96, min_periods=96).std()
            features["rv_pct_17280"] = _rolling_percentile(features["rv_96"], window=17280)
            features["high_96"] = features["high"].rolling(window=96, min_periods=96).max()
            features["low_96"] = features["low"].rolling(window=96, min_periods=96).min()
            features["range_96"] = features["high_96"] - features["low_96"]
            features["range_med_2880"] = features["range_96"].rolling(window=2880, min_periods=2880).median()

            features["year"] = features["timestamp"].dt.year
            features["month"] = features["timestamp"].dt.month
            out_dir = Path("project") / "lake" / "features" / "perp" / symbol / "15m" / "features_v1"

            for (year, month), group in features.groupby(["year", "month"], sort=True):
                group_out = group.drop(columns=["year", "month"]).reset_index(drop=True)
                out_path = out_dir / f"year={year}" / f"month={month:02d}" / f"features_{year}_{month:02d}.parquet"
                write_parquet(group_out, out_path)
                outputs.append({"path": str(out_path), **_collect_stats(group_out)})

        finalize_manifest(manifest, inputs, outputs, "success")
        return 0
    except Exception as exc:
        finalize_manifest(manifest, inputs, outputs, "failed", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
