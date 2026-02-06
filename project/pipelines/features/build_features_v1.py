from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import timedelta, timezone
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.config import load_configs
from pipelines._lib.io_utils import (
    choose_partition_dir,
    ensure_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
    write_parquet,
)
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.validation import ensure_utc_timestamp, validate_columns

DEFAULT_WINDOWS = {
    "rv": 96,
    "rv_pct": 2880,
    "range": 96,
    "range_med": 480,
}


def _collect_stats(df: pd.DataFrame) -> Dict[str, object]:
    if df.empty:
        return {"rows": 0, "start_ts": None, "end_ts": None}
    return {
        "rows": int(len(df)),
        "start_ts": df["timestamp"].min().isoformat(),
        "end_ts": df["timestamp"].max().isoformat(),
    }


def _expected_15m_index(start: pd.Timestamp, end_exclusive: pd.Timestamp) -> pd.DatetimeIndex:
    if end_exclusive <= start:
        return pd.DatetimeIndex([], tz=timezone.utc)
    return pd.date_range(
        start=start,
        end=end_exclusive - timedelta(minutes=15),
        freq="15min",
        tz=timezone.utc,
    )


def _rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    def pct_rank(values: np.ndarray) -> float:
        last = values[-1]
        return float(np.sum(values <= last) / len(values) * 100.0)

    return series.rolling(window=window, min_periods=window).apply(pct_rank, raw=True)


def _compute_segment_id(is_gap: pd.Series) -> pd.Series:
    gap_series = is_gap.fillna(True)
    valid = ~gap_series
    segment_start = valid & ~valid.shift(fill_value=False)
    segment_id = segment_start.cumsum()
    return segment_id.where(valid)


def _segment_rolling(
    series: pd.Series, segment_id: pd.Series, window: int, min_periods: int, func: str
) -> pd.Series:
    grouped = series.groupby(segment_id, sort=False)
    rolled = grouped.rolling(window=window, min_periods=min_periods)
    if func == "std":
        result = rolled.std()
    elif func == "max":
        result = rolled.max()
    elif func == "min":
        result = rolled.min()
    elif func == "median":
        result = rolled.median()
    else:
        raise ValueError(f"Unsupported rolling function: {func}")
    return result.reset_index(level=0, drop=True)


def _segment_rolling_percentile(series: pd.Series, segment_id: pd.Series, window: int) -> pd.Series:
    def pct_rank(values: np.ndarray) -> float:
        last = values[-1]
        return float(np.sum(values <= last) / len(values) * 100.0)

    grouped = series.groupby(segment_id, sort=False)
    rolled = grouped.rolling(window=window, min_periods=window).apply(pct_rank, raw=True)
    return rolled.reset_index(level=0, drop=True)


def _build_features_frame(
    bars: pd.DataFrame, windows: Dict[str, int] | None = None
) -> tuple[pd.DataFrame, pd.Series, Dict[str, float], Dict[str, str]]:
    windows = windows or DEFAULT_WINDOWS
    rv_pct_window = windows["rv_pct"]
    range_med_window = windows["range_med"]
    rv_pct_col = f"rv_pct_{rv_pct_window}"
    range_med_col = f"range_med_{range_med_window}"
    is_gap = bars.get("is_gap", pd.Series(False, index=bars.index))
    segment_id = _compute_segment_id(is_gap)

    close = bars["close"].astype(float)
    log_close = np.log(close)
    logret_1 = log_close.groupby(segment_id, sort=False).diff()
    ret_1 = (
        close.groupby(segment_id, sort=False)
        .apply(lambda s: s.pct_change(fill_method=None))
        .reset_index(level=0, drop=True)
    )

    rv_96 = _segment_rolling(logret_1, segment_id, windows["rv"], windows["rv"], "std")
    high_96 = _segment_rolling(bars["high"].astype(float), segment_id, windows["range"], windows["range"], "max")
    low_96 = _segment_rolling(bars["low"].astype(float), segment_id, windows["range"], windows["range"], "min")
    range_96 = high_96 - low_96
    range_med = _segment_rolling(range_96, segment_id, windows["range_med"], windows["range_med"], "median")
    rv_pct = _segment_rolling_percentile(rv_96, segment_id, windows["rv_pct"])

    features = bars[
        ["timestamp", "open", "high", "low", "close", "funding_event_ts", "funding_rate_scaled"]
    ].copy()
    features["ret_1"] = ret_1
    features["logret_1"] = logret_1
    features["rv_96"] = rv_96
    features[rv_pct_col] = rv_pct
    features["high_96"] = high_96
    features["low_96"] = low_96
    features["range_96"] = range_96
    features[range_med_col] = range_med

    nan_rates = {
        "ret_1": float(features["ret_1"].isna().mean()),
        "logret_1": float(features["logret_1"].isna().mean()),
        "rv_96": float(features["rv_96"].isna().mean()),
        rv_pct_col: float(features[rv_pct_col].isna().mean()),
        range_med_col: float(features[range_med_col].isna().mean()),
    }
    column_map = {
        "rv_pct": rv_pct_col,
        "range_med": range_med_col,
    }

    return features, segment_id, nan_rates, column_map


def _partition_complete(path: Path, expected_ts: pd.DatetimeIndex) -> bool:
    if not path.exists():
        csv_path = path.with_suffix(".csv")
        if csv_path.exists():
            path = csv_path
        else:
            return False
    try:
        if len(expected_ts) == 0:
            return True
        data = read_parquet([path])
        if data.empty:
            return False
        if "timestamp" not in data.columns:
            return False
        ts = pd.to_datetime(data["timestamp"], utc=True, format="mixed")
        if ts.isna().any():
            return False
        if ts.duplicated().any():
            return False
        return set(ts) == set(expected_ts)
    except Exception:
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Build features v1")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--allow_missing_funding", type=int, default=0)
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    run_id = args.run_id
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

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
        "trade_day_timezone": config.get("trade_day_timezone", "UTC"),
        "force": int(args.force),
        "allow_missing_funding": int(args.allow_missing_funding),
    }
    manifest = start_manifest("build_features_v1", run_id, params, inputs, outputs)
    stats: Dict[str, object] = {"symbols": {}}

    try:
        for symbol in symbols:
            cleaned_candidates = [
                run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", "perp", symbol, "bars_15m"),
                DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "bars_15m",
            ]
            funding_candidates = [
                run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", "perp", symbol, "funding_15m"),
                DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "funding_15m",
            ]
            cleaned_dir = choose_partition_dir(cleaned_candidates)
            funding_dir = choose_partition_dir(funding_candidates)
            cleaned_files = list_parquet_files(cleaned_dir) if cleaned_dir else []
            funding_files = list_parquet_files(funding_dir) if funding_dir else []
            bars = read_parquet(cleaned_files)
            funding = read_parquet(funding_files)
            if bars.empty:
                raise ValueError(f"No cleaned bars for {symbol}")

            validate_columns(bars, ["timestamp", "open", "high", "low", "close"])
            bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
            ensure_utc_timestamp(bars["timestamp"], "timestamp")
            bars = bars.sort_values("timestamp").reset_index(drop=True)

            inputs.append({"path": str(cleaned_dir) if cleaned_dir else str(cleaned_candidates[0]), **_collect_stats(bars)})
            if not funding.empty:
                inputs.append({"path": str(funding_dir) if funding_dir else str(funding_candidates[0]), **_collect_stats(funding)})

            if funding.empty:
                if not args.allow_missing_funding:
                    raise ValueError(
                        f"No cleaned funding data for {symbol}. Use --allow_missing_funding=1 to proceed."
                    )
                bars["funding_event_ts"] = pd.NaT
                bars["funding_rate_scaled"] = np.nan
            else:
                validate_columns(funding, ["timestamp", "funding_event_ts", "funding_rate_scaled"])
                funding["timestamp"] = pd.to_datetime(funding["timestamp"], utc=True)
                funding["funding_event_ts"] = pd.to_datetime(funding["funding_event_ts"], utc=True)
                funding = funding.sort_values("timestamp").reset_index(drop=True)
                bars = bars.merge(
                    funding[["timestamp", "funding_event_ts", "funding_rate_scaled"]],
                    on="timestamp",
                    how="left",
                )

            if "is_gap" not in bars.columns:
                logging.warning("Missing is_gap column for %s; assuming no gaps.", symbol)
                bars["is_gap"] = False

            features, segment_id, nan_rates, column_map = _build_features_frame(bars, DEFAULT_WINDOWS)
            segment_lengths = segment_id.value_counts().sort_index()
            if segment_lengths.empty:
                segment_stats = {"count": 0, "min": 0, "median": 0, "max": 0}
            else:
                segment_stats = {
                    "count": int(len(segment_lengths)),
                    "min": int(segment_lengths.min()),
                    "median": float(segment_lengths.median()),
                    "max": int(segment_lengths.max()),
                }

            drop_mask = features[
                ["ret_1", "logret_1", "rv_96", column_map["rv_pct"], column_map["range_med"]]
            ].isna().any(axis=1)
            pct_rows_dropped = float(drop_mask.mean()) if len(features) else 0.0

            features["year"] = features["timestamp"].dt.year
            features["month"] = features["timestamp"].dt.month
            out_dir = run_scoped_lake_path(DATA_ROOT, run_id, "features", "perp", symbol, "15m", "features_v1")

            partitions_written: List[str] = []
            partitions_skipped: List[str] = []

            for (year, month), group in features.groupby(["year", "month"], sort=True):
                group_out = group.drop(columns=["year", "month"]).reset_index(drop=True)
                out_path = out_dir / f"year={year}" / f"month={month:02d}" / f"features_{symbol}_v1_{year}-{month:02d}.parquet"
                expected_ts = _expected_15m_index(
                    group_out["timestamp"].min(),
                    group_out["timestamp"].max() + timedelta(minutes=15),
                )
                if not args.force and _partition_complete(out_path, expected_ts):
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
                "nan_rates": nan_rates,
                "segment_stats": segment_stats,
                "pct_rows_dropped": pct_rows_dropped,
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
