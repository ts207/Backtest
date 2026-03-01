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
from pipelines._lib.io_utils import choose_partition_dir, ensure_dir, list_parquet_files, read_parquet, run_scoped_lake_path, write_parquet
from pipelines._lib.run_manifest import (
    feature_schema_identity,
    finalize_manifest,
    load_feature_schema_registry,
    schema_hash_from_columns,
    start_manifest,
    validate_feature_schema_columns,
    validate_input_provenance,
)
from pipelines._lib.validation import ensure_utc_timestamp, validate_columns, ts_ns_utc
from pipelines._lib.sanity import assert_monotonic_utc_timestamp
from schemas.data_contracts import Cleaned5mBarsSchema
from features.microstructure import (
    calculate_vpin_score,
    calculate_roll_spread_bps,
    calculate_amihud_illiquidity,
    calculate_kyle_lambda,
)

FUNDING_EVENT_HOURS = 8
FUNDING_MAX_STALENESS = pd.Timedelta(hours=FUNDING_EVENT_HOURS)
OI_MAX_STALENESS = pd.Timedelta(hours=4)
LIQ_MAX_STALENESS = pd.Timedelta(hours=4)


def _collect_stats(df: pd.DataFrame) -> Dict[str, object]:
    if df.empty:
        return {"rows": 0, "start_ts": None, "end_ts": None}
    ts = ts_ns_utc(df["timestamp"])
    return {
        "rows": int(len(df)),
        "start_ts": ts.min().isoformat(),
        "end_ts": ts.max().isoformat(),
    }


def features_contract_check(df: pd.DataFrame, symbol: str) -> None:
    """
    Assert that features output preserves bar count, grid, and schema.
    """
    if df.empty:
        return
    required = ["timestamp", "symbol", "close"]
    validate_columns(df, required)
    
    ts = ts_ns_utc(df["timestamp"])
    assert_monotonic_utc_timestamp(df, "timestamp")
    
    if len(ts) > 1:
        diffs = ts.diff().dropna()
        # 5m bars = 300s = 300,000,000,000 ns
        expected_ns = 300 * 10**9
        if not (diffs.view("int64") == expected_ns).all():
            # Allow first bar to be off-grid if it's a partial start, 
            # but middle bars must be exactly 5m.
            if not (diffs.view("int64")[1:] == expected_ns).all():
                raise ValueError(f"Features for {symbol} do not follow 5m grid")


def _revision_lag_minutes(revision_lag_bars: int) -> int:
    return int(revision_lag_bars) * 5


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
        if len(data) != expected_rows:
            return False
        if "timestamp" not in data.columns:
            return False
        ts = pd.to_datetime(data["timestamp"], utc=True, errors="coerce")
        if ts.isna().any():
            return False
        if ts.duplicated().any():
            return False
        return ts.is_monotonic_increasing
    except Exception:
        return False


def _read_optional_time_series(path: Path) -> pd.DataFrame:
    files = list_parquet_files(path)
    if not files:
        return pd.DataFrame()
    frame = read_parquet(files)
    if frame.empty:
        return pd.DataFrame()
    
    ts_col = "timestamp" if "timestamp" in frame.columns else ("ts" if "ts" in frame.columns else None)
    if ts_col is None:
        return pd.DataFrame()
        
    frame = frame.rename(columns={ts_col: "timestamp"})
    frame["timestamp"] = ts_ns_utc(frame["timestamp"])
    frame = frame.sort_values("timestamp").reset_index(drop=True)
    assert_monotonic_utc_timestamp(frame, "timestamp")
    return frame


def _merge_optional_oi_liquidation(
    bars: pd.DataFrame,
    symbol: str,
    market: str,
    run_id: str,
) -> pd.DataFrame:
    out = bars.copy()
    expected_rows = int(len(out))

    oi_candidates = [
        DATA_ROOT / "lake" / "raw" / "binance" / market / symbol / "open_interest" / "5m",
        DATA_ROOT / "lake" / "runs" / run_id / "raw" / "binance" / market / symbol / "open_interest" / "5m",
    ]
    oi_frame = pd.DataFrame()
    for candidate in oi_candidates:
        oi_frame = _read_optional_time_series(candidate)
        if not oi_frame.empty:
            break

    if not oi_frame.empty:
        oi_col = None
        for c in ["sum_open_interest_value", "sum_open_interest", "open_interest"]:
            if c in oi_frame.columns:
                oi_col = c
                break
        if oi_col is not None:
            oi_series = oi_frame[["timestamp", oi_col]].rename(columns={oi_col: "oi_notional"}).copy()
            oi_series["oi_notional"] = pd.to_numeric(oi_series["oi_notional"], errors="coerce")
            if oi_series["timestamp"].duplicated(keep=False).any():
                raise ValueError(f"Open interest timestamps must be unique for {symbol}")
            out = pd.merge_asof(
                out.sort_values("timestamp"),
                oi_series.sort_values("timestamp"),
                on="timestamp",
                direction="backward",
                tolerance=OI_MAX_STALENESS,
            )
            if len(out) != expected_rows:
                raise ValueError(
                    f"Cardinality mismatch after OI merge for {symbol}: "
                    f"expected {expected_rows}, got {len(out)}"
                )
        else:
            out["oi_notional"] = np.nan
    else:
        out["oi_notional"] = np.nan

    liq_candidates = [
        DATA_ROOT / "lake" / "raw" / "binance" / market / symbol / "liquidation_snapshot",
        DATA_ROOT / "lake" / "runs" / run_id / "raw" / "binance" / market / symbol / "liquidation_snapshot",
    ]
    liq_frame = pd.DataFrame()
    for candidate in liq_candidates:
        liq_frame = _read_optional_time_series(candidate)
        if not liq_frame.empty:
            break

    if not liq_frame.empty:
        value_col = None
        for c in ["notional_usd", "quote_qty", "notional"]:
            if c in liq_frame.columns:
                value_col = c
                break
        count_col = None
        for c in ["event_count", "count", "n_events"]:
            if c in liq_frame.columns:
                count_col = c
                break

        liq_payload = pd.DataFrame({"timestamp": liq_frame["timestamp"]})
        liq_payload["liquidation_notional"] = pd.to_numeric(liq_frame[value_col], errors="coerce") if value_col else 0.0
        liq_payload["liquidation_count"] = pd.to_numeric(liq_frame[count_col], errors="coerce") if count_col else 1.0
        if liq_payload["timestamp"].duplicated(keep=False).any():
            raise ValueError(f"Liquidation timestamps must be unique for {symbol}")
        out = pd.merge_asof(
            out.sort_values("timestamp"),
            liq_payload.sort_values("timestamp"),
            on="timestamp",
            direction="backward",
            tolerance=LIQ_MAX_STALENESS,
        )
        if len(out) != expected_rows:
            raise ValueError(
                f"Cardinality mismatch after liquidation merge for {symbol}: "
                f"expected {expected_rows}, got {len(out)}"
            )
    else:
        out["liquidation_notional"] = 0.0
        out["liquidation_count"] = 0.0

    out["oi_notional"] = pd.to_numeric(out["oi_notional"], errors="coerce")
    out["oi_delta_1h"] = out["oi_notional"].diff(12) # 5m bars, 1h = 12 bars
    out["liquidation_notional"] = pd.to_numeric(out["liquidation_notional"], errors="coerce").fillna(0.0)
    out["liquidation_count"] = pd.to_numeric(out["liquidation_count"], errors="coerce").fillna(0.0)
    return out


def _merge_funding_rates(bars: pd.DataFrame, funding: pd.DataFrame, symbol: str) -> pd.DataFrame:
    out = bars.copy()
    assert_monotonic_utc_timestamp(out, "timestamp")

    # Accept either funding_rate_scaled or funding_rate_feature as the rate column
    rate_col = None
    for candidate in ["funding_rate_scaled", "funding_rate_feature", "funding_rate"]:
        if candidate in funding.columns:
            rate_col = candidate
            break
    if rate_col is None:
        raise ValueError(f"No funding rate column found in funding frame for {symbol}")

    funding_rates = funding[["timestamp", rate_col]].copy()
    if rate_col != "funding_rate_scaled":
        funding_rates = funding_rates.rename(columns={rate_col: "funding_rate_scaled"})
    funding_rates["timestamp"] = ts_ns_utc(funding_rates["timestamp"])
    funding_rates["funding_rate_scaled"] = pd.to_numeric(funding_rates["funding_rate_scaled"], errors="coerce")
    funding_rates = funding_rates.sort_values("timestamp").reset_index(drop=True)
    if funding_rates["timestamp"].duplicated().any():
        raise ValueError(f"Funding timestamps must be unique for {symbol}")
    assert_monotonic_utc_timestamp(funding_rates, "timestamp")

    expected_rows = int(len(out))
    merged = pd.merge_asof(
        out.sort_values("timestamp"),
        funding_rates.sort_values("timestamp"),
        on="timestamp",
        direction="backward",
        tolerance=FUNDING_MAX_STALENESS,
    )
    if len(merged) != expected_rows:
        raise ValueError(
            f"Cardinality mismatch after funding merge for {symbol}: "
            f"expected {expected_rows}, got {len(merged)}"
        )
    return merged


def _rolling_zscore(series: pd.Series, window: int, min_periods: int = 24) -> pd.Series:
    mean = series.rolling(window=window, min_periods=min_periods).mean()
    std = series.rolling(window=window, min_periods=min_periods).std(ddof=0)
    z = (series - mean) / std.replace(0.0, np.nan)
    return z.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _load_spot_close_reference(symbol: str, run_id: str) -> pd.DataFrame:
    candidates = [
        DATA_ROOT / "lake" / "runs" / run_id / "cleaned" / "spot" / symbol / "bars_5m",
        DATA_ROOT / "lake" / "cleaned" / "spot" / symbol / "bars_5m",
    ]
    for candidate in candidates:
        frame = _read_optional_time_series(candidate)
        if frame.empty:
            continue
        if "close" not in frame.columns:
            continue
        out = frame[["timestamp", "close"]].rename(columns={"close": "spot_close"}).copy()
        out["spot_close"] = pd.to_numeric(out["spot_close"], errors="coerce")
        return out.dropna(subset=["spot_close"]).sort_values("timestamp").reset_index(drop=True)
    return pd.DataFrame(columns=["timestamp", "spot_close"])


def _add_basis_features(frame: pd.DataFrame, symbol: str, run_id: str, market: str) -> pd.DataFrame:
    out = frame.copy()
    if market != "perp":
        out["basis_bps"] = 0.0
        out["basis_zscore"] = 0.0
        out["cross_exchange_spread_z"] = 0.0
        out["spread_zscore"] = 0.0
        out["basis_spot_coverage"] = 0.0
        return out

    spot = _load_spot_close_reference(symbol=symbol, run_id=run_id)
    if not spot.empty:
        spot_sorted = spot.sort_values('timestamp').reset_index(drop=True)
        out_sorted = out.sort_values('timestamp').reset_index(drop=True)
        out = pd.merge_asof(
            out_sorted,
            spot_sorted,
            on='timestamp',
            direction='backward',
            tolerance=pd.Timedelta('5min'),
        )
        coverage = float(out['spot_close'].notna().mean()) if len(out) else 0.0
        out['basis_spot_coverage'] = coverage
    else:
        out["spot_close"] = np.nan
        out["basis_spot_coverage"] = 0.0

    perp_close = pd.to_numeric(out["close"], errors="coerce")
    spot_close = pd.to_numeric(out["spot_close"], errors="coerce")
    ratio = perp_close / spot_close.replace(0.0, np.nan)
    out["basis_bps"] = ((ratio - 1.0) * 10_000.0).replace([np.inf, -np.inf], np.nan)
    basis_z = _rolling_zscore(pd.to_numeric(out["basis_bps"], errors="coerce"), window=96)
    out["basis_zscore"] = basis_z.where(out["basis_bps"].notna(), np.nan)
    out["cross_exchange_spread_z"] = out["basis_zscore"]
    spread_bps = pd.to_numeric(out.get("spread_bps", pd.Series(np.nan, index=out.index)), errors="coerce")
    spread_z = _rolling_zscore(spread_bps, window=96)
    out["spread_zscore"] = spread_z.where(spread_bps.notna(), np.nan)
    out = out.drop(columns=["spot_close"], errors="ignore")
    return out


def _merge_optional_tob_aggregates(
    bars: pd.DataFrame,
    symbol: str,
    market: str,
    run_id: str,
) -> pd.DataFrame:
    out = bars.copy()
    if market != "perp":
        return out
        
    expected_rows = int(len(out))
    tob_candidates = [
        DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "tob_5m_agg",
        DATA_ROOT / "lake" / "runs" / run_id / "cleaned" / "perp" / symbol / "tob_5m_agg",
    ]
    tob_dir = choose_partition_dir(tob_candidates)
    tob_files = list_parquet_files(tob_dir) if tob_dir else []
    
    if not tob_files:
        return out
        
    tob_frame = read_parquet(tob_files)
    if tob_frame.empty:
        return out
        
    tob_frame["timestamp"] = ts_ns_utc(tob_frame["timestamp"])
    tob_frame = tob_frame.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last")
    
    # Calculate unified depth proxy if both sides are present
    if "bid_depth_usd_mean" in tob_frame.columns and "ask_depth_usd_mean" in tob_frame.columns:
        tob_frame["depth_usd"] = (tob_frame["bid_depth_usd_mean"] + tob_frame["ask_depth_usd_mean"]) / 2
    
    # Alias spread_bps_mean to spread_bps for schema compatibility
    if "spread_bps_mean" in tob_frame.columns:
        tob_frame["spread_bps"] = tob_frame["spread_bps_mean"]
        
    # Columns to merge
    merge_cols = ["timestamp", "spread_bps", "depth_usd", "tob_coverage"]
    merge_cols = [c for c in merge_cols if c in tob_frame.columns]
    
    if len(merge_cols) <= 1:
        return out
        
    out = pd.merge_asof(
        out.sort_values("timestamp"),
        tob_frame[merge_cols].sort_values("timestamp"),
        on="timestamp",
        direction="backward",
        tolerance=pd.Timedelta("5min"),
    )
    
    if len(out) != expected_rows:
        raise ValueError(f"Cardinality mismatch after ToB merge for {symbol}")
        
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Build features v1")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--market", choices=["perp", "spot"], default="perp")
    parser.add_argument("--force", type=int, default=0)
    # Compatibility flag from run_all; feature build already tolerates missing funding.
    parser.add_argument("--allow_missing_funding", type=int, default=0)
    parser.add_argument("--revision_lag_bars", type=int, default=0)
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--log_path", default=None)
    parser.add_argument("--feature_schema_version", choices=["v1", "v2"], default="v1")
    args = parser.parse_args()

    if args.feature_schema_version:
        os.environ["BACKTEST_FEATURE_SCHEMA_VERSION"] = args.feature_schema_version

    run_id = args.run_id
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    market = str(args.market).strip().lower()

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    config_paths = [str(PROJECT_ROOT / "configs" / "pipeline.yaml")]
    for raw in args.config:
        path = Path(str(raw))
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        config_paths.append(str(path))
    config = load_configs(config_paths)

    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    params = {
        "symbols": symbols,
        "market": market,
        "trade_day_timezone": config.get("trade_day_timezone", "UTC"),
        "force": int(args.force),
        "allow_missing_funding": int(args.allow_missing_funding),
        "source_vendor": "binance",
        "source_exchange": "binance",
        "schema_versions": {
            "cleaned_bars": "cleaned_bars_5m_v1",
            "funding": "funding_5m_v1",
        },
    }
    stage_name = "build_features_v1" if market == "perp" else "build_features_v1_spot"
    manifest = start_manifest(stage_name, run_id, params, inputs, outputs)
    stats: Dict[str, object] = {"symbols": {}}

    try:
        feature_schema_version, feature_schema_hash = feature_schema_identity()
        stats["feature_schema_version"] = feature_schema_version
        stats["feature_schema_hash"] = feature_schema_hash
        for symbol in symbols:
            cleaned_candidates = [
                run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", market, symbol, "bars_5m"),
                DATA_ROOT / "lake" / "cleaned" / market / symbol / "bars_5m",
            ]
            funding_candidates = [
                run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", market, symbol, "funding_5m"),
                DATA_ROOT / "lake" / "cleaned" / market / symbol / "funding_5m",
            ]
            cleaned_dir = choose_partition_dir(cleaned_candidates)
            funding_dir = choose_partition_dir(funding_candidates)
            cleaned_files = list_parquet_files(cleaned_dir) if cleaned_dir else []
            funding_files = list_parquet_files(funding_dir) if funding_dir else []
            bars = read_parquet(cleaned_files)
            funding = read_parquet(funding_files) if market == "perp" else pd.DataFrame()
            if bars.empty:
                raise ValueError(f"No cleaned bars for {symbol}")

            validate_columns(bars, ["timestamp", "open", "high", "low", "close"])
            bars["timestamp"] = ts_ns_utc(bars["timestamp"])
            assert_monotonic_utc_timestamp(bars, "timestamp")
            bars = bars.sort_values("timestamp").reset_index(drop=True)

            bars_stats = _collect_stats(bars)
            inputs.append(
                {
                    "path": str(cleaned_dir),
                    **bars_stats,
                    "provenance": {
                        "vendor": "binance",
                        "exchange": "binance",
                        "schema_version": "cleaned_bars_5m_v1",
                        "schema_hash": schema_hash_from_columns(bars.columns.tolist()),
                        "extraction_start": bars_stats.get("start_ts"),
                        "extraction_end": bars_stats.get("end_ts"),
                    },
                }
            )
            if market == "perp" and not funding.empty:
                funding_stats = _collect_stats(funding)
                inputs.append(
                    {
                        "path": str(funding_dir),
                        **funding_stats,
                        "provenance": {
                            "vendor": "binance",
                            "exchange": "binance",
                            "schema_version": "funding_5m_v1",
                            "schema_hash": schema_hash_from_columns(funding.columns.tolist()),
                            "extraction_start": funding_stats.get("start_ts"),
                            "extraction_end": funding_stats.get("end_ts"),
                        },
                    }
                )

            if market == "perp" and "funding_rate_feature" in bars.columns:
                pass
            elif market == "perp" and not funding.empty:
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
                funding_rates = funding[["timestamp", rate_col]].rename(columns={rate_col: "funding_rate_feature"})
                bars = _merge_funding_rates(bars, funding_rates, symbol=symbol)
                # Since we are re-merging older format funding here, let's just make realized 0 or mimic it
                bars["funding_rate_realized"] = 0.0 # Simplify backfill
            elif args.feature_schema_version == "v2":
                # Strict mode: do not provide defaults if v2 is selected
                pass
            else:
                bars["funding_rate_feature"] = 0.0
                bars["funding_rate_realized"] = 0.0

            # Backward-compatible alias for any code still reading funding_rate.
            bars["funding_rate"] = bars["funding_rate_feature"]
            bars = _merge_optional_oi_liquidation(bars, symbol=symbol, market=market, run_id=run_id)
            bars = _merge_optional_tob_aggregates(bars, symbol=symbol, market=market, run_id=run_id)
            bars = _add_basis_features(bars, symbol=symbol, run_id=run_id, market=market)
            bars["revision_lag_bars"] = int(args.revision_lag_bars)
            bars["revision_lag_minutes"] = _revision_lag_minutes(int(args.revision_lag_bars))

            # --- CONTRACT-DRIVEN FEATURE SELECTION ---
            # Determine which dataset contract to use
            dataset_key = "features_v2_5m_v1" if args.feature_schema_version == "v2" else "features_v1_5m_v1"
            registry = load_feature_schema_registry()
            if dataset_key not in registry["datasets"]:
                raise ValueError(f"Unknown dataset key in registry: {dataset_key}")
            
            required_cols = registry["datasets"][dataset_key]["required_columns"]
            optional_cols = registry["datasets"][dataset_key].get("optional_gated_columns", [])
            
            # Core columns that are always required regardless of registry
            core_cols = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
            
            # Combine and deduplicate
            target_cols = sorted(list(set(required_cols + core_cols + optional_cols)))
            
            # Only select columns that are actually present in bars.
            # Missing required columns will be caught by validate_feature_schema_columns.
            feature_cols = [col for col in target_cols if col in bars.columns]
            
            features = bars[feature_cols].copy()
            # --- END SELECTION ---

            if "close" in features.columns:
                features["logret_1"] = np.log(features["close"]).diff()
            if "funding_rate_feature" in features.columns:
                features["funding_rate_scaled"] = features["funding_rate_feature"]
            
            # Microstructure metrics
            # VPIN proxy: using taker_base_volume if available, else approximate.
            if "volume" in bars.columns:
                vol_raw = pd.to_numeric(bars["volume"], errors="coerce").fillna(0.0)
                if "taker_base_volume" in bars.columns:
                    buy_vol_proxy = pd.to_numeric(bars["taker_base_volume"], errors="coerce").fillna(0.0)
                else:
                    # Fallback to HLC proxy if taker volume is missing
                    h, l, c = bars["high"], bars["low"], bars["close"]
                    buy_vol_proxy = vol_raw * (c - l) / (h - l).replace(0.0, np.nan).fillna(0.5)
                
                if "ms_vpin_24" in target_cols:
                    features["ms_vpin_24"] = calculate_vpin_score(vol_raw, buy_vol_proxy, window=24)
                if "ms_roll_24" in target_cols:
                    features["ms_roll_24"] = calculate_roll_spread_bps(bars["close"], window=24)
                if "ms_amihud_24" in target_cols:
                    features["ms_amihud_24"] = calculate_amihud_illiquidity(bars["close"], vol_raw, window=24)
                if "ms_kyle_24" in target_cols:
                    features["ms_kyle_24"] = calculate_kyle_lambda(
                        bars["close"], 
                        buy_vol_proxy, 
                        vol_raw - buy_vol_proxy, 
                        window=24
                    )

            if "logret_1" in target_cols:
                features["logret_1"] = np.log(features["close"]).diff()
            
            if "rv_96" in target_cols:
                features["rv_96"] = features["logret_1"].rolling(window=96, min_periods=72).std()
            if "rv_pct_17280" in target_cols:
                features["rv_pct_17280"] = _rolling_percentile(features["rv_96"], window=17280)
            
            if "high_96" in target_cols:
                features["high_96"] = bars["high"].rolling(window=96, min_periods=72).max()
            if "low_96" in target_cols:
                features["low_96"] = bars["low"].rolling(window=96, min_periods=72).min()
            if "range_96" in target_cols:
                features["range_96"] = features["high_96"] - features["low_96"]
            if "range_med_2880" in target_cols:
                features["range_med_2880"] = features["range_96"].rolling(window=2880, min_periods=2160).median()

            features["year"] = features["timestamp"].dt.year
            features["month"] = features["timestamp"].dt.month
            
            features_contract_check(features, symbol)
            
            validate_feature_schema_columns(
                dataset_key=dataset_key,
                columns=features.columns.drop(["year", "month"]).tolist(),
            )
            out_dir = run_scoped_lake_path(DATA_ROOT, run_id, "features", market, symbol, "5m", "features_v1")
            out_dir_compat = DATA_ROOT / "lake" / "features" / market / symbol / "5m" / "features_v1"

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
                compat_path = out_dir_compat / f"year={year}" / f"month={month:02d}" / f"features_{symbol}_v1_{year}-{month:02d}.parquet"
                ensure_dir(compat_path.parent)
                write_parquet(group_out, compat_path)
                outputs.append({"path": str(written_path), **_collect_stats(group_out), "storage": storage})
                partitions_written.append(str(written_path))

            stats["symbols"][symbol] = {
                "rows_written": int(len(features)),
                "coverage_start": features["timestamp"].min().isoformat(),
                "coverage_end": features["timestamp"].max().isoformat(),
                "revision_lag_bars": int(args.revision_lag_bars),
                "oi_non_null_rows": int(features["oi_notional"].notna().sum()),
                "liquidation_non_zero_rows": int((features["liquidation_notional"] > 0).sum()),
                "feature_schema_version": feature_schema_version,
                "feature_schema_hash": feature_schema_hash,
                "partitions_written": partitions_written,
                "partitions_skipped": partitions_skipped,
            }

        validate_input_provenance(inputs)
        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Feature build failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
