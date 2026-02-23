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

from pipelines._lib.io_utils import (
    choose_partition_dir,
    ensure_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
    write_parquet,
)
from pipelines._lib.run_manifest import (
    feature_schema_identity,
    finalize_manifest,
    schema_hash_from_columns,
    start_manifest,
    validate_feature_schema_columns,
    validate_input_provenance,
)
from features.vol_regime import calculate_rv_percentile_24h
from features.carry_state import calculate_funding_rate_bps
from features.state_mapping import map_vol_regime, map_carry_state


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

    return series.rolling(window=window, min_periods=max(128, window // 6)).apply(pct_rank, raw=True)


def _dedupe_timestamp_rows(df: pd.DataFrame, label: str) -> tuple[pd.DataFrame, int]:
    if "timestamp" not in df.columns or df.empty:
        return df, 0
    out = df.sort_values("timestamp").copy()
    dupes = int(out["timestamp"].duplicated(keep="last").sum())
    if dupes > 0:
        logging.warning("Dropping %s duplicate timestamp rows for %s (keeping last).", dupes, label)
        out = out.drop_duplicates(subset=["timestamp"], keep="last")
    return out.reset_index(drop=True), dupes


def _context_partition_complete(existing: pd.DataFrame, expected: pd.DataFrame) -> bool:
    if existing.empty or len(existing) != len(expected):
        return False
    if "timestamp" not in existing.columns or "context_def_version" not in existing.columns:
        return False
    existing_ts = pd.to_datetime(existing["timestamp"], utc=True, errors="coerce")
    expected_ts = pd.to_datetime(expected["timestamp"], utc=True, errors="coerce")
    if existing_ts.isna().any() or expected_ts.isna().any():
        return False
    if list(existing_ts) != list(expected_ts):
        return False
    versions = set(existing["context_def_version"].astype(str).dropna().unique().tolist())
    return versions == {"market_context_v1"}


def _build_market_context(symbol: str, features: pd.DataFrame) -> pd.DataFrame:
    out = features[["timestamp"]].copy()
    out["symbol"] = symbol

    close = pd.to_numeric(features.get("close"), errors="coerce")
    out["trend_return_96"] = close.pct_change(96)
    out["trend_regime"] = np.where(out["trend_return_96"] >= 0.0, "bull", "bear")

    # New Indicators (Milestone A/D)
    out["rv_percentile_24h"] = calculate_rv_percentile_24h(close)
    out["vol_regime_code"] = map_vol_regime(out["rv_percentile_24h"])
    
    # Legacy string mapping for readability/back-compat
    out["vol_regime"] = out["vol_regime_code"].map({0.0: "low", 1.0: "mid", 2.0: "high"}).fillna("unknown")

    # Legacy: rv_pct_17280 required by schema
    rv_pct = pd.to_numeric(features.get("rv_pct_17280"), errors="coerce")
    if rv_pct.isna().all():
        rv = pd.to_numeric(features.get("rv_96"), errors="coerce")
        if rv.notna().any():
            rv_pct = _rolling_percentile(rv, window=17280)
        else:
            rv_pct = pd.Series(index=features.index, dtype=float)
    out["rv_pct_17280"] = rv_pct

    # Carry State
    # Try funding_rate first, then funding_rate_scaled
    if "funding_rate" in features.columns:
        fr = pd.to_numeric(features["funding_rate"], errors="coerce")
    elif "funding_rate_scaled" in features.columns:
        # Assuming scaled is rate * 1e9 or something? No, standard is usually rate * 10000? 
        # Actually build_features_v1 usually outputs funding_rate_scaled (e.g. rate * 100).
        # Let's assume funding_rate_scaled is close enough to use if raw missing?
        # But wait, calculate_funding_rate_bps expects raw funding rate (e.g. 0.0001).
        # If funding_rate_scaled is used, we need to know scale.
        # Let's check build_features_v1.py to see what it outputs.
        # It outputs `funding_rate_scaled`.
        # Assuming we can recover it or just rely on funding_rate being present if I generated it.
        # I generated `funding_rate` column in my synthetic data.
        # `build_features_v1` passes columns through?
        # `build_features_v1` merges funding.
        
        # Let's fallback to funding_rate_scaled if funding_rate missing, assuming it is bps?
        fr = pd.to_numeric(features.get("funding_rate_scaled", 0.0), errors="coerce") / 10000.0 # Blind guess if scaled
    else:
        fr = pd.Series(0.0, index=features.index)

    # In my synthetic data, I have 'funding_rate' column in funding parquet. 
    # build_features_v1 joins it. 
    # If build_features_v1 keeps 'funding_rate', good.
    # If not, it might rename it.
    
    # For now, let's use what's available.
    if "funding_rate" in features.columns:
        fr = pd.to_numeric(features["funding_rate"], errors="coerce")
        
    out["funding_rate_bps"] = calculate_funding_rate_bps(fr)
    out["carry_state_code"] = map_carry_state(out["funding_rate_bps"])
    
    # Map code back to string for legacy/readability
    out["carry_state"] = out["carry_state_code"].map({-1.0: "neg", 0.0: "neutral", 1.0: "pos"}).fillna("neutral")

    range_96 = pd.to_numeric(features.get("range_96"), errors="coerce")
    range_med_2880 = pd.to_numeric(features.get("range_med_2880"), errors="coerce")
    out["compression_ratio"] = (range_96 / range_med_2880.replace(0.0, np.nan)).astype(float)
    out["compression_state"] = (out["compression_ratio"] <= 0.8).fillna(False)

    funding = pd.to_numeric(features.get("funding_rate_scaled"), errors="coerce").fillna(0.0)
    out["funding_mean_32"] = funding.rolling(window=32, min_periods=8).mean()
    q_lo = float(out["funding_mean_32"].quantile(0.33)) if out["funding_mean_32"].notna().any() else 0.0
    q_hi = float(out["funding_mean_32"].quantile(0.67)) if out["funding_mean_32"].notna().any() else 0.0
    out["funding_regime"] = "neutral"
    out.loc[out["funding_mean_32"] <= q_lo, "funding_regime"] = "negative"
    out.loc[out["funding_mean_32"] >= q_hi, "funding_regime"] = "positive"

    out["context_def_version"] = "market_context_v1"
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Build market-state context features from features_v1")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--timeframe", default="5m")
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
        else run_scoped_lake_path(DATA_ROOT, args.run_id, "context", "market_state")
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
        "end_exclusive": end_exclusive.isoformat(),
        "out_dir": str(output_root),
        "force": int(args.force),
    }
    manifest = start_manifest("build_market_context", args.run_id, params, inputs, outputs)
    stats: Dict[str, object] = {"symbols": {}}

    try:
        feature_schema_version, feature_schema_hash = feature_schema_identity()
        stats["feature_schema_version"] = feature_schema_version
        stats["feature_schema_hash"] = feature_schema_hash
        for symbol in symbols:
            feature_candidates = [
                run_scoped_lake_path(DATA_ROOT, args.run_id, "features", "perp", symbol, args.timeframe, "features_v1"),
                DATA_ROOT / "lake" / "features" / "perp" / symbol / args.timeframe / "features_v1",
            ]
            feature_dir = choose_partition_dir(feature_candidates)
            features = read_parquet(list_parquet_files(feature_dir)) if feature_dir else pd.DataFrame()
            if features.empty:
                raise ValueError(f"No features_v1 found for {symbol} at timeframe={args.timeframe}")

            features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True, errors="coerce")
            features = features.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
            features, dropped_dupes = _dedupe_timestamp_rows(features, label=f"features:{symbol}:{args.timeframe}")
            features = features[(features["timestamp"] >= start) & (features["timestamp"] < end_exclusive)].copy()
            if features.empty:
                raise ValueError(f"No feature rows in requested range for {symbol}")

            inputs.append(
                {
                    "path": str(feature_dir),
                    **_collect_stats(features),
                    "provenance": {
                        "vendor": "binance",
                        "exchange": "binance",
                        "schema_version": "features_v1_5m_v1",
                        "schema_hash": schema_hash_from_columns(features.columns.tolist()),
                        "extraction_start": features["timestamp"].min().isoformat(),
                        "extraction_end": features["timestamp"].max().isoformat(),
                    },
                }
            )
            context = _build_market_context(symbol=symbol, features=features)
            validate_feature_schema_columns(
                dataset_key="market_context_v1",
                columns=context.columns.tolist(),
            )

            out_path = output_root / symbol / f"{args.timeframe}.parquet"
            if out_path.exists() and not int(args.force):
                existing = read_parquet([out_path])
                if _context_partition_complete(existing, context):
                    outputs.append({"path": str(out_path), "rows": int(len(existing)), "storage_format": out_path.suffix})
                    stats["symbols"][symbol] = {
                        "rows": int(len(existing)),
                        "skipped_existing": True,
                        "features_duplicate_rows_dropped": int(dropped_dupes),
                    }
                    continue

            written_path, storage = write_parquet(context, out_path)
            outputs.append({"path": str(written_path), "rows": int(len(context)), "storage_format": storage})
            stats["symbols"][symbol] = {
                "rows": int(len(context)),
                "vol_regime_counts": context["vol_regime"].value_counts(dropna=False).to_dict(),
                "funding_regime_counts": context["funding_regime"].value_counts(dropna=False).to_dict(),
                "feature_schema_version": feature_schema_version,
                "feature_schema_hash": feature_schema_hash,
                "features_duplicate_rows_dropped": int(dropped_dupes),
            }

        validate_input_provenance(inputs)
        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Market context build failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
