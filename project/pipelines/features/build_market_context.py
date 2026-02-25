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
from pipelines._lib.ontology_contract import MATERIALIZED_STATE_COLUMNS_BY_ID
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


def _canonical_funding_rate_scaled(features: pd.DataFrame, *, symbol: str) -> pd.Series:
    if "funding_rate_scaled" not in features.columns:
        raise ValueError(
            f"features_v1 for {symbol} missing funding_rate_scaled; "
            "market context requires canonical funding_rate_scaled input."
        )
    funding = pd.to_numeric(features["funding_rate_scaled"], errors="coerce")
    if funding.isna().all():
        raise ValueError(f"Funding series is fully null for {symbol}")
    missing_pct = float(funding.isna().mean()) if len(funding) else 0.0
    if missing_pct > 0.0:
        raise ValueError(
            f"Funding series contains gaps for {symbol}: missing_pct={missing_pct:.4f}; "
            "rebuild upstream funding alignment before market context."
        )
    return funding.astype(float)


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

    funding_rate_scaled = _canonical_funding_rate_scaled(features, symbol=symbol)
    out["funding_rate_bps"] = calculate_funding_rate_bps(funding_rate_scaled)
    out["carry_state_code"] = map_carry_state(out["funding_rate_bps"])
    
    # Map code back to string for legacy/readability
    out["carry_state"] = out["carry_state_code"].map({-1.0: "neg", 0.0: "neutral", 1.0: "pos"}).fillna("neutral")

    range_96 = pd.to_numeric(features.get("range_96"), errors="coerce")
    range_med_2880 = pd.to_numeric(features.get("range_med_2880"), errors="coerce")
    out["compression_ratio"] = (range_96 / range_med_2880.replace(0.0, np.nan)).astype(float)
    out["compression_state"] = (out["compression_ratio"] <= 0.8).fillna(False)

    out["funding_mean_32"] = out["funding_rate_bps"].rolling(window=32, min_periods=8).mean()
    q_lo = float(out["funding_mean_32"].quantile(0.33)) if out["funding_mean_32"].notna().any() else 0.0
    q_hi = float(out["funding_mean_32"].quantile(0.67)) if out["funding_mean_32"].notna().any() else 0.0
    out["funding_regime"] = "neutral"
    out.loc[out["funding_mean_32"] <= q_lo, "funding_regime"] = "negative"
    out.loc[out["funding_mean_32"] >= q_hi, "funding_regime"] = "positive"

    # Materialized ontology states (bounded set) for phase2 state expansion.
    def _feature_series(name: str) -> pd.Series:
        if name in features.columns:
            return pd.to_numeric(features[name], errors="coerce")
        return pd.Series(np.nan, index=features.index, dtype=float)

    spread_z = _feature_series("spread_zscore")
    rv_96 = _feature_series("rv_96")
    oi_notional = _feature_series("oi_notional")
    oi_delta_1h = _feature_series("oi_delta_1h")

    spread_p90 = float(spread_z.quantile(0.90)) if spread_z.notna().any() else np.nan
    funding_abs = out["funding_rate_bps"].abs()
    funding_p95 = float(funding_abs.quantile(0.95)) if funding_abs.notna().any() else np.nan
    oi_p90 = float(oi_notional.quantile(0.90)) if oi_notional.notna().any() else np.nan
    oi_delta_p10 = float(oi_delta_1h.quantile(0.10)) if oi_delta_1h.notna().any() else np.nan
    rv_96_med = rv_96.rolling(window=96, min_periods=24).median()
    funding_extreme = funding_abs >= funding_p95 if np.isfinite(funding_p95) else pd.Series(False, index=out.index)

    out[MATERIALIZED_STATE_COLUMNS_BY_ID["LOW_LIQUIDITY_STATE"]] = (
        (spread_z >= spread_p90).fillna(False)
        | (out["rv_percentile_24h"] >= 0.80).fillna(False)
    )
    out[MATERIALIZED_STATE_COLUMNS_BY_ID["SPREAD_ELEVATED_STATE"]] = (spread_z >= spread_p90).fillna(False)
    out[MATERIALIZED_STATE_COLUMNS_BY_ID["REFILL_LAG_STATE"]] = (
        out[MATERIALIZED_STATE_COLUMNS_BY_ID["LOW_LIQUIDITY_STATE"]]
        & out[MATERIALIZED_STATE_COLUMNS_BY_ID["LOW_LIQUIDITY_STATE"]]
        .rolling(window=6, min_periods=1)
        .max()
        .astype(bool)
    )
    out[MATERIALIZED_STATE_COLUMNS_BY_ID["AFTERSHOCK_STATE"]] = (
        (rv_96 > (rv_96_med * 1.25)).fillna(False)
        | (out["rv_percentile_24h"] >= 0.80).fillna(False)
    )
    out[MATERIALIZED_STATE_COLUMNS_BY_ID["COMPRESSION_STATE"]] = out["compression_state"].fillna(False).astype(bool)
    out[MATERIALIZED_STATE_COLUMNS_BY_ID["HIGH_VOL_REGIME"]] = out["vol_regime_code"].eq(2.0)
    out[MATERIALIZED_STATE_COLUMNS_BY_ID["LOW_VOL_REGIME"]] = out["vol_regime_code"].eq(0.0)
    out[MATERIALIZED_STATE_COLUMNS_BY_ID["CROWDING_STATE"]] = (
        funding_extreme.fillna(False)
        & (oi_notional >= oi_p90).fillna(False)
    )
    out[MATERIALIZED_STATE_COLUMNS_BY_ID["FUNDING_PERSISTENCE_STATE"]] = (
        funding_extreme.fillna(False)
        .rolling(window=12, min_periods=1)
        .sum()
        .ge(6)
    )
    out[MATERIALIZED_STATE_COLUMNS_BY_ID["DELEVERAGING_STATE"]] = (
        (oi_delta_1h <= oi_delta_p10).fillna(False)
        & (out["rv_percentile_24h"] >= 0.66).fillna(False)
    )

    # Broad fallback state masks so every ontology state has a materialized column.
    trend_abs = out["trend_return_96"].abs().fillna(0.0)
    trend_q75 = float(trend_abs.quantile(0.75)) if trend_abs.notna().any() else 0.0
    trend_q25 = float(trend_abs.quantile(0.25)) if trend_abs.notna().any() else 0.0
    trending_mask = trend_abs >= trend_q75
    chop_mask = trend_abs <= trend_q25

    basis_z = _feature_series("basis_zscore")
    if basis_z.isna().all():
        basis_z = _feature_series("cross_exchange_spread_z")
    basis_p90 = float(basis_z.abs().quantile(0.90)) if basis_z.notna().any() else np.nan
    desync_mask = (basis_z.abs() >= basis_p90).fillna(False) if np.isfinite(basis_p90) else pd.Series(False, index=out.index)

    ts = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    hour = ts.dt.hour.fillna(-1).astype(int)
    minute = ts.dt.minute.fillna(-1).astype(int)
    open_window_mask = ((hour.isin([0, 8, 13])) & (minute <= 15)).fillna(False)
    close_window_mask = ((hour.isin([7, 12, 23])) & (minute >= 45)).fillna(False)
    funding_window_mask = ((hour.isin([0, 8, 16])) & (minute <= 10)).fillna(False)
    news_window_mask = ((hour.isin([12, 13, 14, 18])) & (minute.between(25, 35))).fillna(False)

    low_liq_mask = out[MATERIALIZED_STATE_COLUMNS_BY_ID["LOW_LIQUIDITY_STATE"]].fillna(False).astype(bool)
    spread_mask = out[MATERIALIZED_STATE_COLUMNS_BY_ID["SPREAD_ELEVATED_STATE"]].fillna(False).astype(bool)
    aftershock_mask = out[MATERIALIZED_STATE_COLUMNS_BY_ID["AFTERSHOCK_STATE"]].fillna(False).astype(bool)
    compression_mask = out[MATERIALIZED_STATE_COLUMNS_BY_ID["COMPRESSION_STATE"]].fillna(False).astype(bool)
    high_vol_mask = out[MATERIALIZED_STATE_COLUMNS_BY_ID["HIGH_VOL_REGIME"]].fillna(False).astype(bool)
    crowding_mask = out[MATERIALIZED_STATE_COLUMNS_BY_ID["CROWDING_STATE"]].fillna(False).astype(bool)
    funding_persist_mask = out[MATERIALIZED_STATE_COLUMNS_BY_ID["FUNDING_PERSISTENCE_STATE"]].fillna(False).astype(bool)
    deleveraging_mask = out[MATERIALIZED_STATE_COLUMNS_BY_ID["DELEVERAGING_STATE"]].fillna(False).astype(bool)
    friction_high_mask = (spread_mask | low_liq_mask | aftershock_mask).fillna(False)

    for state_id, column in MATERIALIZED_STATE_COLUMNS_BY_ID.items():
        if column in out.columns:
            out[column] = out[column].fillna(False).astype(bool)
            continue

        token = str(state_id).upper()
        if any(k in token for k in ["LIQUIDITY", "SPREAD", "DEPTH", "SWEEP", "ABSORPTION", "ILLIQUID"]):
            mask = (low_liq_mask | spread_mask)
        elif any(k in token for k in ["VOL", "AFTERSHOCK", "COMPRESSION"]):
            mask = (aftershock_mask | compression_mask | high_vol_mask)
        elif any(k in token for k in ["FUNDING", "CROWD", "SQUEEZE", "DELEVERAGING", "LIQUIDATION", "CARRY"]):
            mask = (crowding_mask | funding_persist_mask | deleveraging_mask)
        elif any(k in token for k in ["TREND", "CHOP", "PULLBACK", "BREAKOUT", "FAILURE", "REVERS"]):
            mask = (trending_mask | chop_mask)
        elif any(k in token for k in ["DESYNC", "BASIS", "CONVERGENCE", "ARBITRAGE"]):
            mask = desync_mask
        elif any(k in token for k in ["OPEN_WINDOW", "SESSION_OPEN"]):
            mask = open_window_mask
        elif any(k in token for k in ["CLOSE_WINDOW", "SESSION_CLOSE"]):
            mask = close_window_mask
        elif any(k in token for k in ["NEWS", "FUNDING_WINDOW", "FUNDING_TIMESTAMP"]):
            mask = (news_window_mask | funding_window_mask)
        elif any(k in token for k in ["FRICTION", "SLIPPAGE"]):
            mask = friction_high_mask
        else:
            mask = pd.Series(False, index=out.index)

        out[column] = pd.Series(mask, index=out.index).fillna(False).astype(bool)

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
