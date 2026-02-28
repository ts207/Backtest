from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

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
from pipelines._lib.run_manifest import (
    feature_schema_identity,
    finalize_manifest,
    schema_hash_from_columns,
    start_manifest,
    validate_feature_schema_columns,
    validate_input_provenance,
)

FUNDING_MAX_STALENESS = pd.Timedelta("8h")


def _to_utc_ts(s: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s, utc=True, errors="coerce").astype("datetime64[ns, UTC]")

    s_num = pd.to_numeric(s, errors="coerce")
    if s_num.notna().any():
        v = float(s_num.dropna().iloc[0])
        if v > 1e17:
            unit = "ns"
        elif v > 1e14:
            unit = "us"
        elif v > 1e11:
            unit = "ms"
        else:
            unit = "s"
        return pd.to_datetime(s_num, utc=True, unit=unit, errors="coerce").astype("datetime64[ns, UTC]")

    return pd.to_datetime(s, utc=True, errors="coerce").astype("datetime64[ns, UTC]")


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


def _align_funding_to_bars(bars: pd.DataFrame, funding: pd.DataFrame, *, symbol: str) -> pd.DataFrame:
    required = {"timestamp", "funding_rate_scaled"}
    missing = required - set(funding.columns)
    if missing:
        raise ValueError(f"Funding data missing required columns for {symbol}: {sorted(missing)}")
    if bars["timestamp"].duplicated(keep=False).any():
        raise ValueError(f"Bar timestamps must be unique for funding alignment ({symbol})")

    funding_rates = funding[["timestamp", "funding_rate_scaled"]].copy()
    funding_rates["timestamp"] = _to_utc_ts(funding_rates["timestamp"])
    funding_rates["funding_rate_scaled"] = pd.to_numeric(funding_rates["funding_rate_scaled"], errors="coerce")
    funding_rates = funding_rates.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    if funding_rates["timestamp"].duplicated(keep=False).any():
        raise ValueError(f"Funding timestamps must be unique for {symbol}")

    # Normalize bar timestamps to ns to match funding_rates (parquet may load as us in pandas 2.x)
    bars = bars.copy()
    bars["timestamp"] = bars["timestamp"].astype("datetime64[ns, UTC]")
    # Drop stale funding column if present — merge_asof would suffix-collide it otherwise
    if "funding_rate_scaled" in bars.columns:
        bars = bars.drop(columns=["funding_rate_scaled"])

    expected_rows = int(len(bars))
    aligned = pd.merge_asof(
        bars.sort_values("timestamp"),
        funding_rates.sort_values("timestamp"),
        on="timestamp",
        direction="backward",
        tolerance=FUNDING_MAX_STALENESS,
    )
    if len(aligned) != expected_rows:
        raise ValueError(
            f"Cardinality mismatch after funding alignment for {symbol}: "
            f"expected {expected_rows}, got {len(aligned)}"
        )
    return aligned


def _assert_complete_funding_series(frame: pd.DataFrame, *, symbol: str) -> pd.Series:
    if "funding_rate_scaled" not in frame.columns:
        raise ValueError(f"Missing funding_rate_scaled for {symbol}")
    funding = pd.to_numeric(frame["funding_rate_scaled"], errors="coerce")
    if funding.isna().all():
        raise ValueError(f"Unable to align funding_rate_scaled for {symbol}")

    missing_pct = float(funding.isna().mean()) if len(funding) else 0.0
    if missing_pct > 0.0:
        raise ValueError(
            f"Funding alignment gaps for {symbol}: missing_pct={missing_pct:.4f}; "
            "keep funding gaps explicit and rebuild with complete funding coverage."
        )

    if "funding_missing" in frame.columns:
        funding_missing = frame["funding_missing"].fillna(True).astype(bool)
        flagged_missing_pct = float(funding_missing.mean()) if len(funding_missing) else 0.0
        if flagged_missing_pct > 0.0:
            raise ValueError(
                f"Funding coverage gaps flagged for {symbol}: funding_missing_pct={flagged_missing_pct:.4f}; "
                "rebuild cleaned funding inputs before context generation."
            )
    return funding.astype(float)


def _read_any_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.DataFrame()


def _normalize_raw_funding(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # Normalize column names
    df = df.rename(columns={c: c.strip() for c in df.columns})

    # Find timestamp column
    ts_col = None
    for c in ["timestamp", "fundingTime", "funding_time", "time", "datetime"]:
        if c in df.columns:
            ts_col = c
            break
    if ts_col is None:
        return pd.DataFrame()

    # Find rate column
    rate_col = None
    for c in ["funding_rate_scaled", "fundingRate", "funding_rate", "funding"]:
        if c in df.columns:
            rate_col = c
            break
    if rate_col is None:
        return pd.DataFrame()

    out = pd.DataFrame(
        {
            "timestamp": _to_utc_ts(df[ts_col]),
            "funding_rate_scaled": pd.to_numeric(df[rate_col], errors="coerce"),
        }
    ).dropna(subset=["timestamp"])
    return out


def _find_raw_funding_files(symbol: str) -> List[Path]:
    raw_root = DATA_ROOT / "lake" / "raw"
    if not raw_root.exists():
        return []

    pats = [
        f"*{symbol}*funding*.csv",
        f"*{symbol}*fundingRate*.csv",
        f"*{symbol}*funding*.parquet",
        f"*{symbol}*fundingRate*.parquet",
        # also catch files where symbol is in a parent directory but not filename
        f"*funding*{symbol}*.csv",
        f"*fundingRate*{symbol}*.csv",
        f"*funding*{symbol}*.parquet",
        f"*fundingRate*{symbol}*.parquet",
    ]
    files: List[Path] = []
    for pat in pats:
        files.extend(raw_root.rglob(pat))
    # de-dup + keep only real files
    out = sorted({p for p in files if p.is_file()})
    return out


def _load_funding(*, run_id: str, symbol: str, timeframe: str) -> Tuple[pd.DataFrame, str, int]:
    # 1) run-scoped cleaned funding
    # 2) global cleaned funding
    funding_candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", "perp", symbol, f"funding_{timeframe}"),
        DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / f"funding_{timeframe}",
    ]
    funding_dir = choose_partition_dir(funding_candidates)
    if funding_dir:
        funding = read_parquet(list_parquet_files(funding_dir))
        if not funding.empty:
            if "timestamp" not in funding.columns:
                raise ValueError(f"Funding dataset missing timestamp for {symbol} at timeframe={timeframe}")
            if "funding_rate_scaled" not in funding.columns:
                raise ValueError(f"Funding schema missing funding_rate_scaled for {symbol} at timeframe={timeframe}")

            funding = funding[["timestamp", "funding_rate_scaled"]].copy()
            funding["timestamp"] = _to_utc_ts(funding["timestamp"])
            funding = funding.dropna(subset=["timestamp"])
            funding, funding_dupes = _dedupe_timestamp_rows(funding, label=f"funding:{symbol}:{timeframe}")
            return funding, str(funding_dir), funding_dupes

    # 3) raw fallback: search anywhere under data/lake/raw
    raw_files = _find_raw_funding_files(symbol)
    if raw_files:
        frames: List[pd.DataFrame] = []
        for p in raw_files:
            df = _read_any_table(p)
            norm = _normalize_raw_funding(df)
            if not norm.empty:
                frames.append(norm)

        if frames:
            funding = pd.concat(frames, ignore_index=True)
            funding = funding.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
            funding, funding_dupes = _dedupe_timestamp_rows(funding, label=f"funding_raw:{symbol}")
            return funding, str(DATA_ROOT / "lake" / "raw"), funding_dupes

    return pd.DataFrame(), "", 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Build read-only context features")
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
        feature_schema_version, feature_schema_hash = feature_schema_identity()
        stats["feature_schema_version"] = feature_schema_version
        stats["feature_schema_hash"] = feature_schema_hash

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

            funding_input_path = ""
            funding_dupes = 0

            need_funding_align = (
                ("funding_rate_scaled" not in bars.columns)
                or pd.to_numeric(bars.get("funding_rate_scaled"), errors="coerce").isna().all()
            )

            if need_funding_align:
                funding, funding_input_path, funding_dupes = _load_funding(
                    run_id=args.run_id,
                    symbol=symbol,
                    timeframe=args.timeframe,
                )
                if funding.empty:
                    raise ValueError(f"Missing funding_rate_scaled for {symbol} at timeframe={args.timeframe}")

                bars = _align_funding_to_bars(bars, funding, symbol=symbol)

            bars["funding_missing"] = pd.to_numeric(bars["funding_rate_scaled"], errors="coerce").isna()
            bars["funding_rate_scaled"] = _assert_complete_funding_series(bars, symbol=symbol)

            inputs.append(
                {
                    "path": str(bars_dir),
                    **_collect_stats(bars),
                    "provenance": {
                        "vendor": "binance",
                        "exchange": "binance",
                        "schema_version": "cleaned_bars_5m_v1",
                        "schema_hash": schema_hash_from_columns(bars.columns.tolist()),
                        "extraction_start": bars["timestamp"].min().isoformat(),
                        "extraction_end": bars["timestamp"].max().isoformat(),
                    },
                }
            )

            if funding_input_path:
                funding_non_null = (
                    bars[["timestamp", "funding_rate_scaled"]].dropna(subset=["funding_rate_scaled"]).copy()
                )
                if not funding_non_null.empty:
                    inputs.append(
                        {
                            "path": funding_input_path,
                            **_collect_stats(funding_non_null),
                            "provenance": {
                                "vendor": "binance",
                                "exchange": "binance",
                                "schema_version": "funding_v1",
                                "schema_hash": schema_hash_from_columns(funding_non_null.columns.tolist()),
                                "extraction_start": funding_non_null["timestamp"].min().isoformat(),
                                "extraction_end": funding_non_null["timestamp"].max().isoformat(),
                            },
                        }
                    )

            fp = build_funding_persistence_state(bars, symbol=symbol, config=DEFAULT_FP_CONFIG)
            validate_feature_schema_columns(
                dataset_key="context_funding_persistence_v1",
                columns=fp.columns.tolist(),
            )

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
                "feature_schema_version": feature_schema_version,
                "feature_schema_hash": feature_schema_hash,
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