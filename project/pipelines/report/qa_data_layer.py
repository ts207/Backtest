from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import choose_partition_dir, ensure_dir, list_parquet_files, read_parquet, run_scoped_lake_path
from pipelines._lib.run_manifest import finalize_manifest, start_manifest

# ---------------------------------------------------------------------------
# QC severity type alias and check functions
# ---------------------------------------------------------------------------

QCSeverity = str  # "ok" | "warning" | "critical"


def check_return_outliers(
    close: pd.Series,
    z_threshold: float = 5.0,
    critical_count: int = 5,
) -> Dict[str, object]:
    """Check for return outliers where |z-score| > z_threshold.

    Returns a dict with keys: outlier_count, severity.
    """
    ret = close.pct_change().dropna()
    if ret.empty:
        return {"outlier_count": 0, "severity": "ok"}
    z = (ret - ret.mean()) / (ret.std() + 1e-10)
    outlier_count = int((z.abs() > z_threshold).sum())
    severity: QCSeverity = "ok"
    if outlier_count >= critical_count:
        severity = "critical"
    elif outlier_count > 0:
        severity = "warning"
    return {"outlier_count": outlier_count, "severity": severity}


def check_duplicate_bars(timestamps: pd.Series) -> Dict[str, object]:
    """Check for duplicate timestamps in a bar series.

    Returns a dict with keys: duplicate_count, severity.
    """
    dup_count = int(timestamps.duplicated().sum())
    severity: QCSeverity = (
        "ok" if dup_count == 0 else ("critical" if dup_count > 5 else "warning")
    )
    return {"duplicate_count": dup_count, "severity": severity}


def check_funding_gaps(
    funding_ts: pd.DatetimeIndex | pd.Series,
    expected_interval_hours: float = 8,
) -> Dict[str, object]:
    """Check for gaps larger than 1.5x the expected funding interval.

    Returns a dict with keys: gap_count, max_gap_hours, severity.
    """
    ts = pd.Series(pd.to_datetime(funding_ts, utc=True)).sort_values().dropna().reset_index(drop=True)
    if len(ts) < 2:
        return {"gap_count": 0, "max_gap_hours": 0.0, "severity": "ok"}
    gaps_hours = ts.diff().dropna().dt.total_seconds() / 3600.0
    large_gaps = int((gaps_hours > expected_interval_hours * 1.5).sum())
    max_gap = float(gaps_hours.max())
    severity: QCSeverity = (
        "ok" if large_gaps == 0 else ("critical" if large_gaps > 3 else "warning")
    )
    return {"gap_count": large_gaps, "max_gap_hours": max_gap, "severity": severity}


# ---------------------------------------------------------------------------
# Existing per-dataset quality helper
# ---------------------------------------------------------------------------


def _check_quality(df: pd.DataFrame, name: str) -> Dict[str, object]:
    if df.empty:
        return {"status": "empty"}
    
    ts = pd.to_datetime(df["timestamp"], utc=True)
    gaps = ts.diff() > ts.diff().min() # simple gap check
    gap_count = int(gaps.sum())
    
    return {
        "rows": int(len(df)),
        "start": ts.min().isoformat(),
        "end": ts.max().isoformat(),
        "gap_count": gap_count,
        "is_monotonic": bool(ts.is_monotonic_increasing),
        "duplicate_count": int(ts.duplicated().sum()),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="QA Data Layer (Slice 1)")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    manifest = start_manifest("qa_data_layer", args.run_id, {"symbols": symbols}, [], [])
    stats: Dict[str, object] = {"symbols": {}}

    try:
        for symbol in symbols:
            symbol_stats = {}
            
            # Check bars_1m_perp
            perp_bars_dir = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "bars_1m"
            perp_bars = read_parquet(list_parquet_files(perp_bars_dir))
            symbol_stats["perp_bars_1m"] = _check_quality(perp_bars, "perp_bars_1m")
            
            # Check bars_1m_spot
            spot_bars_dir = DATA_ROOT / "lake" / "cleaned" / "spot" / symbol / "bars_1m"
            spot_bars = read_parquet(list_parquet_files(spot_bars_dir))
            symbol_stats["spot_bars_1m"] = _check_quality(spot_bars, "spot_bars_1m")
            
            # Check tob_1m_agg
            tob_agg_dir = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "tob_1m_agg"
            tob_agg = read_parquet(list_parquet_files(tob_agg_dir))
            symbol_stats["tob_1m_agg"] = _check_quality(tob_agg, "tob_1m_agg")
            
            # Check basis_1m
            basis_dir = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "basis_1m"
            basis = read_parquet(list_parquet_files(basis_dir))
            symbol_stats["basis_1m"] = _check_quality(basis, "basis_1m")
            
            stats["symbols"][symbol] = symbol_stats

        report_dir = DATA_ROOT / "reports" / "qa" / args.run_id
        ensure_dir(report_dir)
        report_path = report_dir / "slice1_data_integrity.json"
        report_path.write_text(json.dumps(stats, indent=2, sort_keys=True), encoding="utf-8")

        # ------------------------------------------------------------------
        # Formal QC report — written to data/reports/qc/<run_id>/qc_summary.json
        # Fail-closed on any critical severity (contract #5).
        # ------------------------------------------------------------------
        qc_results: Dict[str, object] = {}
        for symbol, sym_stats in stats["symbols"].items():
            symbol_qc: Dict[str, object] = {}

            # Return outlier check on perp bars close prices
            perp_bars_dir = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "bars_1m"
            perp_bars = read_parquet(list_parquet_files(perp_bars_dir))
            if not perp_bars.empty and "close" in perp_bars.columns:
                close_series = pd.to_numeric(perp_bars["close"], errors="coerce").dropna()
                symbol_qc["return_outliers"] = check_return_outliers(close_series)
            else:
                symbol_qc["return_outliers"] = {"outlier_count": 0, "severity": "ok", "note": "no_close_column"}

            # Duplicate bars check on perp bars timestamps
            if not perp_bars.empty and "timestamp" in perp_bars.columns:
                ts_series = pd.to_datetime(perp_bars["timestamp"], utc=True)
                symbol_qc["duplicate_bars"] = check_duplicate_bars(ts_series)
            else:
                symbol_qc["duplicate_bars"] = {"duplicate_count": 0, "severity": "ok", "note": "no_timestamp_column"}

            # Missing expected partitions: reuse gap_count from existing quality check
            perp_q = sym_stats.get("perp_bars_1m", {})
            symbol_qc["missing_partitions"] = {
                "gap_count": perp_q.get("gap_count", 0),
                "is_monotonic": perp_q.get("is_monotonic", True),
                "severity": (
                    "critical"
                    if not perp_q.get("is_monotonic", True)
                    else ("warning" if perp_q.get("gap_count", 0) > 0 else "ok")
                ),
            }

            # Funding gap check
            funding_dir = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "funding_rate"
            funding_files = list_parquet_files(funding_dir)
            if funding_files:
                funding_df = read_parquet(funding_files)
                if not funding_df.empty and "timestamp" in funding_df.columns:
                    funding_ts = pd.to_datetime(funding_df["timestamp"], utc=True)
                    symbol_qc["funding_gaps"] = check_funding_gaps(funding_ts)
                else:
                    symbol_qc["funding_gaps"] = {"gap_count": 0, "max_gap_hours": 0.0, "severity": "ok", "note": "no_funding_data"}
            else:
                symbol_qc["funding_gaps"] = {"gap_count": 0, "max_gap_hours": 0.0, "severity": "ok", "note": "no_funding_files"}

            # OI discontinuities: check for large step-changes in open interest
            oi_dir = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "oi_1m"
            oi_files = list_parquet_files(oi_dir)
            if oi_files:
                oi_df = read_parquet(oi_files)
                if not oi_df.empty and "open_interest" in oi_df.columns:
                    oi_series = pd.to_numeric(oi_df["open_interest"], errors="coerce").dropna()
                    symbol_qc["oi_discontinuities"] = check_return_outliers(
                        oi_series, z_threshold=5.0, critical_count=5
                    )
                else:
                    symbol_qc["oi_discontinuities"] = {"outlier_count": 0, "severity": "ok", "note": "no_oi_column"}
            else:
                symbol_qc["oi_discontinuities"] = {"outlier_count": 0, "severity": "ok", "note": "no_oi_files"}

            qc_results[symbol] = symbol_qc

        qc_summary = {"run_id": args.run_id, "symbols": qc_results}

        qc_report_dir = DATA_ROOT / "reports" / "qc" / args.run_id
        ensure_dir(qc_report_dir)
        qc_summary_path = qc_report_dir / "qc_summary.json"
        qc_summary_path.write_text(json.dumps(qc_summary, indent=2, sort_keys=True), encoding="utf-8")
        logging.info("QC summary written to %s", qc_summary_path)

        # Fail-closed: raise SystemExit(1) on any critical severity
        critical_checks: List[str] = []
        for symbol, sym_qc in qc_results.items():
            for check_name, check_result in sym_qc.items():
                if isinstance(check_result, dict) and check_result.get("severity") == "critical":
                    critical_checks.append(f"{symbol}.{check_name}")

        if critical_checks:
            logging.error("Critical QC anomalies detected: %s", critical_checks)
            finalize_manifest(manifest, "failed", error="critical_qc_anomalies", stats=stats)
            raise SystemExit(1)

        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except SystemExit:
        raise
    except Exception as exc:
        logging.exception("QA failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
