"""
monitor_feature_drift.py
========================
Computes Population Stability Index (PSI) for key engineered features
between a reference (training) window and a monitoring (live) window.
PSI > 0.25 triggers a WARNING; PSI > 0.50 triggers ERROR and exit code 1.

Usage:
    python project/scripts/monitor_feature_drift.py \\
        --run_id my_run --symbol BTCUSDT \\
        --ref_start 2024-01-01 --ref_end 2024-06-30 \\
        --live_start 2024-07-01 --live_end 2024-09-30 \\
        [--timeframe 5m] [--n_bins 10]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))

MONITOR_FEATURES = [
    "atr_14",
    "funding_rate_scaled",
    "vol_zscore",
    "spread_bps",
    "quote_volume",
]

PSI_WARN_THRESHOLD = 0.25
PSI_ERROR_THRESHOLD = 0.50


def _compute_psi(ref: pd.Series, live: pd.Series, n_bins: int = 10) -> float:
    """Compute Population Stability Index between ref and live distributions."""
    ref = ref.dropna()
    live = live.dropna()
    if ref.empty or live.empty:
        return 0.0
    bins = np.percentile(ref, np.linspace(0, 100, n_bins + 1))
    bins = np.unique(bins)
    if len(bins) < 2:
        return 0.0
    ref_counts, _ = np.histogram(ref, bins=bins)
    live_counts, _ = np.histogram(live, bins=bins)
    ref_pct = (ref_counts / max(ref_counts.sum(), 1)).clip(1e-9)
    live_pct = (live_counts / max(live_counts.sum(), 1)).clip(1e-9)
    psi = float(np.sum((live_pct - ref_pct) * np.log(live_pct / ref_pct)))
    return psi


def _load_features(run_id: str, symbol: str, timeframe: str) -> pd.DataFrame:
    candidates = [
        DATA_ROOT / "lake" / "runs" / run_id / "features" / "perp" / symbol / timeframe / "features_v1",
        DATA_ROOT / "lake" / "features" / "perp" / symbol / timeframe / "features_v1",
    ]
    for d in candidates:
        if d.exists():
            files = sorted(d.rglob("*.parquet"))
            if files:
                return pd.read_parquet(files)
    return pd.DataFrame()


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor feature distribution drift via PSI.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--ref_start", required=True, help="Reference window start (YYYY-MM-DD)")
    parser.add_argument("--ref_end", required=True, help="Reference window end (YYYY-MM-DD)")
    parser.add_argument("--live_start", required=True, help="Live window start (YYYY-MM-DD)")
    parser.add_argument("--live_end", required=True, help="Live window end (YYYY-MM-DD)")
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--n_bins", type=int, default=10)
    args = parser.parse_args()

    features = _load_features(args.run_id, args.symbol.upper(), args.timeframe)
    if features.empty:
        print(f"[drift] ERROR: no features found for run_id={args.run_id} symbol={args.symbol}", file=sys.stderr)
        return 1

    features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)
    ref = features[(features["timestamp"] >= pd.Timestamp(args.ref_start, tz="UTC")) & (features["timestamp"] <= pd.Timestamp(args.ref_end, tz="UTC"))]
    live = features[(features["timestamp"] >= pd.Timestamp(args.live_start, tz="UTC")) & (features["timestamp"] <= pd.Timestamp(args.live_end, tz="UTC"))]

    any_error = False
    for feat in MONITOR_FEATURES:
        if feat not in features.columns:
            print(f"[drift] SKIP {feat}: not in feature schema")
            continue
        psi = _compute_psi(ref[feat], live[feat], n_bins=args.n_bins)
        if psi >= PSI_ERROR_THRESHOLD:
            level = "ERROR"
            any_error = True
        elif psi >= PSI_WARN_THRESHOLD:
            level = "WARN"
        else:
            level = "OK"
        print(f"[drift][{level}] {args.symbol} | {feat}: PSI={psi:.4f}", file=sys.stderr if level == "ERROR" else sys.stdout)

    return 1 if any_error else 0


if __name__ == "__main__":
    sys.exit(main())
