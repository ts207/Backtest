from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import list_parquet_files, read_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest

LOGGER = logging.getLogger(__name__)

def check_nans(df: pd.DataFrame, threshold: float = 0.05) -> List[str]:
    issues = []
    for col in df.columns:
        nan_pct = df[col].isna().mean()
        if nan_pct > threshold:
            issues.append(f"Column '{col}' has {nan_pct:.2%} NaNs (threshold {threshold:.2%})")
    return issues

def check_constant_values(df: pd.DataFrame) -> List[str]:
    issues = []
    for col in df.select_dtypes(include=[np.number]).columns:
        if df[col].nunique() <= 1 and not df[col].isna().all():
            issues.append(f"Column '{col}' is constant.")
    return issues

def check_outliers(df: pd.DataFrame, z_threshold: float = 10.0) -> List[str]:
    issues = []
    for col in df.select_dtypes(include=[np.number]).columns:
        if col in ['timestamp', 'open', 'high', 'low', 'close', 'volume']:
            continue
        series = df[col].dropna()
        if series.empty:
            continue
        z_scores = (series - series.mean()) / series.std()
        outlier_pct = (z_scores.abs() > z_threshold).mean()
        if outlier_pct > 0.01: # More than 1% are extreme outliers
            issues.append(f"Column '{col}' has {outlier_pct:.2%} extreme outliers (> {z_threshold} sigma)")
    return issues

def validate_symbol(data_root: Path, symbol: str, timeframe: str = "5m") -> Dict[str, List[str]]:
    symbol_issues = {}
    
    # 1. Check cleaned bars
    bars_dir = data_root / "lake" / "cleaned" / "perp" / symbol / f"bars_{timeframe}"
    if bars_dir.exists():
        df_bars = read_parquet(list_parquet_files(bars_dir))
        if not df_bars.empty:
            symbol_issues["bars"] = check_nans(df_bars) + check_constant_values(df_bars)
            
    # 2. Check features
    features_dir = data_root / "lake" / "features" / "perp" / symbol / timeframe / "features_v1"
    if features_dir.exists():
        df_feats = read_parquet(list_parquet_files(features_dir))
        if not df_feats.empty:
            feat_issues = check_nans(df_feats) + check_constant_values(df_feats) + check_outliers(df_feats)
            symbol_issues["features"] = feat_issues
            
    return symbol_issues

def main():
    parser = argparse.ArgumentParser(description="Research Production Grade: Data Integrity Gate")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--nan_threshold", type=float, default=0.05)
    parser.add_argument("--z_threshold", type=float, default=10.0)
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    data_root = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
    
    start_ts = start_manifest(PROJECT_ROOT / "pipelines" / "clean" / "validate_feature_integrity.py", vars(args))
    
    all_issues = {}
    for symbol in symbols:
        LOGGER.info(f"Auditing data integrity for {symbol}...")
        issues = validate_symbol(data_root, symbol)
        if issues:
            all_issues[symbol] = issues

    status = "success"
    if all_issues:
        LOGGER.warning(f"Integrity check found issues in {len(all_issues)} symbols.")
        # We don't necessarily fail the run yet, but we log it for the "Research Ledger"
        status = "warning"

    finalize_manifest(
        run_id=args.run_id,
        stage="validate_feature_integrity",
        status=status,
        start_ts=start_ts,
        artifacts=[],
        metrics={"symbols_with_issues": len(all_issues), "details": all_issues}
    )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
