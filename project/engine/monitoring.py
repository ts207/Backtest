from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


def calculate_psi(
    expected_series: pd.Series,
    actual_series: pd.Series,
    bins: int = 10,
    epsilon: float = 1e-4,
) -> float:
    """
    Calculate the Population Stability Index (PSI) between two distributions.
    
    A PSI < 0.1 indicates no significant population change.
    A PSI between 0.1 and 0.2 indicates a minor shift.
    A PSI > 0.2 indicates a significant population shift.
    """
    expected = pd.to_numeric(expected_series, errors="coerce").dropna().values
    actual = pd.to_numeric(actual_series, errors="coerce").dropna().values

    if len(expected) == 0 or len(actual) == 0:
        return np.nan

    # Compute percentiles for bin edges
    percentiles = np.linspace(0, 100, bins + 1)
    bin_edges = np.percentile(expected, percentiles)
    # Ensure min/max cover the actual data as well to avoid out-of-bounds
    bin_edges[0] = -np.inf
    bin_edges[-1] = np.inf

    # deduplicate bin edges, which can happen if many values are identical
    bin_edges = np.unique(bin_edges)

    expected_counts, _ = np.histogram(expected, bins=bin_edges)
    actual_counts, _ = np.histogram(actual, bins=bin_edges)

    # Convert counts to proportions
    expected_props = expected_counts / len(expected)
    actual_props = actual_counts / len(actual)

    # Avoid zero proportions
    expected_props = np.where(expected_props == 0, epsilon, expected_props)
    actual_props = np.where(actual_props == 0, epsilon, actual_props)

    # Calculate PSI
    psi = np.sum((actual_props - expected_props) * np.log(actual_props / expected_props))
    return float(psi)


def feature_drift_report(
    train_features: pd.DataFrame,
    oos_features: pd.DataFrame,
    feature_cols: List[str] | None = None,
) -> Dict[str, float]:
    """
    Compute PSI for each feature to detect drift in Out-Of-Sample data.
    """
    if feature_cols is None:
        # Infer numeric columns
        feature_cols = [
            c for c in train_features.select_dtypes(include=[np.number]).columns 
            if c not in ("timestamp", "symbol")
        ]
        
    report = {}
    for col in feature_cols:
        if col in train_features.columns and col in oos_features.columns:
            psi = calculate_psi(train_features[col], oos_features[col])
            report[col] = psi
            
    return report


def analyze_slippage_drift(
    theoretical_cost_bps: pd.Series,
    realized_cost_bps: pd.Series,
) -> Dict[str, float]:
    """
    Compare backtested execution costs with theoretically realized production costs.
    """
    theo = pd.to_numeric(theoretical_cost_bps, errors="coerce").fillna(0.0)
    real = pd.to_numeric(realized_cost_bps, errors="coerce").fillna(0.0)
    
    diff = real - theo
    
    return {
        "mean_slippage_error_bps": float(diff.mean()),
        "p95_slippage_error_bps": float(diff.quantile(0.95)),
        "max_slippage_error_bps": float(diff.max()),
        "underestimation_fraction": float((diff > 0).mean()),
    }


def detect_tail_events(
    returns_series: pd.Series,
    sigma_threshold: float = 4.0,
) -> Dict[str, object]:
    """
    Detect anomalous tail events in a returns series.
    """
    ret = pd.to_numeric(returns_series, errors="coerce").dropna()
    if ret.empty:
        return {"tail_event_count": 0, "anomalies": []}
        
    mean = ret.mean()
    std = ret.std()
    
    if std == 0:
        return {"tail_event_count": 0, "anomalies": []}
        
    z_scores = (ret - mean) / std
    anomalies = ret[z_scores.abs() > sigma_threshold]
    
    return {
        "tail_event_count": len(anomalies),
        "mean": float(mean),
        "std_dev": float(std),
        "anomalies_idx": [str(idx) for idx in anomalies.index.tolist()],
        "anomalies_val": [float(val) for val in anomalies.tolist()],
    }
