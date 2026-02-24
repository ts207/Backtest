from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional

import numpy as np
import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))
from pipelines._lib.timeframe_constants import DEFAULT_EVENT_HORIZON_BARS


def _load_gates_spec() -> Dict[str, Any]:
    path = PROJECT_ROOT / "spec" / "gates.yaml"
    if not path.exists():
        return {}
    with open(path, "r") as f:
        return yaml.safe_load(f)


from pipelines._lib.io_utils import (
    ensure_dir,
    read_parquet,
    list_parquet_files,
    run_scoped_lake_path,
    choose_partition_dir,
)


def _default_horizons_bars_csv() -> str:
    return ",".join(str(int(x)) for x in DEFAULT_EVENT_HORIZON_BARS)


def _load_bars(run_id: str, symbol: str, timeframe: str = "5m") -> pd.DataFrame:
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", "perp", symbol, f"bars_{timeframe}"),
        DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / f"bars_{timeframe}",
    ]
    bars_dir = choose_partition_dir(candidates)
    if not bars_dir:
        return pd.DataFrame()
    files = list_parquet_files(bars_dir)
    if not files:
        return pd.DataFrame()
    return read_parquet(files)


def _load_features(run_id: str, symbol: str) -> pd.DataFrame:
    """Load PIT features table for join-rate computation."""
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "features", "perp", symbol, "5m", "features_v1"),
        DATA_ROOT / "lake" / "features" / "perp" / symbol / "5m" / "features_v1",
    ]
    features_dir = choose_partition_dir(candidates)
    if not features_dir:
        return pd.DataFrame()
    files = list_parquet_files(features_dir)
    if not files:
        return pd.DataFrame()
    df = read_parquet(files)
    if df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    return df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)


def _compute_join_rate(
    events_df: pd.DataFrame,
    features_df: pd.DataFrame,
    horizons_bars: List[int],
) -> Dict[str, float]:
    """
    Join-rate computation using merge_asof(direction="backward").

    Feature join rate
    ----------------
    Each event is matched to the most-recent feature bar whose timestamp is
    <= the event timestamp.  This handles the common case where events carry
    sub-bar timestamps (e.g. 10:03:17) that will never match a bar boundary
    (10:00:00) under exact equality.

    Label join rate (per horizon h)
    --------------------------------
    Starting from the matched bar's *integer position* in features_df, we
    check that bar at position (matched_pos + h) exists and has a finite
    close price.  We derive matched_pos from the merge_asof result rather
    than re-searching by timestamp, which avoids the off-by-one that
    searchsorted(side="left") introduces for unaligned timestamps.
    """
    null_result: Dict[str, float] = {"features": 0.0, **{f"label_{h}b": 0.0 for h in horizons_bars}}

    if events_df.empty or features_df.empty:
        return null_result

    ts_col = "timestamp" if "timestamp" in events_df.columns else "enter_ts"
    if ts_col not in events_df.columns:
        return null_result

    # Build a single-column events frame sorted by timestamp.
    evt_ts = pd.to_datetime(events_df[ts_col], utc=True, errors="coerce")
    evt_frame = pd.DataFrame({"timestamp": evt_ts}).dropna().sort_values("timestamp").reset_index(drop=True)
    n_events = len(evt_frame)
    if n_events == 0:
        return null_result

    # Stamp each feature row with its integer position so we can recover it
    # after the merge without a second searchsorted call.
    feat = features_df.copy().sort_values("timestamp").reset_index(drop=True)
    feat["_feat_pos"] = feat.index  # 0-based integer position in feat

    # merge_asof(direction="backward"): for each event find the latest bar
    # whose timestamp <= event timestamp.
    merged = pd.merge_asof(
        evt_frame,
        feat[["timestamp", "_feat_pos", "close"] if "close" in feat.columns else ["timestamp", "_feat_pos"]],
        on="timestamp",
        direction="backward",
    )

    # Feature join rate: rows where merge found a match (_feat_pos is not NaN).
    feat_joined = merged["_feat_pos"].notna().sum()
    feature_join_rate = float(feat_joined / n_events)

    # Label join rate per horizon.
    has_close = "close" in feat.columns
    close_arr = feat["close"].to_numpy(dtype=float) if has_close else None
    n_feat = len(feat)

    label_rates: Dict[str, float] = {}
    for h in horizons_bars:
        if not has_close or close_arr is None:
            label_rates[f"label_{h}b"] = 0.0
            continue
        valid = 0
        for _, row in merged.iterrows():
            pos = row["_feat_pos"]
            if pd.isna(pos):
                continue
            future_pos = int(pos) + h
            if future_pos < n_feat and np.isfinite(close_arr[future_pos]):
                valid += 1
        label_rates[f"label_{h}b"] = float(valid / n_events)

    return {"features": feature_join_rate, **label_rates}


def _compute_sensitivity(
    events_df: pd.DataFrame,
    severity_cols: List[str],
    pct_delta: float = 0.10,
) -> Dict[str, float]:
    """
    Real sensitivity sweep: vary the effective event threshold by ±pct_delta
    and measure how prevalence changes.  Uses severity/magnitude columns in
    the events_df if present; falls back to a rank-based approximation.

    Returns prevalence_elasticity = |%Δ events / %Δ threshold|.
    """
    if events_df.empty:
        return {
            "threshold_delta_pct": [-pct_delta * 100, pct_delta * 100],
            "prevalence_stability_index": float("nan"),
            "prevalence_elasticity": float("nan"),
            "sensitivity_method": "no_events",
        }

    n_base = len(events_df)

    # Find a numeric severity column to use as a proxy for the event threshold
    severity_col: Optional[str] = None
    for col in severity_cols:
        if col in events_df.columns:
            arr = pd.to_numeric(events_df[col], errors="coerce").dropna()
            if len(arr) > 0:
                severity_col = col
                break

    if severity_col is None:
        # No severity column: use rank-based approximation.
        # Removing the bottom pct_delta fraction ≈ tightening threshold.
        n_tight = int(np.floor(n_base * (1.0 - pct_delta)))
        n_loose = n_base  # can't loosen without re-running detector
        elasticity = abs((n_tight - n_base) / n_base / pct_delta) if n_base > 0 else float("nan")
        return {
            "threshold_delta_pct": [-pct_delta * 100, pct_delta * 100],
            "prevalence_stability_index": float(n_tight / n_base) if n_base > 0 else float("nan"),
            "prevalence_elasticity": float(elasticity),
            "sensitivity_method": "rank_approximation",
        }

    sev = pd.to_numeric(events_df[severity_col], errors="coerce").dropna()
    threshold_base = float(sev.median())
    if threshold_base == 0.0:
        threshold_base = float(sev.mean()) or 1.0

    threshold_tight = threshold_base * (1.0 + pct_delta)
    threshold_loose = threshold_base * (1.0 - pct_delta)

    n_tight = int((sev >= threshold_tight).sum())
    n_loose = int((sev >= threshold_loose).sum())

    if n_base > 0:
        elasticity_tight = abs((n_tight - n_base) / n_base / pct_delta) if pct_delta != 0 else 0.0
        elasticity_loose = abs((n_loose - n_base) / n_base / pct_delta) if pct_delta != 0 else 0.0
        elasticity = float(max(elasticity_tight, elasticity_loose))
        stability_index = float(n_tight / n_base)
    else:
        elasticity = float("nan")
        stability_index = float("nan")

    return {
        "threshold_delta_pct": [-pct_delta * 100, pct_delta * 100],
        "prevalence_stability_index": stability_index,
        "prevalence_elasticity": elasticity,
        "sensitivity_method": f"severity_col:{severity_col}",
    }


# Known severity columns per event type (in priority order)
_SEVERITY_COLS: Dict[str, List[str]] = {
    "VOL_SHOCK": ["rv_shock_magnitude", "shock_severity", "range_pct"],
    "LIQUIDITY_VACUUM": ["depth_drop_pct", "stress_score", "spread_bps"],
    "FORCED_FLOW_EXHAUSTION": ["exhaustion_score", "range_pct"],
    "CROSS_VENUE_DESYNC": ["basis_bps", "desync_magnitude"],
    "FUNDING_EXTREME_ONSET": ["episode_magnitude", "funding_rate_abs"],
    "FUNDING_PERSISTENCE_TRIGGER": ["episode_magnitude", "funding_rate_abs"],
    "FUNDING_NORMALIZATION_TRIGGER": ["episode_magnitude", "funding_rate_abs"],
    "OI_SPIKE_POSITIVE": ["oi_z", "oi_pct_change"],
    "OI_SPIKE_NEGATIVE": ["oi_z", "oi_pct_change"],
    "OI_FLUSH": ["oi_z", "oi_pct_change"],
    "LIQUIDATION_CASCADE": ["liquidation_notional", "liquidation_count"],
}


def validate_event_quality(
    events_df: pd.DataFrame,
    bars_df: pd.DataFrame,
    event_type: str,
    symbol: str,
    run_id: str,
    timeframe: str = "5m",
    horizons_bars: Optional[List[int]] = None,
) -> Dict[str, Any]:
    if horizons_bars is None:
        horizons_bars = list(DEFAULT_EVENT_HORIZON_BARS)

    if events_df.empty:
        return {"pass": False, "reason": "No events detected"}

    # 1. Prevalence
    total_bars = len(bars_df)
    total_events = len(events_df)
    events_per_10k = (total_events / total_bars) * 10000 if total_bars > 0 else 0

    bars_per_day = 288 if timeframe == "5m" else 96
    days = total_bars / bars_per_day if bars_per_day > 0 else 1
    events_per_day = total_events / days if days > 0 else 0

    # 2. Clustering (dedup efficacy)
    events_df = events_df.sort_values("enter_idx")
    diffs = events_df["enter_idx"].diff().dropna()
    clustering_5 = float((diffs <= 5).mean()) if not diffs.empty else 0.0

    # 3. Join Rate — real join against features + labels
    features_df = _load_features(run_id, symbol)
    join_metrics = _compute_join_rate(events_df, features_df, horizons_bars)
    # Primary join rate = feature join rate
    join_rate = join_metrics.get("features", 0.0)

    # 4. Sensitivity Sweep — real threshold perturbation via severity columns
    severity_cols = _SEVERITY_COLS.get(event_type, ["severity", "magnitude", "score"])
    sensitivity = _compute_sensitivity(events_df, severity_cols)

    # 5. Hard Fail Rules (Gate E-1)
    gates = _load_gates_spec().get("gate_e1", {})
    min_prev = gates.get("min_prevalence_10k", 1.0)
    max_prev = gates.get("max_prevalence_10k", 500.0)
    min_join = gates.get("min_join_rate", 0.99)
    max_clust = gates.get("max_clustering_5b", 0.20)
    max_elasticity = gates.get("max_prevalence_elasticity", 2.0)

    fail_reasons = []
    if not (min_prev <= events_per_10k <= max_prev):
        fail_reasons.append(f"PREVALENCE_OUT_OF_BOUNDS ({events_per_10k:.2f} not in [{min_prev}, {max_prev}])")
    if join_rate < min_join:
        fail_reasons.append(f"LOW_JOIN_RATE ({join_rate:.4f} < {min_join})")
    if clustering_5 > max_clust:
        fail_reasons.append(f"EXCESSIVE_CLUSTERING ({clustering_5:.4f} > {max_clust})")
    elasticity = sensitivity.get("prevalence_elasticity", float("nan"))
    if not pd.isna(elasticity) and elasticity > max_elasticity:
        fail_reasons.append(f"HIGH_ELASTICITY ({elasticity:.2f} > {max_elasticity})")

    report = {
        "event_type": event_type,
        "symbol": symbol,
        "metrics": {
            "total_events": total_events,
            "events_per_day": float(events_per_day),
            "events_per_10k_bars": float(events_per_10k),
            "clustering_ratio_5b": float(clustering_5),
            "join_rate_features": float(join_metrics.get("features", 0.0)),
            "join_rate": float(join_rate),
            **{k: float(v) for k, v in join_metrics.items() if k != "features"},
        },
        "thresholds": {
            "min_prevalence_10k": min_prev,
            "max_prevalence_10k": max_prev,
            "min_join_rate": min_join,
            "max_clustering_5b": max_clust,
            "max_prevalence_elasticity": max_elasticity,
        },
        "sensitivity_sweep": sensitivity,
        "gate_e1_pass": len(fail_reasons) == 0,
        "fail_reasons": fail_reasons,
    }
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--event_type", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument(
        "--horizons_bars",
        default=_default_horizons_bars_csv(),
        help="Forward horizons in bars for label join-rate check (default: derived from timeframe constants)",
    )
    args = parser.parse_args()

    horizons_bars = [int(x.strip()) for x in args.horizons_bars.split(",") if x.strip()]
    symbols = [s.strip() for s in args.symbols.split(",")]

    reports_root = DATA_ROOT / "reports" / args.event_type / args.run_id
    events_path = reports_root / f"{args.event_type}_events.csv"

    if not events_path.exists():
        print(f"Events file not found: {events_path}")
        sys.exit(1)

    events_df = pd.read_csv(events_path)
    reports = []
    overall_pass = True

    for symbol in symbols:
        bars = _load_bars(args.run_id, symbol, args.timeframe)
        sym_events = events_df[events_df["symbol"] == symbol] if "symbol" in events_df.columns else events_df
        report = validate_event_quality(
            sym_events, bars, args.event_type, symbol, args.run_id,
            timeframe=args.timeframe, horizons_bars=horizons_bars,
        )
        reports.append(report)
        if not report.get("gate_e1_pass", False):
            overall_pass = False

    quality_report_path = reports_root / "event_quality_report.json"
    with open(quality_report_path, "w") as f:
        json.dump(reports, f, indent=2)

    print(f"Event Quality Report written to {quality_report_path}")
    if not overall_pass:
        print("GATE E-1 FAILED for one or more symbols.")
        sys.exit(1)


if __name__ == "__main__":
    main()
