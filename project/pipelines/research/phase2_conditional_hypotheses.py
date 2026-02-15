from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Tuple

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
)
from pipelines.research.analyze_conditional_expectancy import (
    _bh_adjust,
    _two_sided_p_from_t,
    build_walk_forward_split_labels,
)


PRIMARY_OUTPUT_COLUMNS = [
    "candidate_id",
    "condition",
    "condition_desc",
    "action",
    "action_family",
    "sample_size",
    "train_samples",
    "validation_samples",
    "test_samples",
    "baseline_mode",
    "delta_adverse_mean",
    "delta_adverse_ci_low",
    "delta_adverse_ci_high",
    "delta_opportunity_mean",
    "delta_opportunity_ci_low",
    "delta_opportunity_ci_high",
    "delta_exposure_mean",
    "opportunity_forward_mean",
    "opportunity_tail_mean",
    "opportunity_composite_mean",
    "opportunity_cost_mean",
    "net_benefit_mean",
    "expectancy_per_trade",
    "robustness_score",
    "event_frequency",
    "capacity_proxy",
    "profit_density_score",
    "gate_a_ci_separated",
    "gate_b_time_stable",
    "gate_b_year_signs",
    "gate_c_regime_stable",
    "gate_c_stable_splits",
    "gate_c_required_splits",
    "gate_d_friction_floor",
    "gate_f_exposure_guard",
    "gate_g_net_benefit",
    "gate_e_simplicity",
    "validation_delta_adverse_mean",
    "test_delta_adverse_mean",
    "test_t_stat",
    "test_p_value",
    "test_p_value_adj_bh",
    "gate_oos_validation_test",
    "gate_multiplicity",
    "gate_pass",
    "gate_all",
    "fail_reasons",
]

SYMBOL_OUTPUT_COLUMNS = [
    "candidate_id",
    "event_type",
    "condition",
    "action",
    "symbol",
    "sample_size",
    "ev",
    "variance",
    "sharpe_like",
    "stability_score",
    "window_consistency",
    "sign_persistence",
    "drawdown_profile",
    "capacity_proxy",
    "rejection_reason_codes",
    "promotion_status",
    "deployable",
]

REJECTION_NEGATIVE_EV = "NEGATIVE_EV"
REJECTION_UNSTABLE = "UNSTABLE"
REJECTION_LOW_SAMPLE = "LOW_SAMPLE"

DEPLOYMENT_MODE_CONCENTRATE = "concentrate"
DEPLOYMENT_MODE_DIVERSIFY = "diversify"


@dataclass(frozen=True)
class Phase1EventSource:
    event_type: str
    reports_dir: str
    events_file: str
    controls_file: str
    summary_file: str


PHASE1_EVENT_SOURCES: Dict[str, Phase1EventSource] = {
    "vol_shock_relaxation": Phase1EventSource(
        event_type="vol_shock_relaxation",
        reports_dir="vol_shock_relaxation",
        events_file="vol_shock_relaxation_events.csv",
        controls_file="vol_shock_relaxation_controls.csv",
        summary_file="vol_shock_relaxation_summary.json",
    ),
    "liquidity_refill_lag_window": Phase1EventSource(
        event_type="liquidity_refill_lag_window",
        reports_dir="liquidity_refill_lag_window",
        events_file="liquidity_refill_lag_window_events.csv",
        controls_file="liquidity_refill_lag_window_controls.csv",
        summary_file="liquidity_refill_lag_window_summary.json",
    ),
    "liquidity_absence_window": Phase1EventSource(
        event_type="liquidity_absence_window",
        reports_dir="liquidity_absence_window",
        events_file="liquidity_absence_window_events.csv",
        controls_file="liquidity_absence_window_controls.csv",
        summary_file="liquidity_absence_window_summary.json",
    ),
    "vol_aftershock_window": Phase1EventSource(
        event_type="vol_aftershock_window",
        reports_dir="vol_aftershock_window",
        events_file="vol_aftershock_window_events.csv",
        controls_file="vol_aftershock_window_controls.csv",
        summary_file="vol_aftershock_window_summary.json",
    ),
    "directional_exhaustion_after_forced_flow": Phase1EventSource(
        event_type="directional_exhaustion_after_forced_flow",
        reports_dir="directional_exhaustion_after_forced_flow",
        events_file="directional_exhaustion_after_forced_flow_events.csv",
        controls_file="directional_exhaustion_after_forced_flow_controls.csv",
        summary_file="directional_exhaustion_after_forced_flow_summary.json",
    ),
    "cross_venue_desync": Phase1EventSource(
        event_type="cross_venue_desync",
        reports_dir="cross_venue_desync",
        events_file="cross_venue_desync_events.csv",
        controls_file="cross_venue_desync_controls.csv",
        summary_file="cross_venue_desync_summary.json",
    ),
    "liquidity_vacuum": Phase1EventSource(
        event_type="liquidity_vacuum",
        reports_dir="liquidity_vacuum",
        events_file="liquidity_vacuum_events.csv",
        controls_file="liquidity_vacuum_controls.csv",
        summary_file="liquidity_vacuum_summary.json",
    ),
    "funding_extreme_reversal_window": Phase1EventSource(
        event_type="funding_extreme_reversal_window",
        reports_dir="funding_extreme_reversal_window",
        events_file="funding_extreme_reversal_window_events.csv",
        controls_file="funding_extreme_reversal_window_controls.csv",
        summary_file="funding_extreme_reversal_window_summary.json",
    ),
    "range_compression_breakout_window": Phase1EventSource(
        event_type="range_compression_breakout_window",
        reports_dir="range_compression_breakout_window",
        events_file="range_compression_breakout_window_events.csv",
        controls_file="range_compression_breakout_window_controls.csv",
        summary_file="range_compression_breakout_window_summary.json",
    ),
}


def _first_existing(df: pd.DataFrame, candidates: List[str]) -> str | None:
    for name in candidates:
        if name in df.columns:
            return name
    return None


def _numeric_non_negative(df: pd.DataFrame, col: str | None, n: int) -> pd.Series:
    if col is None:
        return pd.Series(0.0, index=df.index)
    out = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float).clip(lower=0.0)
    if len(out) != n:
        return pd.Series(0.0, index=df.index)
    return out


def _numeric_any(df: pd.DataFrame, col: str | None, n: int, default: float = np.nan) -> pd.Series:
    if col is None:
        return pd.Series(default, index=df.index)
    out = pd.to_numeric(df[col], errors="coerce").astype(float)
    if len(out) != n:
        return pd.Series(default, index=df.index)
    return out


def _normalize_phase1_frames(events: pd.DataFrame, controls: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    out_events = events.copy()
    out_controls = controls.copy()

    if "event_id" not in out_events.columns:
        if "parent_event_id" in out_events.columns:
            out_events["event_id"] = out_events["parent_event_id"].astype(str)
        else:
            out_events["event_id"] = [f"event_{i:08d}" for i in range(len(out_events))]

    if "event_id" not in out_controls.columns and "parent_event_id" in out_controls.columns:
        out_controls["event_id"] = out_controls["parent_event_id"].astype(str)

    if "enter_ts" not in out_events.columns:
        for col in ["anchor_ts", "timestamp"]:
            if col in out_events.columns:
                out_events["enter_ts"] = out_events[col]
                break
    if "enter_idx" not in out_events.columns and "start_idx" in out_events.columns:
        out_events["enter_idx"] = pd.to_numeric(out_events["start_idx"], errors="coerce")

    if "enter_ts" in out_events.columns:
        out_events["enter_ts"] = pd.to_datetime(out_events["enter_ts"], utc=True, errors="coerce")
    else:
        out_events["enter_ts"] = pd.NaT

    if "year" not in out_events.columns:
        out_events["year"] = out_events["enter_ts"].dt.year
    out_events["year"] = pd.to_numeric(out_events["year"], errors="coerce").fillna(0).astype(int)
    return out_events, out_controls


def _phase1_pass_status(summary_path: Path, require_phase1_pass: bool) -> Tuple[bool, bool, str]:
    phase1_pass = True
    phase1_structure_pass = True
    phase1_decision = "not_required"
    if not require_phase1_pass:
        return phase1_pass, phase1_structure_pass, phase1_decision

    if not summary_path.exists():
        return False, False, "missing_summary"

    try:
        phase1_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False, False, "invalid_summary"

    phase1_decision = str(phase1_payload.get("decision", "unknown"))
    gates = phase1_payload.get("gates", {}) if isinstance(phase1_payload, dict) else {}
    if "phase1_structure_pass" in phase1_payload:
        phase1_structure_pass = bool(phase1_payload.get("phase1_structure_pass"))
    elif isinstance(gates, dict) and "phase1_structure_pass" in gates:
        phase1_structure_pass = bool(gates.get("phase1_structure_pass"))
    elif phase1_decision.lower() in {"promote", "pass"}:
        phase1_structure_pass = True
    elif "event_count" in phase1_payload:
        phase1_structure_pass = bool(int(phase1_payload.get("event_count", 0)) > 0)
    elif "events" in phase1_payload:
        phase1_structure_pass = bool(int(phase1_payload.get("events", 0)) > 0)
    elif isinstance(phase1_payload.get("summaries"), list):
        total_events = 0
        for row in phase1_payload.get("summaries", []):
            if not isinstance(row, dict):
                continue
            total_events += int(row.get("event_count", 0) or 0)
        phase1_structure_pass = bool(total_events > 0)
    else:
        phase1_structure_pass = False
    phase1_pass = phase1_structure_pass
    return phase1_pass, phase1_structure_pass, phase1_decision


def _table_text(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False)
    except ImportError:
        return df.to_string(index=False)


def _read_csv_allow_empty(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


@dataclass(frozen=True)
class ConditionSpec:
    name: str
    description: str
    mask_fn: Callable[[pd.DataFrame], pd.Series]


@dataclass(frozen=True)
class ActionSpec:
    name: str
    family: str
    params: Dict[str, object]


def _bootstrap_ci(values: np.ndarray, n_boot: int, seed: int, alpha: float = 0.05) -> Tuple[float, float, float]:
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    if len(arr) == 0:
        return np.nan, np.nan, np.nan
    rng = np.random.default_rng(seed)
    boots = []
    for _ in range(n_boot):
        sample = rng.choice(arr, size=len(arr), replace=True)
        boots.append(float(np.mean(sample)))
    return float(np.mean(arr)), float(np.quantile(boots, alpha / 2.0)), float(np.quantile(boots, 1.0 - alpha / 2.0))


def _build_conditions(events: pd.DataFrame) -> List[ConditionSpec]:
    conds: List[ConditionSpec] = [ConditionSpec("all", "all events", lambda d: pd.Series(True, index=d.index))]

    if "symbol" in events.columns:
        for symbol in sorted(events["symbol"].dropna().astype(str).unique().tolist()):
            conds.append(
                ConditionSpec(
                    name=f"symbol_{symbol}",
                    description=f"symbol == {symbol}",
                    mask_fn=lambda d, sym=symbol: d["symbol"].astype(str) == sym,
                )
            )

    if "bull_bear" in events.columns:
        for bb in sorted(events["bull_bear"].dropna().astype(str).unique().tolist()):
            conds.append(
                ConditionSpec(
                    name=f"bull_bear_{bb}",
                    description=f"bull_bear == {bb}",
                    mask_fn=lambda d, v=bb: d["bull_bear"].astype(str) == v,
                )
            )

    if "vol_regime" in events.columns:
        for vr in sorted(events["vol_regime"].dropna().astype(str).unique().tolist()):
            conds.append(
                ConditionSpec(
                    name=f"vol_regime_{vr}",
                    description=f"vol_regime == {vr}",
                    mask_fn=lambda d, v=vr: d["vol_regime"].astype(str) == v,
                )
            )

    if "tod_bucket" in events.columns:
        conds.extend(
            [
                ConditionSpec("session_asia", "enter hour in [0,7]", lambda d: d["tod_bucket"].between(0, 7, inclusive="both")),
                ConditionSpec("session_eu", "enter hour in [8,15]", lambda d: d["tod_bucket"].between(8, 15, inclusive="both")),
                ConditionSpec("session_us", "enter hour in [16,23]", lambda d: d["tod_bucket"].between(16, 23, inclusive="both")),
            ]
        )
    elif "anchor_hour" in events.columns:
        conds.extend(
            [
                ConditionSpec("session_asia", "enter hour in [0,7]", lambda d: d["anchor_hour"].between(0, 7, inclusive="both")),
                ConditionSpec("session_eu", "enter hour in [8,15]", lambda d: d["anchor_hour"].between(8, 15, inclusive="both")),
                ConditionSpec("session_us", "enter hour in [16,23]", lambda d: d["anchor_hour"].between(16, 23, inclusive="both")),
            ]
        )

    if "t_rv_peak" in events.columns:
        conds.extend(
            [
                ConditionSpec("age_bucket_0_8", "t_rv_peak in [0,8]", lambda d: d["t_rv_peak"].fillna(10**9).between(0, 8, inclusive="both")),
                ConditionSpec("age_bucket_9_30", "t_rv_peak in [9,30]", lambda d: d["t_rv_peak"].fillna(10**9).between(9, 30, inclusive="both")),
                ConditionSpec("age_bucket_31_96", "t_rv_peak in [31,96]", lambda d: d["t_rv_peak"].fillna(10**9).between(31, 96, inclusive="both")),
            ]
        )
    if "rv_decay_half_life" in events.columns:
        conds.append(
            ConditionSpec("near_half_life", "rv_decay_half_life <= 30", lambda d: d["rv_decay_half_life"].fillna(10**9) <= 30)
        )
    if {"t_rv_peak", "duration_bars"}.issubset(events.columns):
        conds.extend(
            [
                ConditionSpec(
                    "fractional_age_0_33",
                    "t_rv_peak / duration_bars <= 0.33",
                    lambda d: (d["t_rv_peak"].fillna(10**9) / d["duration_bars"].replace(0, np.nan)).fillna(10**9) <= 0.33,
                ),
                ConditionSpec(
                    "fractional_age_34_66",
                    "t_rv_peak / duration_bars in (0.33, 0.66]",
                    lambda d: (
                        (d["t_rv_peak"].fillna(10**9) / d["duration_bars"].replace(0, np.nan)).fillna(10**9) > 0.33
                    )
                    & ((d["t_rv_peak"].fillna(10**9) / d["duration_bars"].replace(0, np.nan)).fillna(10**9) <= 0.66),
                ),
                ConditionSpec(
                    "fractional_age_67_100",
                    "t_rv_peak / duration_bars > 0.66",
                    lambda d: (d["t_rv_peak"].fillna(10**9) / d["duration_bars"].replace(0, np.nan)).fillna(10**9) > 0.66,
                ),
            ]
        )

    # de-dup by name preserving order
    seen = set()
    out = []
    for c in conds:
        if c.name in seen:
            continue
        seen.add(c.name)
        out.append(c)
    return out


def _build_actions() -> List[ActionSpec]:
    return [
        ActionSpec("no_action", "baseline", {}),
        ActionSpec("entry_gate_skip", "entry_gating", {"k": 0.0}),
        ActionSpec("risk_throttle_0.5", "risk_throttle", {"k": 0.5}),
        ActionSpec("risk_throttle_0", "risk_throttle", {"k": 0.0}),
        ActionSpec("delay_0", "timing", {"delay_bars": 0}),
        ActionSpec("delay_8", "timing", {"delay_bars": 8}),
        ActionSpec("delay_30", "timing", {"delay_bars": 30}),
        ActionSpec("reenable_at_half_life", "timing", {"landmark": "rv_decay_half_life"}),
    ]


def _attach_forward_opportunity(
    events: pd.DataFrame,
    controls: pd.DataFrame,
    run_id: str,
    symbols: List[str],
    timeframe: str,
    horizon_bars: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if events.empty:
        return events, controls

    out_events = events.copy()
    out_controls = controls.copy()
    if "enter_ts" not in out_events.columns:
        for col in ["anchor_ts", "timestamp"]:
            if col in out_events.columns:
                out_events["enter_ts"] = out_events[col]
                break
    if "enter_idx" not in out_events.columns and "start_idx" in out_events.columns:
        out_events["enter_idx"] = pd.to_numeric(out_events["start_idx"], errors="coerce")
    out_events["enter_ts"] = pd.to_datetime(out_events.get("enter_ts"), utc=True, errors="coerce")

    rows = []
    for symbol in symbols:
        bars_candidates = [
            run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", "perp", symbol, f"bars_{timeframe}"),
            DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / f"bars_{timeframe}",
        ]
        bars_dir = choose_partition_dir(bars_candidates)
        bars = read_parquet(list_parquet_files(bars_dir)) if bars_dir else pd.DataFrame()
        if bars.empty:
            continue
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True, errors="coerce")
        bars = bars.sort_values("timestamp").reset_index(drop=True)
        close = bars["close"].astype(float)
        fwd_abs_return = (close.shift(-horizon_bars) / close - 1.0).abs()
        rows.append(
            pd.DataFrame(
                {
                    "symbol": symbol,
                    "bar_idx": np.arange(len(bars), dtype=int),
                    "timestamp": bars["timestamp"],
                    "forward_abs_return_h": fwd_abs_return,
                }
            )
        )

    if not rows:
        out_events["forward_abs_return_h"] = np.nan
        out_events["forward_abs_return_h_ctrl"] = np.nan
        out_events["opportunity_value_excess"] = np.nan
        return out_events, out_controls

    fwd = pd.concat(rows, ignore_index=True)
    fwd_ts = fwd.rename(columns={"timestamp": "enter_ts"})

    out_events = out_events.merge(
        fwd_ts[["symbol", "enter_ts", "forward_abs_return_h"]],
        on=["symbol", "enter_ts"],
        how="left",
    )
    if "enter_idx" in out_events.columns:
        out_events = out_events.merge(
            fwd[["symbol", "bar_idx", "forward_abs_return_h"]].rename(columns={"bar_idx": "enter_idx", "forward_abs_return_h": "forward_abs_return_h_idx"}),
            on=["symbol", "enter_idx"],
            how="left",
        )
        out_events["forward_abs_return_h"] = out_events["forward_abs_return_h"].where(
            out_events["forward_abs_return_h"].notna(),
            out_events["forward_abs_return_h_idx"],
        )
        out_events = out_events.drop(columns=["forward_abs_return_h_idx"])

    if not out_controls.empty and "event_id" in out_controls.columns and "control_idx" in out_controls.columns:
        event_to_symbol = out_events[["event_id", "symbol"]].drop_duplicates()
        if "symbol" not in out_controls.columns:
            out_controls = out_controls.merge(event_to_symbol, on="event_id", how="left")
        else:
            out_controls = out_controls.merge(
                event_to_symbol.rename(columns={"symbol": "event_symbol"}),
                on="event_id",
                how="left",
            )
            out_controls["symbol"] = out_controls["symbol"].where(
                out_controls["symbol"].notna(),
                out_controls["event_symbol"],
            )
            out_controls = out_controls.drop(columns=["event_symbol"])

        if "symbol" in out_controls.columns:
            out_controls = out_controls.merge(
                fwd[["symbol", "bar_idx", "forward_abs_return_h"]].rename(
                    columns={"bar_idx": "control_idx", "forward_abs_return_h": "forward_abs_return_h_ctrl_row"}
                ),
                on=["symbol", "control_idx"],
                how="left",
            )
            ctrl_mean = out_controls.groupby("event_id", as_index=False)["forward_abs_return_h_ctrl_row"].mean()
            out_events = out_events.merge(
                ctrl_mean.rename(columns={"forward_abs_return_h_ctrl_row": "forward_abs_return_h_ctrl"}),
                on="event_id",
                how="left",
            )
        else:
            out_events["forward_abs_return_h_ctrl"] = np.nan
    else:
        out_events["forward_abs_return_h_ctrl"] = np.nan

    out_events["opportunity_value_excess"] = out_events["forward_abs_return_h"] - out_events["forward_abs_return_h_ctrl"]
    out_events["opportunity_value_excess"] = out_events["opportunity_value_excess"].where(
        out_events["opportunity_value_excess"].notna(),
        out_events["forward_abs_return_h"],
    )
    return out_events, out_controls


def _prepare_baseline(events: pd.DataFrame, controls: pd.DataFrame) -> pd.DataFrame:
    out = events.copy()
    out["baseline_mode"] = "event_proxy_only"

    adverse_binary_col = _first_existing(out, ["secondary_shock_within_h", "secondary_shock_within", "tail_move_within"])
    adverse_mag_col = _first_existing(out, ["range_pct_96", "range_expansion"])
    opportunity_col = _first_existing(out, ["relaxed_within_96", "forward_abs_return_h"])

    adverse_binary = _numeric_non_negative(out, adverse_binary_col, n=len(out))
    adverse_mag = _numeric_non_negative(out, adverse_mag_col, n=len(out))
    if adverse_binary_col and adverse_mag_col:
        out["adverse_proxy"] = 0.5 * adverse_binary + 0.5 * adverse_mag
    elif adverse_binary_col:
        out["adverse_proxy"] = adverse_binary
    elif adverse_mag_col:
        out["adverse_proxy"] = adverse_mag
    else:
        out["adverse_proxy"] = 0.0

    if opportunity_col:
        out["opportunity_proxy"] = _numeric_non_negative(out, opportunity_col, n=len(out))
    elif adverse_binary_col:
        out["opportunity_proxy"] = (1.0 - adverse_binary).clip(lower=0.0)
    else:
        out["opportunity_proxy"] = 0.0

    time_to_adverse_col = _first_existing(out, ["time_to_secondary_shock", "time_to_tail_move"])
    timing_landmark_col = _first_existing(out, ["rv_decay_half_life", "parent_time_to_relax", "time_to_relax"])
    out["time_to_adverse"] = _numeric_any(out, time_to_adverse_col, n=len(out), default=np.nan)
    out["timing_landmark"] = _numeric_any(out, timing_landmark_col, n=len(out), default=np.nan)

    if controls.empty or "event_id" not in controls.columns or "event_id" not in out.columns:
        out["adverse_proxy_ctrl"] = np.nan
        out["opportunity_proxy_ctrl"] = np.nan
        out["adverse_proxy_excess"] = out["adverse_proxy"]
        out["opportunity_proxy_excess"] = out["opportunity_proxy"]
        return out

    numeric_ctrl = controls.groupby("event_id", as_index=False).mean(numeric_only=True)
    ctrl_cols = [
        c
        for c in [
            "secondary_shock_within_h",
            "secondary_shock_within",
            "tail_move_within",
            "range_pct_96",
            "range_expansion",
            "relaxed_within_96",
            "forward_abs_return_h",
            "time_to_secondary_shock",
            "time_to_tail_move",
            "rv_decay_half_life",
            "parent_time_to_relax",
            "time_to_relax",
        ]
        if c in numeric_ctrl.columns
    ]
    if not ctrl_cols:
        out["adverse_proxy_ctrl"] = np.nan
        out["opportunity_proxy_ctrl"] = np.nan
        out["adverse_proxy_excess"] = out["adverse_proxy"]
        out["opportunity_proxy_excess"] = out["opportunity_proxy"]
        return out

    rename_map = {c: f"{c}_ctrl" for c in ctrl_cols}
    merged = out.merge(numeric_ctrl[["event_id"] + ctrl_cols].rename(columns=rename_map), on="event_id", how="left")

    adverse_binary_ctrl_col = _first_existing(merged, ["secondary_shock_within_h_ctrl", "secondary_shock_within_ctrl", "tail_move_within_ctrl"])
    adverse_mag_ctrl_col = _first_existing(merged, ["range_pct_96_ctrl", "range_expansion_ctrl"])
    opportunity_ctrl_col = _first_existing(merged, ["relaxed_within_96_ctrl", "forward_abs_return_h_ctrl"])

    adverse_binary_ctrl = _numeric_non_negative(merged, adverse_binary_ctrl_col, n=len(merged))
    adverse_mag_ctrl = _numeric_non_negative(merged, adverse_mag_ctrl_col, n=len(merged))
    if adverse_binary_ctrl_col and adverse_mag_ctrl_col:
        merged["adverse_proxy_ctrl"] = 0.5 * adverse_binary_ctrl + 0.5 * adverse_mag_ctrl
    elif adverse_binary_ctrl_col:
        merged["adverse_proxy_ctrl"] = adverse_binary_ctrl
    elif adverse_mag_ctrl_col:
        merged["adverse_proxy_ctrl"] = adverse_mag_ctrl
    else:
        merged["adverse_proxy_ctrl"] = np.nan

    if opportunity_ctrl_col:
        merged["opportunity_proxy_ctrl"] = _numeric_non_negative(merged, opportunity_ctrl_col, n=len(merged))
    else:
        merged["opportunity_proxy_ctrl"] = np.nan

    if "time_to_adverse" not in merged.columns:
        merged["time_to_adverse"] = _numeric_any(merged, _first_existing(merged, ["time_to_secondary_shock", "time_to_tail_move"]), n=len(merged), default=np.nan)
    if "timing_landmark" not in merged.columns:
        merged["timing_landmark"] = _numeric_any(merged, _first_existing(merged, ["rv_decay_half_life", "parent_time_to_relax", "time_to_relax"]), n=len(merged), default=np.nan)

    merged["adverse_proxy_excess"] = (merged["adverse_proxy"] - merged["adverse_proxy_ctrl"]).clip(lower=0.0)
    merged["opportunity_proxy_excess"] = (merged["opportunity_proxy"] - merged["opportunity_proxy_ctrl"]).clip(lower=0.0)
    merged["baseline_mode"] = "matched_controls_excess"
    return merged


def _apply_action_proxy(sub: pd.DataFrame, action: ActionSpec) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    # Returns per-event contributions for: adverse_delta, opportunity_delta, exposure_delta
    # Negative adverse_delta is improvement (risk reduction).
    if "adverse_proxy_excess" in sub.columns:
        adverse = sub["adverse_proxy_excess"].fillna(0).astype(float).to_numpy()
    else:
        sec = sub["secondary_shock_within_h"].fillna(0).astype(float).to_numpy()
        rng = sub["range_pct_96"].fillna(0).astype(float).to_numpy()
        adverse = 0.5 * sec + 0.5 * np.clip(rng, 0.0, None)

    if "opportunity_value_excess" in sub.columns:
        opp_value = sub["opportunity_value_excess"].fillna(0).astype(float).to_numpy()
    elif "opportunity_proxy_excess" in sub.columns:
        opp_value = sub["opportunity_proxy_excess"].fillna(0).astype(float).to_numpy()
    else:
        opp_value = sub["relaxed_within_96"].fillna(0).astype(float).to_numpy()

    if action.name in {"no_action", "delay_0"}:
        return np.zeros(len(sub), dtype=float), np.zeros(len(sub), dtype=float), np.zeros(len(sub), dtype=float)

    if action.family in {"entry_gating", "risk_throttle"}:
        k = float(action.params.get("k", 1.0))
        exposure_delta = np.full(len(sub), -(1.0 - k), dtype=float)
        adverse_delta = -(1.0 - k) * adverse
        opportunity_delta = exposure_delta * opp_value
        return adverse_delta, opportunity_delta, exposure_delta

    if action.name.startswith("delay_"):
        d = int(action.params.get("delay_bars", 0))
        t_adverse = _numeric_any(
            sub,
            _first_existing(sub, ["time_to_adverse", "time_to_secondary_shock", "time_to_tail_move"]),
            n=len(sub),
            default=10**9,
        ).fillna(10**9).to_numpy()
        adverse_delta = -(t_adverse <= d).astype(float) * adverse
        exposure_delta = -np.full(len(sub), min(1.0, d / 96.0), dtype=float)
        opportunity_delta = exposure_delta * opp_value
        return adverse_delta, opportunity_delta, exposure_delta

    if action.name == "reenable_at_half_life":
        t_landmark = _numeric_any(
            sub,
            _first_existing(sub, ["timing_landmark", "rv_decay_half_life", "parent_time_to_relax", "time_to_relax"]),
            n=len(sub),
            default=10**9,
        ).fillna(10**9).to_numpy()
        t_adverse = _numeric_any(
            sub,
            _first_existing(sub, ["time_to_adverse", "time_to_secondary_shock", "time_to_tail_move"]),
            n=len(sub),
            default=10**9,
        ).fillna(10**9).to_numpy()
        adverse_delta = -(t_adverse <= t_landmark).astype(float) * adverse
        exposure_delta = -np.clip(t_landmark / 96.0, 0.0, 1.0)
        opportunity_delta = exposure_delta * opp_value
        return adverse_delta, opportunity_delta, exposure_delta

    raise ValueError(f"Unsupported action: {action.name}")


def _gate_year_stability(sub: pd.DataFrame, effect_col: str, min_ratio: float = 0.8) -> Tuple[bool, str]:
    if "year" not in sub.columns or sub.empty:
        return False, "insufficient_years"
    signs = []
    for _, g in sub.groupby("year", sort=True):
        x = g[effect_col].mean()
        signs.append(1 if x > 0 else -1 if x < 0 else 0)
    non_zero = [s for s in signs if s != 0]
    if not non_zero:
        return False, "all_zero"
    improvement_ratio = non_zero.count(-1) / len(non_zero)
    catastrophic_reversal = (non_zero.count(1) > 0) and (improvement_ratio < min_ratio)
    return bool(improvement_ratio >= min_ratio and not catastrophic_reversal), ",".join(str(s) for s in signs)


def _gate_regime_stability(
    sub: pd.DataFrame,
    effect_col: str,
    condition_name: str,
    min_stable_splits: int = 2,
) -> Tuple[bool, int, int]:
    # If condition itself is on split variable, skip that split.
    checks: List[pd.Series] = []
    if not condition_name.startswith("symbol_") and "symbol" in sub.columns:
        checks.append(sub.groupby("symbol")[effect_col].mean())
    if not condition_name.startswith("vol_regime_") and "vol_regime" in sub.columns:
        checks.append(sub.groupby("vol_regime")[effect_col].mean())
    if not condition_name.startswith("bull_bear_") and "bull_bear" in sub.columns:
        checks.append(sub.groupby("bull_bear")[effect_col].mean())

    # Majority-style regime gate by default (2/3 when all splits exist).
    stable_splits = 0
    for s in checks:
        nz = [v for v in s.tolist() if abs(v) > 1e-12]
        if not nz:
            continue
        # Improvement should stay non-positive (adverse reduction) across splits.
        if not any(v > 0 for v in nz):
            stable_splits += 1

    if not checks:
        return False, 0, 0

    required_splits = min(max(1, int(min_stable_splits)), len(checks))
    return stable_splits >= required_splits, stable_splits, required_splits


def _split_count(sub: pd.DataFrame, label: str) -> int:
    if "split_label" not in sub.columns:
        return 0
    return int((sub["split_label"] == label).sum())


def _split_mean(sub: pd.DataFrame, split_label: str, col: str) -> float:
    if "split_label" not in sub.columns or col not in sub.columns:
        return np.nan
    frame = sub[sub["split_label"] == split_label]
    if frame.empty:
        return np.nan
    values = pd.to_numeric(frame[col], errors="coerce").dropna()
    return float(values.mean()) if not values.empty else np.nan


def _split_t_stat_and_p_value(sub: pd.DataFrame, split_label: str, col: str) -> Tuple[float, float]:
    if "split_label" not in sub.columns or col not in sub.columns:
        return np.nan, np.nan
    frame = sub[sub["split_label"] == split_label]
    values = pd.to_numeric(frame[col], errors="coerce").dropna().to_numpy(dtype=float)
    if len(values) < 2:
        return np.nan, np.nan
    std = float(np.std(values, ddof=1))
    if std <= 0.0:
        return np.nan, np.nan
    t_stat = float(np.mean(values) / (std / np.sqrt(len(values))))
    return t_stat, float(_two_sided_p_from_t(t_stat))


def _capacity_proxy_from_frame(sub: pd.DataFrame) -> float:
    for col in ["quote_volume", "volume", "notional", "turnover", "liquidation_notional"]:
        if col not in sub.columns:
            continue
        vals = pd.to_numeric(sub[col], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        vals = vals[vals > 0]
        if vals.empty:
            continue
        return float(np.log1p(float(vals.median())))
    return np.nan


def _evaluate_candidate(
    sub: pd.DataFrame,
    condition: ConditionSpec,
    action: ActionSpec,
    bootstrap_iters: int,
    seed: int,
    cost_floor: float,
    tail_material_threshold: float,
    opportunity_tight_eps: float,
    opportunity_near_zero_eps: float,
    net_benefit_floor: float,
    simplicity_gate: bool,
    min_regime_stable_splits: int = 2,
    total_event_count: int = 0,
) -> Dict[str, object]:
    adverse_delta_vec, opp_delta_vec, exposure_delta_vec = _apply_action_proxy(sub, action)

    mean_adv, ci_low_adv, ci_high_adv = _bootstrap_ci(adverse_delta_vec, bootstrap_iters, seed + 1)
    mean_opp, ci_low_opp, ci_high_opp = _bootstrap_ci(opp_delta_vec, bootstrap_iters, seed + 7)
    mean_exp, _, _ = _bootstrap_ci(exposure_delta_vec, bootstrap_iters, seed + 13)

    # primary improvement means adverse delta is negative
    gate_a = bool(np.isfinite(ci_high_adv) and ci_high_adv < 0)

    tmp = sub.copy()
    tmp["adverse_effect"] = adverse_delta_vec
    gate_b, year_signs = _gate_year_stability(tmp, "adverse_effect")
    gate_c, gate_c_stable_splits, gate_c_required_splits = _gate_regime_stability(
        tmp,
        "adverse_effect",
        condition_name=condition.name,
        min_stable_splits=min_regime_stable_splits,
    )

    risk_reduction = float(-mean_adv) if np.isfinite(mean_adv) else np.nan
    material_tail = bool(np.isfinite(risk_reduction) and risk_reduction >= tail_material_threshold)
    gate_d = bool(np.isfinite(risk_reduction) and risk_reduction >= cost_floor) or material_tail

    opportunity_forward = (
        float(np.nanmean(sub["forward_abs_return_h"].to_numpy(dtype=float)))
        if "forward_abs_return_h" in sub.columns and len(sub) > 0
        else np.nan
    )
    opportunity_tail = (
        float(np.nanmean(sub["opportunity_value_excess"].to_numpy(dtype=float)))
        if "opportunity_value_excess" in sub.columns and len(sub) > 0
        else np.nan
    )
    opportunity_composite = float(np.nanmean([opportunity_forward, opportunity_tail]))
    if not np.isfinite(opportunity_composite):
        opportunity_composite = 0.0

    opportunity_cost = float(max(0.0, -mean_opp)) if np.isfinite(mean_opp) else np.nan
    net_benefit = float(risk_reduction - opportunity_cost) if np.isfinite(risk_reduction) and np.isfinite(opportunity_cost) else np.nan
    expectancy_per_trade = float(net_benefit) if np.isfinite(net_benefit) else 0.0

    opportunity_ci_tight_overlap = bool(
        np.isfinite(ci_low_opp)
        and np.isfinite(ci_high_opp)
        and (ci_low_opp <= 0.0 <= ci_high_opp)
        and (max(abs(ci_low_opp), abs(ci_high_opp)) <= opportunity_tight_eps)
    )
    opportunity_near_zero = bool(
        (np.isfinite(opportunity_cost) and opportunity_cost <= opportunity_near_zero_eps)
        or opportunity_ci_tight_overlap
    )
    full_block_exposure = bool(np.isfinite(mean_exp) and mean_exp <= -0.9)
    gate_f = not (full_block_exposure and not opportunity_near_zero)
    gate_g = bool(np.isfinite(net_benefit) and net_benefit >= net_benefit_floor)
    gate_e = bool(simplicity_gate)

    gate_values = [gate_a, gate_b, gate_c, gate_d, gate_f, gate_g]
    robustness_score = float(np.mean([1.0 if g else 0.0 for g in gate_values]))

    train_samples = _split_count(sub, "train")

    validation_samples = _split_count(sub, "validation")
    test_samples = _split_count(sub, "test")
    validation_delta_adverse_mean = _split_mean(tmp, "validation", "adverse_effect")
    test_delta_adverse_mean = _split_mean(tmp, "test", "adverse_effect")
    test_t_stat, test_p_value = _split_t_stat_and_p_value(tmp, "test", "adverse_effect")
    gate_oos = bool(
        validation_samples > 0
        and test_samples > 0
        and np.isfinite(validation_delta_adverse_mean)
        and np.isfinite(test_delta_adverse_mean)
        and validation_delta_adverse_mean < 0.0
        and test_delta_adverse_mean < 0.0
    )
    if gate_oos:
        robustness_score = min(1.0, robustness_score + 0.1)

    sample_size = int(len(sub))
    event_frequency = float(sample_size / total_event_count) if total_event_count > 0 else 0.0
    capacity_proxy = _capacity_proxy_from_frame(sub)
    profit_density_score = float(expectancy_per_trade * robustness_score * event_frequency)

    fail_reasons = []
    if not gate_a:
        fail_reasons.append("gate_a_ci")
    if not gate_b:
        fail_reasons.append("gate_b_time")
    if not gate_c:
        fail_reasons.append("gate_c_regime")
    if not gate_d:
        fail_reasons.append("gate_d_friction")
    if not gate_f:
        fail_reasons.append("gate_f_exposure_guard")
    if not gate_g:
        fail_reasons.append("gate_g_net_benefit")
    if not gate_e:
        fail_reasons.append("gate_e_simplicity")
    if not gate_oos:
        fail_reasons.append("gate_oos_validation_test")

    return {
        "candidate_id": f"{condition.name}__{action.name}",
        "condition": condition.name,
        "condition_desc": condition.description,
        "action": action.name,
        "action_family": action.family,
        "sample_size": int(len(sub)),
        "train_samples": train_samples,
        "validation_samples": validation_samples,
        "test_samples": test_samples,
        "baseline_mode": str(sub["baseline_mode"].iloc[0]) if "baseline_mode" in sub.columns and not sub.empty else "event_proxy_only",
        "delta_adverse_mean": mean_adv,
        "delta_adverse_ci_low": ci_low_adv,
        "delta_adverse_ci_high": ci_high_adv,
        "delta_opportunity_mean": mean_opp,
        "delta_opportunity_ci_low": ci_low_opp,
        "delta_opportunity_ci_high": ci_high_opp,
        "delta_exposure_mean": mean_exp,
        "gate_a_ci_separated": gate_a,
        "gate_b_time_stable": gate_b,
        "gate_b_year_signs": year_signs,
        "gate_c_regime_stable": gate_c,
        "gate_c_stable_splits": gate_c_stable_splits,
        "gate_c_required_splits": gate_c_required_splits,
        "gate_d_friction_floor": gate_d,
        "opportunity_forward_mean": opportunity_forward,
        "opportunity_tail_mean": opportunity_tail,
        "opportunity_composite_mean": opportunity_composite,
        "opportunity_cost_mean": opportunity_cost,
        "net_benefit_mean": net_benefit,
        "expectancy_per_trade": expectancy_per_trade,
        "robustness_score": robustness_score,
        "event_frequency": event_frequency,
        "capacity_proxy": capacity_proxy,
        "profit_density_score": profit_density_score,
        "gate_f_exposure_guard": gate_f,
        "gate_g_net_benefit": gate_g,
        "gate_e_simplicity": gate_e,
        "validation_delta_adverse_mean": validation_delta_adverse_mean,
        "test_delta_adverse_mean": test_delta_adverse_mean,
        "test_t_stat": test_t_stat,
        "test_p_value": test_p_value,
        "test_p_value_adj_bh": np.nan,
        "gate_oos_validation_test": gate_oos,
        "gate_multiplicity": False,
        "gate_pass": bool(gate_a and gate_b and gate_c and gate_d and gate_f and gate_g and gate_e and gate_oos),
        "gate_all": False,
        "fail_reasons": ",".join(fail_reasons) if fail_reasons else "",
    }


def _compute_drawdown_profile(pnl: np.ndarray) -> float:
    arr = np.asarray(pnl, dtype=float)
    arr = arr[np.isfinite(arr)]
    if len(arr) == 0:
        return 0.0
    cum = np.cumsum(arr)
    running_peak = np.maximum.accumulate(cum)
    drawdown = cum - running_peak
    return float(np.min(drawdown)) if len(drawdown) else 0.0


def _build_symbol_evaluation_table(
    events: pd.DataFrame,
    candidates: pd.DataFrame,
    conditions: List[ConditionSpec],
    actions: List[ActionSpec],
    event_type: str,
    min_ev: float,
    min_stability_score: float,
    min_sample_size: int,
) -> pd.DataFrame:
    if events.empty or candidates.empty or "symbol" not in events.columns:
        return pd.DataFrame(columns=SYMBOL_OUTPUT_COLUMNS)

    condition_map = {c.name: c for c in conditions}
    action_map = {a.name: a for a in actions}
    rows: List[Dict[str, object]] = []

    for _, cand in candidates.iterrows():
        condition_name = str(cand.get("condition", ""))
        action_name = str(cand.get("action", ""))
        cond = condition_map.get(condition_name)
        action = action_map.get(action_name)
        if cond is None or action is None:
            continue

        sub = events[cond.mask_fn(events)].copy()
        if sub.empty or "symbol" not in sub.columns:
            continue

        adverse_delta_vec, opp_delta_vec, _ = _apply_action_proxy(sub, action)
        sub["net_benefit_effect"] = (-adverse_delta_vec) - np.maximum(0.0, -opp_delta_vec)

        for symbol, sym_frame in sub.groupby("symbol", sort=True):
            sym_values = pd.to_numeric(sym_frame["net_benefit_effect"], errors="coerce").dropna().to_numpy(dtype=float)
            if len(sym_values) == 0:
                continue
            ev = float(np.mean(sym_values))
            variance = float(np.var(sym_values, ddof=1)) if len(sym_values) > 1 else 0.0
            std = float(np.sqrt(max(variance, 0.0)))
            sharpe_like = float(ev / std) if std > 1e-12 else 0.0

            splits = []
            if "split_label" in sym_frame.columns:
                for _, g in sym_frame.groupby("split_label", sort=True):
                    vals = pd.to_numeric(g["net_benefit_effect"], errors="coerce").dropna().to_numpy(dtype=float)
                    if len(vals):
                        splits.append(float(np.mean(vals)))
            if not splits:
                splits = [ev]

            overall_sign = 1 if ev > 0 else -1 if ev < 0 else 0
            non_zero_split_signs = [1 if x > 0 else -1 if x < 0 else 0 for x in splits if abs(x) > 1e-12]
            window_consistency = (
                float(sum(1 for s in non_zero_split_signs if s == overall_sign) / len(non_zero_split_signs))
                if non_zero_split_signs
                else 0.0
            )
            sign_persistence = (
                float(sum(1 for s in non_zero_split_signs if s == non_zero_split_signs[0]) / len(non_zero_split_signs))
                if non_zero_split_signs
                else 0.0
            )
            drawdown_profile = _compute_drawdown_profile(sym_values)
            drawdown_penalty = float(max(0.0, min(1.0, abs(drawdown_profile))))
            stability_score = float(max(0.0, min(1.0, (0.45 * window_consistency) + (0.45 * sign_persistence) - (0.1 * drawdown_penalty))))
            capacity_proxy = _capacity_proxy_from_frame(sym_frame)
            if not np.isfinite(capacity_proxy):
                capacity_proxy = 0.0

            rejection_reasons: List[str] = []
            if ev <= min_ev:
                rejection_reasons.append(REJECTION_NEGATIVE_EV)
            if stability_score < min_stability_score:
                rejection_reasons.append(REJECTION_UNSTABLE)
            if len(sym_values) < min_sample_size:
                rejection_reasons.append(REJECTION_LOW_SAMPLE)

            deployable = len(rejection_reasons) == 0
            rows.append(
                {
                    "candidate_id": str(cand.get("candidate_id", f"{condition_name}__{action_name}")),
                    "event_type": event_type,
                    "condition": condition_name,
                    "action": action_name,
                    "symbol": str(symbol),
                    "sample_size": int(len(sym_values)),
                    "ev": ev,
                    "variance": variance,
                    "sharpe_like": sharpe_like,
                    "stability_score": stability_score,
                    "window_consistency": window_consistency,
                    "sign_persistence": sign_persistence,
                    "drawdown_profile": drawdown_profile,
                    "capacity_proxy": capacity_proxy,
                    "rejection_reason_codes": ",".join(rejection_reasons),
                    "promotion_status": "promoted" if deployable else "rejected",
                    "deployable": deployable,
                }
            )

    if not rows:
        return pd.DataFrame(columns=SYMBOL_OUTPUT_COLUMNS)
    out = pd.DataFrame(rows)
    return out[SYMBOL_OUTPUT_COLUMNS]


def _deployment_mode_from_symbol_dispersion(symbol_eval: pd.DataFrame) -> Dict[str, object]:
    deployable = symbol_eval[symbol_eval.get("deployable", False).astype(bool)].copy() if not symbol_eval.empty else pd.DataFrame()
    if deployable.empty:
        return {
            "deployment_mode": DEPLOYMENT_MODE_DIVERSIFY,
            "rationale": "No deployable symbols passed promotion gates; defaulting to diversify mode until promotion resumes.",
            "dominance_detected": False,
            "top_symbols_considered": [],
            "dispersion_metrics": {
                "score_gap": 0.0,
                "relative_gap": 0.0,
                "confidence_overlap": True,
                "stability_adjusted_spread": 0.0,
            },
            "policy_thresholds": {
                "relative_gap_min": 0.25,
                "stability_adjusted_spread_min": 1.0,
                "confidence_overlap_required_for_diversify": True,
            },
        }

    ranked = deployable.copy()
    ranked["ev"] = pd.to_numeric(ranked.get("ev"), errors="coerce").fillna(0.0)
    ranked["stability_score"] = pd.to_numeric(ranked.get("stability_score"), errors="coerce").fillna(0.0)
    ranked["variance"] = pd.to_numeric(ranked.get("variance"), errors="coerce").fillna(0.0).clip(lower=0.0)
    ranked["sample_size"] = pd.to_numeric(ranked.get("sample_size"), errors="coerce").fillna(0).astype(int)
    ranked["stability_adjusted_score"] = ranked["ev"] * ranked["stability_score"]
    ranked["score_std_err"] = np.sqrt(ranked["variance"] / ranked["sample_size"].clip(lower=1))
    ranked["score_ci_low"] = ranked["stability_adjusted_score"] - (1.96 * ranked["score_std_err"])
    ranked["score_ci_high"] = ranked["stability_adjusted_score"] + (1.96 * ranked["score_std_err"])

    ranked = ranked.sort_values(["stability_adjusted_score", "ev"], ascending=[False, False]).head(2)
    top_rows = ranked.to_dict(orient="records")
    if len(ranked) == 1:
        top_symbol = str(ranked.iloc[0].get("symbol", "unknown"))
        return {
            "deployment_mode": DEPLOYMENT_MODE_CONCENTRATE,
            "rationale": f"Only one deployable symbol ({top_symbol}) is available, so concentration is required.",
            "dominance_detected": True,
            "top_symbols_considered": top_rows,
            "dispersion_metrics": {
                "score_gap": float(ranked.iloc[0].get("stability_adjusted_score", 0.0)),
                "relative_gap": 1.0,
                "confidence_overlap": False,
                "stability_adjusted_spread": float("inf"),
            },
            "policy_thresholds": {
                "relative_gap_min": 0.25,
                "stability_adjusted_spread_min": 1.0,
                "confidence_overlap_required_for_diversify": True,
            },
        }

    top = ranked.iloc[0]
    second = ranked.iloc[1]
    top_score = float(top.get("stability_adjusted_score", 0.0))
    second_score = float(second.get("stability_adjusted_score", 0.0))
    score_gap = float(top_score - second_score)
    relative_gap = float(score_gap / max(abs(top_score), 1e-9))
    top_low = float(top.get("score_ci_low", np.nan))
    top_high = float(top.get("score_ci_high", np.nan))
    second_low = float(second.get("score_ci_low", np.nan))
    second_high = float(second.get("score_ci_high", np.nan))
    confidence_overlap = bool(max(top_low, second_low) <= min(top_high, second_high))
    pooled_std = float(np.sqrt(float(top.get("score_std_err", 0.0)) ** 2 + float(second.get("score_std_err", 0.0)) ** 2))
    stability_adjusted_spread = float(score_gap / pooled_std) if pooled_std > 1e-9 else float("inf")

    relative_gap_min = 0.25
    spread_min = 1.0
    dominance_detected = bool(
        (relative_gap >= relative_gap_min)
        and (stability_adjusted_spread >= spread_min)
        and (not confidence_overlap)
    )
    mode = DEPLOYMENT_MODE_CONCENTRATE if dominance_detected else DEPLOYMENT_MODE_DIVERSIFY
    rationale = (
        f"{top.get('symbol', 'Top symbol')} clearly dominates by score gap={score_gap:.6f}, "
        f"relative_gap={relative_gap:.3f}, spread={stability_adjusted_spread:.3f}, and non-overlapping confidence intervals."
        if dominance_detected
        else (
            "Top symbols are statistically similar after accounting for confidence overlap and stability-adjusted spread; "
            "allocate in diversify mode."
        )
    )
    return {
        "deployment_mode": mode,
        "rationale": rationale,
        "dominance_detected": dominance_detected,
        "top_symbols_considered": top_rows,
        "dispersion_metrics": {
            "score_gap": score_gap,
            "relative_gap": relative_gap,
            "confidence_overlap": confidence_overlap,
            "stability_adjusted_spread": stability_adjusted_spread,
        },
        "policy_thresholds": {
            "relative_gap_min": relative_gap_min,
            "stability_adjusted_spread_min": spread_min,
            "confidence_overlap_required_for_diversify": True,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 2 Conditional Edge Hypothesis (non-optimization)")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--event_type", required=True, choices=sorted(PHASE1_EVENT_SOURCES.keys()))
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--max_conditions", type=int, default=20)
    parser.add_argument("--max_actions", type=int, default=9)
    parser.add_argument("--bootstrap_iters", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=13)
    parser.add_argument("--cost_floor", type=float, default=0.01)
    parser.add_argument("--material_tail_threshold", type=float, default=0.02)
    parser.add_argument("--opportunity_horizon_bars", type=int, default=20)
    parser.add_argument("--opportunity_tight_eps", type=float, default=0.005)
    parser.add_argument("--opportunity_near_zero_eps", type=float, default=0.001)
    parser.add_argument("--net_benefit_floor", type=float, default=0.0)
    parser.add_argument("--min_regime_stable_splits", type=int, default=2)
    parser.add_argument("--min_sample_size", type=int, default=20)
    parser.add_argument("--promotion_min_ev", type=float, default=0.0)
    parser.add_argument("--promotion_min_stability_score", type=float, default=0.55)
    parser.add_argument("--promotion_min_sample_size", type=int, default=20)
    parser.add_argument("--require_phase1_pass", type=int, default=1)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    event_source = PHASE1_EVENT_SOURCES[args.event_type]
    phase1_dir = DATA_ROOT / "reports" / event_source.reports_dir / args.run_id
    events_path = phase1_dir / event_source.events_file
    controls_path = phase1_dir / event_source.controls_file
    phase1_events_file_exists = events_path.exists()

    events = _read_csv_allow_empty(events_path) if phase1_events_file_exists else pd.DataFrame()
    controls = _read_csv_allow_empty(controls_path) if controls_path.exists() else pd.DataFrame()
    events, controls = _normalize_phase1_frames(events, controls)

    phase1_summary_path = phase1_dir / event_source.summary_file
    phase1_pass, phase1_structure_pass, phase1_decision = _phase1_pass_status(
        phase1_summary_path,
        require_phase1_pass=bool(args.require_phase1_pass),
    )

    if not events.empty:
        if "symbol" not in events.columns:
            if len(symbols) == 1:
                events["symbol"] = symbols[0]
            else:
                raise ValueError("Phase 1 events missing symbol column and multiple symbols were requested")
        if "symbol" in events.columns:
            events = events[events["symbol"].astype(str).isin(symbols)].copy()

    if events.empty:
        out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "phase2" / args.run_id / args.event_type
        ensure_dir(out_dir)
        cand_path = out_dir / "phase2_candidates.csv"
        prom_path = out_dir / "promoted_candidates.json"
        symbol_eval_path = out_dir / "phase2_symbol_evaluation.csv"
        manifest_path = out_dir / "phase2_manifests.json"
        summary_path = out_dir / "phase2_summary.md"

        pd.DataFrame(columns=PRIMARY_OUTPUT_COLUMNS).to_csv(cand_path, index=False)
        pd.DataFrame(columns=SYMBOL_OUTPUT_COLUMNS).to_csv(symbol_eval_path, index=False)
        prom_payload = {
            "run_id": args.run_id,
            "event_type": args.event_type,
            "decision": "freeze",
            "phase1_pass": bool(phase1_pass),
            "phase1_structure_pass": bool(phase1_structure_pass),
            "phase1_decision": phase1_decision,
            "promoted_count": 0,
            "deployment_mode": DEPLOYMENT_MODE_DIVERSIFY,
            "deployment_mode_rationale": "No phase-1 events are available, so deployment defaults to diversify mode.",
            "deployment_dispersion_test": {
                "deployment_mode": DEPLOYMENT_MODE_DIVERSIFY,
                "rationale": "No phase-1 events are available, so deployment defaults to diversify mode.",
                "dominance_detected": False,
                "top_symbols_considered": [],
                "dispersion_metrics": {
                    "score_gap": 0.0,
                    "relative_gap": 0.0,
                    "confidence_overlap": True,
                    "stability_adjusted_spread": 0.0,
                },
            },
            "candidates": [],
            "reason": "no_phase1_events",
            "phase1_events_file_exists": bool(phase1_events_file_exists),
        }
        prom_path.write_text(json.dumps(prom_payload, indent=2), encoding="utf-8")
        manifest_payload = {
            "run_id": args.run_id,
            "event_type": args.event_type,
            "phase1_pass": bool(phase1_pass),
            "phase1_structure_pass": bool(phase1_structure_pass),
            "phase1_decision": phase1_decision,
            "conditions_generated": 0,
            "actions_generated": 0,
            "conditions_evaluated": 0,
            "actions_evaluated": 0,
            "candidates_evaluated": 0,
            "caps": {
                "max_conditions": int(args.max_conditions),
                "max_actions": int(args.max_actions),
                "condition_cap_pass": True,
                "action_cap_pass": True,
                "simplicity_gate_pass": True,
            },
            "opportunity": {
                "horizon_bars": int(args.opportunity_horizon_bars),
                "tight_ci_eps": float(args.opportunity_tight_eps),
            },
            "gates": {
                "min_regime_stable_splits": int(args.min_regime_stable_splits),
            },
            "no_phase1_events": True,
            "phase1_events_file_exists": bool(phase1_events_file_exists),
            "deployment_mode": DEPLOYMENT_MODE_DIVERSIFY,
            "deployment_mode_rationale": "No phase-1 events are available, so deployment defaults to diversify mode.",
        }
        manifest_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")
        summary_path.write_text(
            "\n".join(
                [
                    "# Phase 2 Conditional Edge Hypothesis",
                    "",
                    f"Run ID: `{args.run_id}`",
                    f"Event type: `{args.event_type}`",
                    "Decision: **FREEZE**",
                    f"Phase 1 status: `{phase1_decision}`",
                    "",
                    "No Phase 1 events were available for the requested symbols.",
                    f"Deployment mode: {DEPLOYMENT_MODE_DIVERSIFY}",
                    "Deployment rationale: No phase-1 events are available, so deployment defaults to diversify mode.",
                ]
            ),
            encoding="utf-8",
        )
        logging.info("No Phase 1 events available; wrote frozen Phase 2 artifacts to %s", out_dir)
        return 0

    events, controls = _attach_forward_opportunity(
        events=events,
        controls=controls,
        run_id=args.run_id,
        symbols=symbols,
        timeframe="15m",
        horizon_bars=args.opportunity_horizon_bars,
    )
    events = _prepare_baseline(events, controls)

    if "split_label" not in events.columns:
        if "enter_ts" not in events.columns or events["enter_ts"].isna().all():
            raise ValueError("Phase 2 requires split_label or valid enter_ts for walk-forward split assignment")
        events["split_label"] = build_walk_forward_split_labels(events, time_col="enter_ts", symbol_col="symbol")

    missing_splits = ~events["split_label"].isin(["train", "validation", "test"])
    if missing_splits.any():
        raise ValueError("Phase 2 split_label contains invalid values")

    all_conditions = _build_conditions(events)
    all_actions = _build_actions()
    conditions = all_conditions[: args.max_conditions]
    actions = all_actions[: args.max_actions]

    condition_cap_pass = bool(len(all_conditions) <= args.max_conditions)
    action_cap_pass = bool(len(all_actions) <= args.max_actions)
    simplicity_pass = bool(condition_cap_pass and action_cap_pass)

    rows = []
    if phase1_pass:
        for i, cond in enumerate(conditions):
            mask = cond.mask_fn(events)
            sub = events[mask].copy()
            if len(sub) < args.min_sample_size:
                continue
            for j, action in enumerate(actions):
                res = _evaluate_candidate(
                    sub,
                    condition=cond,
                    action=action,
                    bootstrap_iters=args.bootstrap_iters,
                    seed=args.seed + i * 1000 + j * 50,
                    cost_floor=args.cost_floor,
                    tail_material_threshold=args.material_tail_threshold,
                    opportunity_tight_eps=args.opportunity_tight_eps,
                    opportunity_near_zero_eps=args.opportunity_near_zero_eps,
                    net_benefit_floor=args.net_benefit_floor,
                    simplicity_gate=simplicity_pass,
                    min_regime_stable_splits=args.min_regime_stable_splits,
                    total_event_count=len(events),
                )
                rows.append(res)

    candidates = pd.DataFrame(rows)
    if candidates.empty:
        candidates = pd.DataFrame(columns=PRIMARY_OUTPUT_COLUMNS)
    else:
        candidates["test_p_value"] = pd.to_numeric(candidates.get("test_p_value"), errors="coerce")
        candidates["test_p_value_adj_bh"] = _bh_adjust(candidates["test_p_value"].fillna(1.0))
        candidates["gate_multiplicity"] = candidates["test_p_value_adj_bh"] <= 0.05
        if "robustness_score" in candidates.columns:
            candidates["robustness_score"] = (
                pd.to_numeric(candidates["robustness_score"], errors="coerce").fillna(0.0)
                * np.where(candidates["gate_multiplicity"].astype(bool), 1.0, 0.75)
            )
        candidates["expectancy_per_trade"] = pd.to_numeric(candidates.get("expectancy_per_trade"), errors="coerce").fillna(0.0)
        candidates["event_frequency"] = pd.to_numeric(candidates.get("event_frequency"), errors="coerce").fillna(0.0)
        candidates["profit_density_score"] = (
            candidates["expectancy_per_trade"] * candidates["robustness_score"] * candidates["event_frequency"]
        )
        candidates["gate_all"] = (
            candidates["gate_pass"].astype(bool)
            & candidates["gate_oos_validation_test"].astype(bool)
            & candidates["gate_multiplicity"].astype(bool)
        )
        for idx, row in candidates.iterrows():
            reasons = [x for x in str(row.get("fail_reasons", "")).split(",") if x]
            if not bool(row.get("gate_multiplicity", False)):
                reasons.append("gate_multiplicity")
            candidates.at[idx, "fail_reasons"] = ",".join(dict.fromkeys(reasons))

    symbol_eval = _build_symbol_evaluation_table(
        events=events,
        candidates=candidates,
        conditions=conditions,
        actions=actions,
        event_type=args.event_type,
        min_ev=float(args.promotion_min_ev),
        min_stability_score=float(args.promotion_min_stability_score),
        min_sample_size=int(args.promotion_min_sample_size),
    )

    rejection_reason_counts: Dict[str, int] = {}
    if not symbol_eval.empty and "rejection_reason_codes" in symbol_eval.columns:
        for reason_codes in symbol_eval["rejection_reason_codes"].fillna("").astype(str):
            for reason in [x for x in reason_codes.split(",") if x]:
                rejection_reason_counts[reason] = rejection_reason_counts.get(reason, 0) + 1

    promoted = candidates[candidates.get("gate_all", False)].copy() if not candidates.empty else pd.DataFrame()
    if not promoted.empty and not symbol_eval.empty:
        deployable_ids = set(
            symbol_eval[symbol_eval["deployable"].astype(bool)]["candidate_id"].astype(str).unique().tolist()
        )
        promoted = promoted[promoted["candidate_id"].astype(str).isin(deployable_ids)].copy()
    if not promoted.empty:
        promoted = promoted.sort_values(
            ["profit_density_score", "delta_adverse_mean", "delta_opportunity_mean"],
            ascending=[False, True, False],
        ).head(2)

    deployment_policy = _deployment_mode_from_symbol_dispersion(symbol_eval)
    summary_decision = "promote" if (phase1_pass and not promoted.empty) else "freeze"

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "phase2" / args.run_id / args.event_type
    ensure_dir(out_dir)

    cand_path = out_dir / "phase2_candidates.csv"
    symbol_eval_path = out_dir / "phase2_symbol_evaluation.csv"
    prom_path = out_dir / "promoted_candidates.json"
    manifest_path = out_dir / "phase2_manifests.json"
    summary_path = out_dir / "phase2_summary.md"

    candidates.to_csv(cand_path, index=False)
    symbol_eval.to_csv(symbol_eval_path, index=False)
    prom_payload = {
        "run_id": args.run_id,
        "event_type": args.event_type,
        "decision": summary_decision,
        "phase1_pass": bool(phase1_pass),
        "phase1_structure_pass": bool(phase1_structure_pass),
        "phase1_decision": phase1_decision,
        "promoted_count": int(len(promoted)) if isinstance(promoted, pd.DataFrame) else 0,
        "rejected_count": int((~symbol_eval["deployable"].astype(bool)).sum()) if not symbol_eval.empty else 0,
        "rejected_by_reason": dict(sorted(rejection_reason_counts.items())),
        "deployment_mode": str(deployment_policy.get("deployment_mode", DEPLOYMENT_MODE_DIVERSIFY)),
        "deployment_mode_rationale": str(deployment_policy.get("rationale", "")),
        "deployment_dispersion_test": deployment_policy,
        "candidates": promoted.to_dict(orient="records") if isinstance(promoted, pd.DataFrame) and not promoted.empty else [],
    }
    prom_path.write_text(json.dumps(prom_payload, indent=2), encoding="utf-8")

    manifest_payload = {
        "run_id": args.run_id,
        "event_type": args.event_type,
        "phase1_pass": bool(phase1_pass),
        "phase1_structure_pass": bool(phase1_structure_pass),
        "phase1_decision": phase1_decision,
        "conditions_generated": int(len(all_conditions)),
        "actions_generated": int(len(all_actions)),
        "conditions_evaluated": int(len(conditions)),
        "actions_evaluated": int(len(actions)),
        "candidates_evaluated": int(len(candidates)),
        "caps": {
            "max_conditions": int(args.max_conditions),
            "max_actions": int(args.max_actions),
            "condition_cap_pass": condition_cap_pass,
            "action_cap_pass": action_cap_pass,
            "simplicity_gate_pass": simplicity_pass,
        },
        "opportunity": {
            "horizon_bars": int(args.opportunity_horizon_bars),
            "tight_ci_eps": float(args.opportunity_tight_eps),
            "near_zero_eps": float(args.opportunity_near_zero_eps),
            "net_benefit_floor": float(args.net_benefit_floor),
        },
        "gates": {
            "min_regime_stable_splits": int(args.min_regime_stable_splits),
            "promotion_min_ev": float(args.promotion_min_ev),
            "promotion_min_stability_score": float(args.promotion_min_stability_score),
            "promotion_min_sample_size": int(args.promotion_min_sample_size),
            "oos_gate": "validation_and_test_adverse_improvement_required",
            "multiplicity_method": "benjamini_hochberg",
            "multiplicity_alpha": 0.05,
        },
        "hypotheses_tested": int(len(candidates)),
        "adjusted_pass_count": int(candidates["gate_multiplicity"].sum()) if not candidates.empty and "gate_multiplicity" in candidates.columns else 0,
        "oos_pass_count": int(candidates["gate_oos_validation_test"].sum()) if not candidates.empty and "gate_oos_validation_test" in candidates.columns else 0,
        "symbol_evaluations": int(len(symbol_eval)),
        "deployable_symbol_rows": int(symbol_eval["deployable"].astype(bool).sum()) if not symbol_eval.empty else 0,
        "rejected_symbol_rows": int((~symbol_eval["deployable"].astype(bool)).sum()) if not symbol_eval.empty else 0,
        "rejected_symbol_rows_by_reason": dict(sorted(rejection_reason_counts.items())),
        "deployment_mode": str(deployment_policy.get("deployment_mode", DEPLOYMENT_MODE_DIVERSIFY)),
        "deployment_mode_rationale": str(deployment_policy.get("rationale", "")),
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")

    fail_rows = candidates[~candidates["gate_all"]].copy() if not candidates.empty and "gate_all" in candidates.columns else pd.DataFrame()

    lines = [
        "# Phase 2 Conditional Edge Hypothesis",
        "",
        f"Run ID: `{args.run_id}`",
        f"Event type: `{args.event_type}`",
        f"Decision: **{summary_decision.upper()}**",
        f"Phase 1 pass required: `{bool(args.require_phase1_pass)}`",
        f"Phase 1 status: `{phase1_decision}`",
        f"Phase 1 structure pass: `{bool(phase1_structure_pass)}`",
        "",
        "## Counts",
        f"- Conditions generated: {len(all_conditions)} (cap={args.max_conditions})",
        f"- Conditions evaluated: {len(conditions)}",
        f"- Actions generated: {len(all_actions)} (cap={args.max_actions})",
        f"- Actions evaluated: {len(actions)}",
        f"- Candidate rows evaluated: {len(candidates)}",
        f"- Simplicity gate pass: {simplicity_pass}",
        f"- Regime stability required splits: {int(args.min_regime_stable_splits)}",
        f"- Promotion min EV: {float(args.promotion_min_ev):.6f}",
        f"- Promotion min stability score: {float(args.promotion_min_stability_score):.3f}",
        f"- Promotion min sample size: {int(args.promotion_min_sample_size)}",
        f"- Symbol rows promoted: {int(symbol_eval['deployable'].astype(bool).sum()) if not symbol_eval.empty else 0}",
        f"- Symbol rows rejected: {int((~symbol_eval['deployable'].astype(bool)).sum()) if not symbol_eval.empty else 0}",
        f"- Rejected by reason: {dict(sorted(rejection_reason_counts.items())) if rejection_reason_counts else {}}",
        f"- Deployment mode: {deployment_policy.get('deployment_mode', DEPLOYMENT_MODE_DIVERSIFY)}",
        f"- Deployment rationale: {deployment_policy.get('rationale', '')}",
        "",
        "## Deployment dispersion test",
        f"- Metrics: {deployment_policy.get('dispersion_metrics', {})}",
        f"- Thresholds: {deployment_policy.get('policy_thresholds', {})}",
        "",
        "## Top candidates",
        _table_text(candidates.sort_values("delta_adverse_mean").head(3)) if not candidates.empty else "No candidates",
        "",
        "## Explicit failures",
        _table_text(fail_rows[["condition", "action", "sample_size", "fail_reasons"]].head(10))
        if not fail_rows.empty
        else "No failures",
        "",
        "## Symbol-level deployability",
        _table_text(
            symbol_eval.sort_values(["deployable", "stability_score", "ev"], ascending=[False, False, False]).head(10)
        )
        if not symbol_eval.empty
        else "No symbol-level evaluations",
        "",
        "## Promoted",
        _table_text(promoted) if isinstance(promoted, pd.DataFrame) and not promoted.empty else "None",
    ]
    summary_path.write_text("\n".join(lines), encoding="utf-8")

    logging.info("Wrote %s", cand_path)
    logging.info("Wrote %s", symbol_eval_path)
    logging.info("Wrote %s", prom_path)
    logging.info("Wrote %s", manifest_path)
    logging.info("Wrote %s", summary_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
