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




PHASE1_ARTIFACTS_BY_EVENT = {
    "vol_shock_relaxation": {
        "phase1_dir": "vol_shock_relaxation",
        "events_csv": "vol_shock_relaxation_events.csv",
        "controls_csv": "vol_shock_relaxation_controls.csv",
        "summary_json": "vol_shock_relaxation_summary.json",
    },
    "directional_exhaustion_after_forced_flow": {
        "phase1_dir": "directional_exhaustion_after_forced_flow",
        "events_csv": "directional_exhaustion_after_forced_flow_events.csv",
        "controls_csv": "directional_exhaustion_after_forced_flow_controls.csv",
        "summary_json": "directional_exhaustion_after_forced_flow_summary.json",
    },
    "liquidity_refill_lag_window": {
        "phase1_dir": "liquidity_refill_lag_window",
        "events_csv": "liquidity_refill_lag_window_events.csv",
        "controls_csv": "liquidity_refill_lag_window_controls.csv",
        "summary_json": "liquidity_refill_lag_window_summary.json",
    },
}
PRIMARY_OUTPUT_COLUMNS = [
    "condition",
    "condition_desc",
    "action",
    "action_family",
    "sample_size",
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
    "gate_a_ci_separated",
    "gate_b_time_stable",
    "gate_b_year_signs",
    "gate_c_regime_stable",
    "gate_d_friction_floor",
    "gate_f_exposure_guard",
    "gate_g_net_benefit",
    "gate_e_simplicity",
    "gate_pass",
    "gate_all",
    "fail_reasons",
]


def _table_text(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False)
    except ImportError:
        return df.to_string(index=False)


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

    if "t_rv_peak" in events.columns:
        conds.extend(
            [
                ConditionSpec("age_bucket_0_8", "t_rv_peak in [0,8]", lambda d: d["t_rv_peak"].fillna(10**9).between(0, 8, inclusive="both")),
                ConditionSpec("age_bucket_9_30", "t_rv_peak in [9,30]", lambda d: d["t_rv_peak"].fillna(10**9).between(9, 30, inclusive="both")),
                ConditionSpec("age_bucket_31_96", "t_rv_peak in [31,96]", lambda d: d["t_rv_peak"].fillna(10**9).between(31, 96, inclusive="both")),
            ]
        )
    if "rv_decay_half_life" in events.columns:
        conds.append(ConditionSpec("near_half_life", "rv_decay_half_life <= 30", lambda d: d["rv_decay_half_life"].fillna(10**9) <= 30))
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
        events = events.copy()
        events["forward_abs_return_h"] = np.nan
        events["forward_abs_return_h_ctrl"] = np.nan
        events["opportunity_value_excess"] = np.nan
        return events, controls

    fwd = pd.concat(rows, ignore_index=True)
    fwd_ts = fwd.rename(columns={"timestamp": "enter_ts"})

    out_events = events.copy()
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

    out_controls = controls.copy()
    if not out_controls.empty and "event_id" in out_controls.columns and "control_idx" in out_controls.columns:
        event_to_symbol = out_events[["event_id", "symbol"]].drop_duplicates()
        out_controls = out_controls.merge(event_to_symbol, on="event_id", how="left")
        out_controls = out_controls.merge(
            fwd[["symbol", "bar_idx", "forward_abs_return_h"]].rename(columns={"bar_idx": "control_idx", "forward_abs_return_h": "forward_abs_return_h_ctrl_row"}),
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

    out_events["opportunity_value_excess"] = out_events["forward_abs_return_h"] - out_events["forward_abs_return_h_ctrl"]
    out_events["opportunity_value_excess"] = out_events["opportunity_value_excess"].where(
        out_events["opportunity_value_excess"].notna(),
        out_events["forward_abs_return_h"],
    )
    return out_events, out_controls


def _prepare_baseline(events: pd.DataFrame, controls: pd.DataFrame) -> pd.DataFrame:
    out = events.copy()
    out["baseline_mode"] = "event_proxy_only"

    sec = out["secondary_shock_within_h"].fillna(0).astype(float)
    rng = out["range_pct_96"].fillna(0).astype(float).clip(lower=0.0)
    opp = out["relaxed_within_96"].fillna(0).astype(float)
    out["adverse_proxy"] = 0.5 * sec + 0.5 * rng
    out["opportunity_proxy"] = opp

    if controls.empty or "event_id" not in controls.columns or "event_id" not in out.columns:
        out["adverse_proxy_ctrl"] = np.nan
        out["opportunity_proxy_ctrl"] = np.nan
        out["adverse_proxy_excess"] = out["adverse_proxy"]
        out["opportunity_proxy_excess"] = out["opportunity_proxy"]
        return out

    numeric_ctrl = controls.groupby("event_id", as_index=False).mean(numeric_only=True)
    ctrl_cols = [c for c in ["secondary_shock_within_h", "range_pct_96", "relaxed_within_96"] if c in numeric_ctrl.columns]
    if not ctrl_cols:
        out["adverse_proxy_ctrl"] = np.nan
        out["opportunity_proxy_ctrl"] = np.nan
        out["adverse_proxy_excess"] = out["adverse_proxy"]
        out["opportunity_proxy_excess"] = out["opportunity_proxy"]
        return out

    rename_map = {c: f"{c}_ctrl" for c in ctrl_cols}
    merged = out.merge(numeric_ctrl[["event_id"] + ctrl_cols].rename(columns=rename_map), on="event_id", how="left")

    sec_ctrl = merged.get("secondary_shock_within_h_ctrl", pd.Series(np.nan, index=merged.index)).fillna(0).astype(float)
    rng_ctrl = merged.get("range_pct_96_ctrl", pd.Series(np.nan, index=merged.index)).fillna(0).astype(float).clip(lower=0.0)
    opp_ctrl = merged.get("relaxed_within_96_ctrl", pd.Series(np.nan, index=merged.index)).fillna(0).astype(float)

    merged["adverse_proxy_ctrl"] = 0.5 * sec_ctrl + 0.5 * rng_ctrl
    merged["opportunity_proxy_ctrl"] = opp_ctrl
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
        t_sec = sub["time_to_secondary_shock"].fillna(10**9).astype(float).to_numpy()
        adverse_delta = -(t_sec <= d).astype(float) * adverse
        exposure_delta = -np.full(len(sub), min(1.0, d / 96.0), dtype=float)
        opportunity_delta = exposure_delta * opp_value
        return adverse_delta, opportunity_delta, exposure_delta

    if action.name == "reenable_at_half_life":
        t_half = sub["rv_decay_half_life"].fillna(10**9).astype(float).to_numpy()
        t_sec = sub["time_to_secondary_shock"].fillna(10**9).astype(float).to_numpy()
        adverse_delta = -(t_sec <= t_half).astype(float) * adverse
        exposure_delta = -np.clip(t_half / 96.0, 0.0, 1.0)
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


def _gate_regime_stability(sub: pd.DataFrame, effect_col: str, condition_name: str) -> bool:
    # If condition itself is on split variable, skip that split.
    checks: List[pd.Series] = []
    if not condition_name.startswith("symbol_") and "symbol" in sub.columns:
        checks.append(sub.groupby("symbol")[effect_col].mean())
    if not condition_name.startswith("vol_regime_") and "vol_regime" in sub.columns:
        checks.append(sub.groupby("vol_regime")[effect_col].mean())
    for s in checks:
        nz = [v for v in s.tolist() if abs(v) > 1e-12]
        if not nz:
            return False
        # Improvement should stay non-positive (adverse reduction) across splits.
        if any(v > 0 for v in nz):
            return False
    return True


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
    gate_c = _gate_regime_stability(tmp, "adverse_effect", condition_name=condition.name)

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

    return {
        "condition": condition.name,
        "condition_desc": condition.description,
        "action": action.name,
        "action_family": action.family,
        "sample_size": int(len(sub)),
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
        "gate_d_friction_floor": gate_d,
        "opportunity_forward_mean": opportunity_forward,
        "opportunity_tail_mean": opportunity_tail,
        "opportunity_composite_mean": opportunity_composite,
        "opportunity_cost_mean": opportunity_cost,
        "net_benefit_mean": net_benefit,
        "gate_f_exposure_guard": gate_f,
        "gate_g_net_benefit": gate_g,
        "gate_e_simplicity": gate_e,
        "gate_pass": bool(gate_a and gate_b and gate_c and gate_d and gate_f and gate_g and gate_e),
        "gate_all": bool(gate_a and gate_b and gate_c and gate_d and gate_f and gate_g and gate_e),
        "fail_reasons": ",".join(fail_reasons) if fail_reasons else "",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 2 Conditional Edge Hypothesis (non-optimization)")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--event_type", required=True, choices=sorted(PHASE1_ARTIFACTS_BY_EVENT.keys()))
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
    parser.add_argument("--min_sample_size", type=int, default=20)
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

    artifacts = PHASE1_ARTIFACTS_BY_EVENT[args.event_type]
    phase1_dir = DATA_ROOT / "reports" / str(artifacts["phase1_dir"]) / args.run_id
    events_path = phase1_dir / str(artifacts["events_csv"])
    controls_path = phase1_dir / str(artifacts["controls_csv"])
    if not events_path.exists():
        raise ValueError(f"Missing Phase 1 events: {events_path}")

    events = pd.read_csv(events_path)
    if events.empty:
        raise ValueError("No events in Phase 1 output; Phase 2 cannot proceed")
    if "symbol" in events.columns:
        events = events[events["symbol"].astype(str).isin(symbols)].copy()
    enter_series = None
    for ts_col in ["enter_ts", "anchor_ts", "exit_ts"]:
        if ts_col in events.columns:
            enter_series = pd.to_datetime(events[ts_col], utc=True, errors="coerce")
            if enter_series.notna().any():
                break
    if enter_series is None:
        raise ValueError("Phase 1 events missing timestamp columns: expected one of enter_ts/anchor_ts/exit_ts")
    events["enter_ts"] = enter_series
    events["year"] = events["enter_ts"].dt.year.fillna(0).astype(int)

    # ensure required metric columns exist
    for c in ["relaxed_within_96", "auc_excess_rv", "rv_decay_half_life", "secondary_shock_within_h", "range_pct_96", "time_to_secondary_shock", "time_to_relax", "t_rv_peak"]:
        if c not in events.columns:
            events[c] = np.nan

    controls = pd.read_csv(controls_path) if controls_path.exists() else pd.DataFrame()
    events, controls = _attach_forward_opportunity(
        events=events,
        controls=controls,
        run_id=args.run_id,
        symbols=symbols,
        timeframe="15m",
        horizon_bars=args.opportunity_horizon_bars,
    )
    events = _prepare_baseline(events, controls)

    phase1_summary_path = phase1_dir / str(artifacts["summary_json"])
    phase1_pass = True
    phase1_structure_pass = True
    phase1_decision = "unknown"
    if args.require_phase1_pass:
        if not phase1_summary_path.exists():
            phase1_pass = False
            phase1_structure_pass = False
            phase1_decision = "missing_summary"
        else:
            try:
                phase1_payload = json.loads(phase1_summary_path.read_text(encoding="utf-8"))
                phase1_decision = str(phase1_payload.get("decision", "unknown"))
                gates = phase1_payload.get("gates", {}) if isinstance(phase1_payload, dict) else {}
                if "phase1_structure_pass" in phase1_payload:
                    phase1_structure_pass = bool(phase1_payload.get("phase1_structure_pass"))
                elif isinstance(gates, dict) and gates:
                    phase1_structure_pass = bool(
                        gates.get("phase_pass", False)
                        and gates.get("sign_pass", False)
                        and gates.get("non_degenerate_count", False)
                    )
                else:
                    # Legacy phase1 summaries (e.g. DEFF/LRL) do not emit decision gates.
                    # In that case, require only that event rows were generated.
                    phase1_structure_pass = bool(int(phase1_payload.get("events", 0) or 0) > 0)
                    if phase1_decision == "unknown":
                        phase1_decision = "unscored"
                phase1_pass = phase1_structure_pass
            except json.JSONDecodeError:
                phase1_pass = False
                phase1_structure_pass = False
                phase1_decision = "invalid_summary"

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
                )
                rows.append(res)

    candidates = pd.DataFrame(rows)
    if candidates.empty:
        candidates = pd.DataFrame(columns=PRIMARY_OUTPUT_COLUMNS)

    promoted = candidates[candidates.get("gate_all", False)].copy() if not candidates.empty else pd.DataFrame()
    if not promoted.empty:
        promoted = promoted.sort_values(["delta_adverse_mean", "delta_opportunity_mean"], ascending=[True, False]).head(2)

    summary_decision = "promote" if (phase1_pass and not promoted.empty) else "freeze"

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "phase2" / args.run_id / args.event_type
    ensure_dir(out_dir)

    cand_path = out_dir / "phase2_candidates.csv"
    prom_path = out_dir / "promoted_candidates.json"
    manifest_path = out_dir / "phase2_manifests.json"
    summary_path = out_dir / "phase2_summary.md"

    candidates.to_csv(cand_path, index=False)
    prom_payload = {
        "run_id": args.run_id,
        "event_type": args.event_type,
        "decision": summary_decision,
        "phase1_pass": bool(phase1_pass),
        "phase1_structure_pass": bool(phase1_structure_pass),
        "phase1_decision": phase1_decision,
        "promoted_count": int(len(promoted)) if isinstance(promoted, pd.DataFrame) else 0,
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
        "",
        "## Top candidates",
        _table_text(candidates.sort_values("delta_adverse_mean").head(3)) if not candidates.empty else "No candidates",
        "",
        "## Explicit failures",
        _table_text(fail_rows[["condition", "action", "sample_size", "fail_reasons"]].head(10))
        if not fail_rows.empty
        else "No failures",
        "",
        "## Promoted",
        _table_text(promoted) if isinstance(promoted, pd.DataFrame) and not promoted.empty else "None",
    ]
    summary_path.write_text("\n".join(lines), encoding="utf-8")

    logging.info("Wrote %s", cand_path)
    logging.info("Wrote %s", prom_path)
    logging.info("Wrote %s", manifest_path)
    logging.info("Wrote %s", summary_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
