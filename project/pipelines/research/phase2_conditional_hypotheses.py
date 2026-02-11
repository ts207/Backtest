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

from pipelines._lib.io_utils import ensure_dir


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

    conds.extend(
        [
            ConditionSpec("age_bucket_0_8", "t_rv_peak in [0,8]", lambda d: d["t_rv_peak"].fillna(10**9).between(0, 8, inclusive="both")),
            ConditionSpec("age_bucket_9_30", "t_rv_peak in [9,30]", lambda d: d["t_rv_peak"].fillna(10**9).between(9, 30, inclusive="both")),
            ConditionSpec("age_bucket_31_96", "t_rv_peak in [31,96]", lambda d: d["t_rv_peak"].fillna(10**9).between(31, 96, inclusive="both")),
            ConditionSpec("near_half_life", "rv_decay_half_life <= 30", lambda d: d["rv_decay_half_life"].fillna(10**9) <= 30),
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
        ActionSpec("entry_gate_skip", "entry_gating", {"k": 0.0}),
        ActionSpec("risk_throttle_0.5", "risk_throttle", {"k": 0.5}),
        ActionSpec("risk_throttle_0", "risk_throttle", {"k": 0.0}),
        ActionSpec("delay_8", "timing", {"delay_bars": 8}),
        ActionSpec("delay_30", "timing", {"delay_bars": 30}),
        ActionSpec("reenable_at_half_life", "timing", {"landmark": "rv_decay_half_life"}),
    ]


def _apply_action_proxy(sub: pd.DataFrame, action: ActionSpec) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    # Returns per-event contributions for: adverse_delta, opportunity_delta, exposure_delta
    # Negative adverse_delta is improvement (risk reduction).
    sec = sub["secondary_shock_within_h"].fillna(0).astype(float).to_numpy()
    rng = sub["range_pct_96"].fillna(0).astype(float).to_numpy()
    adverse = 0.5 * sec + 0.5 * np.clip(rng, 0.0, None)
    opp = sub["relaxed_within_96"].fillna(0).astype(float).to_numpy()

    if action.family in {"entry_gating", "risk_throttle"}:
        k = float(action.params.get("k", 1.0))
        adverse_delta = -(1.0 - k) * adverse
        opportunity_delta = -(1.0 - k) * opp
        exposure_delta = np.full(len(sub), -(1.0 - k), dtype=float)
        return adverse_delta, opportunity_delta, exposure_delta

    if action.name.startswith("delay_"):
        d = int(action.params.get("delay_bars", 0))
        t_sec = sub["time_to_secondary_shock"].fillna(10**9).astype(float).to_numpy()
        t_relax = sub["time_to_relax"].fillna(10**9).astype(float).to_numpy()
        adverse_delta = -(t_sec <= d).astype(float) * adverse
        opportunity_delta = -(t_relax <= d).astype(float) * opp
        exposure_delta = -np.full(len(sub), min(1.0, d / 96.0), dtype=float)
        return adverse_delta, opportunity_delta, exposure_delta

    if action.name == "reenable_at_half_life":
        t_half = sub["rv_decay_half_life"].fillna(10**9).astype(float).to_numpy()
        t_sec = sub["time_to_secondary_shock"].fillna(10**9).astype(float).to_numpy()
        adverse_delta = -(t_sec <= t_half).astype(float) * adverse
        opportunity_delta = -(t_half <= sub["time_to_relax"].fillna(10**9).astype(float).to_numpy()).astype(float) * opp * 0.25
        exposure_delta = -np.clip(t_half / 96.0, 0.0, 1.0)
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
    dominant = max(non_zero.count(1), non_zero.count(-1)) / len(non_zero)
    no_catastrophic = not (1 in non_zero and -1 in non_zero and len(non_zero) >= 3)
    return bool(dominant >= min_ratio and no_catastrophic), ",".join(str(s) for s in signs)


def _gate_regime_stability(sub: pd.DataFrame, effect_col: str, condition_name: str) -> bool:
    # If condition itself is on split variable, skip that split.
    checks = []
    if not condition_name.startswith("symbol_") and "symbol" in sub.columns:
        checks.append(sub.groupby("symbol")[effect_col].mean())
    if not condition_name.startswith("vol_regime_") and "vol_regime" in sub.columns:
        checks.append(sub.groupby("vol_regime")[effect_col].mean())
    for s in checks:
        nz = [v for v in s.tolist() if abs(v) > 1e-12]
        if not nz:
            return False
        signs = set(1 if v > 0 else -1 for v in nz)
        if len(signs) > 1:
            return False
    return True


def _evaluate_candidate(
    sub: pd.DataFrame,
    condition: ConditionSpec,
    action: ActionSpec,
    bootstrap_iters: int,
    seed: int,
    cost_floor: float,
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

    material_tail = bool(np.isfinite(mean_adv) and abs(mean_adv) >= 0.02)
    gate_d = bool(np.isfinite(mean_adv) and abs(mean_adv) >= cost_floor) or material_tail

    return {
        "condition": condition.name,
        "condition_desc": condition.description,
        "action": action.name,
        "action_family": action.family,
        "sample_size": int(len(sub)),
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
        "gate_all": bool(gate_a and gate_b and gate_c and gate_d),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 2 Conditional Edge Hypothesis (non-optimization)")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--event_type", required=True, choices=["vol_shock_relaxation"])
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--max_conditions", type=int, default=20)
    parser.add_argument("--max_actions", type=int, default=9)
    parser.add_argument("--bootstrap_iters", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=13)
    parser.add_argument("--cost_floor", type=float, default=0.01)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    if args.event_type != "vol_shock_relaxation":
        raise ValueError("Phase 2 currently supports only vol_shock_relaxation")

    phase1_dir = DATA_ROOT / "reports" / "vol_shock_relaxation" / args.run_id
    events_path = phase1_dir / "vol_shock_relaxation_events.csv"
    controls_path = phase1_dir / "vol_shock_relaxation_controls.csv"
    if not events_path.exists():
        raise ValueError(f"Missing Phase 1 events: {events_path}")

    events = pd.read_csv(events_path)
    if events.empty:
        raise ValueError("No events in Phase 1 output; Phase 2 cannot proceed")
    if "symbol" in events.columns:
        events = events[events["symbol"].astype(str).isin(symbols)].copy()
    events["enter_ts"] = pd.to_datetime(events["enter_ts"], utc=True, errors="coerce")
    events["year"] = events["enter_ts"].dt.year.fillna(0).astype(int)

    # ensure required metric columns exist
    for c in ["relaxed_within_96", "auc_excess_rv", "rv_decay_half_life", "secondary_shock_within_h", "range_pct_96", "time_to_secondary_shock", "time_to_relax", "t_rv_peak"]:
        if c not in events.columns:
            events[c] = np.nan

    controls = pd.read_csv(controls_path) if controls_path.exists() else pd.DataFrame()

    conditions = _build_conditions(events)
    actions = _build_actions()
    if len(conditions) > args.max_conditions:
        conditions = conditions[: args.max_conditions]
    if len(actions) > args.max_actions:
        actions = actions[: args.max_actions]

    rows = []
    for i, cond in enumerate(conditions):
        mask = cond.mask_fn(events)
        sub = events[mask].copy()
        if len(sub) < 20:
            continue
        for j, action in enumerate(actions):
            res = _evaluate_candidate(
                sub,
                condition=cond,
                action=action,
                bootstrap_iters=args.bootstrap_iters,
                seed=args.seed + i * 1000 + j * 50,
                cost_floor=args.cost_floor,
            )
            rows.append(res)

    candidates = pd.DataFrame(rows)
    if candidates.empty:
        candidates = pd.DataFrame(
            columns=[
                "condition",
                "action",
                "sample_size",
                "delta_adverse_mean",
                "delta_adverse_ci_low",
                "delta_adverse_ci_high",
                "gate_all",
            ]
        )

    promoted = candidates[candidates.get("gate_all", False)].copy() if not candidates.empty else pd.DataFrame()
    if not promoted.empty:
        promoted = promoted.sort_values(["delta_adverse_mean", "delta_opportunity_mean"], ascending=[True, False]).head(2)

    summary_decision = "promote" if not promoted.empty else "freeze"

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
        "promoted_count": int(len(promoted)) if isinstance(promoted, pd.DataFrame) else 0,
        "candidates": promoted.to_dict(orient="records") if isinstance(promoted, pd.DataFrame) and not promoted.empty else [],
    }
    prom_path.write_text(json.dumps(prom_payload, indent=2), encoding="utf-8")

    manifest_payload = {
        "run_id": args.run_id,
        "event_type": args.event_type,
        "conditions_evaluated": int(len(conditions)),
        "actions_evaluated": int(len(actions)),
        "candidates_evaluated": int(len(candidates)),
        "caps": {
            "max_conditions": int(args.max_conditions),
            "max_actions": int(args.max_actions),
            "condition_cap_pass": bool(len(conditions) <= args.max_conditions),
            "action_cap_pass": bool(len(actions) <= args.max_actions),
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
        "",
        "## Counts",
        f"- Conditions evaluated: {len(conditions)} (cap={args.max_conditions})",
        f"- Actions evaluated: {len(actions)} (cap={args.max_actions})",
        f"- Candidate rows evaluated: {len(candidates)}",
        "",
        "## Top candidates",
        _table_text(candidates.sort_values("delta_adverse_mean").head(3)) if not candidates.empty else "No candidates",
        "",
        "## Explicit failures",
        _table_text(fail_rows.head(10)) if not fail_rows.empty else "No failures",
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
