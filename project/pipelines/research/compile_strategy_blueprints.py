from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.execution_costs import resolve_execution_costs
from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.selection_log import append_selection_log
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from strategy_dsl.policies import event_policy, overlay_defaults
from strategy_dsl.schema import (
    Blueprint,
    ConditionNodeSpec,
    EntrySpec,
    EvaluationSpec,
    ExitSpec,
    LineageSpec,
    OverlaySpec,
    SizingSpec,
    SymbolScopeSpec,
)

COMPILER_VERSION = "strategy_dsl_v1"
DETERMINISTIC_TS = "1970-01-01T00:00:00Z"
QUALITY_MIN_ROBUSTNESS = 0.60
QUALITY_MIN_EVENTS = 100
QUALITY_MAX_COST_RATIO = 0.60
TRIM_WF_WORST_K = 1

SESSION_CONDITION_MAP: Dict[str, Tuple[int, int]] = {
    "session_asia": (0, 7),
    "session_eu": (8, 15),
    "session_us": (16, 23),
}

BULL_BEAR_CONDITION_MAP: Dict[str, int] = {
    "bull_bear_bull": 1,
    "bull_bear_bear": -1,
}

VOL_REGIME_CONDITION_MAP: Dict[str, int] = {
    "vol_regime_low": 0,
    "vol_regime_mid": 1,
    "vol_regime_medium": 1,
    "vol_regime_high": 2,
}


from strategy_dsl.contract_v1 import NonExecutableConditionError, normalize_entry_condition as normalize_entry_condition_v1

PHASE1_EVENT_FILES: Dict[str, Tuple[str, str]] = {
    "vol_shock_relaxation": ("vol_shock_relaxation", "vol_shock_relaxation_events.csv"),
    "liquidity_refill_lag_window": ("liquidity_refill_lag_window", "liquidity_refill_lag_window_events.csv"),
    "liquidity_absence_window": ("liquidity_absence_window", "liquidity_absence_window_events.csv"),
    "vol_aftershock_window": ("vol_aftershock_window", "vol_aftershock_window_events.csv"),
    "directional_exhaustion_after_forced_flow": (
        "directional_exhaustion_after_forced_flow",
        "directional_exhaustion_after_forced_flow_events.csv",
    ),
    "cross_venue_desync": ("cross_venue_desync", "cross_venue_desync_events.csv"),
    "liquidity_vacuum": ("liquidity_vacuum", "liquidity_vacuum_events.csv"),
    "funding_extreme_reversal_window": ("funding_extreme_reversal_window", "funding_extreme_reversal_window_events.csv"),
    "range_compression_breakout_window": ("range_compression_breakout_window", "range_compression_breakout_window_events.csv"),
}


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        out = float(value)
        if np.isnan(out):
            return default
        return out
    except (TypeError, ValueError):
        return default


def _safe_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _checklist_decision(run_id: str) -> str:
    path = DATA_ROOT / "runs" / run_id / "research_checklist" / "checklist.json"
    if not path.exists():
        return "missing"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return "invalid"
    if not isinstance(payload, dict):
        return "invalid"
    return str(payload.get("decision", "missing")).strip().upper() or "missing"


def _parse_symbols(symbols_csv: str) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in str(symbols_csv).split(","):
        symbol = raw.strip().upper()
        if symbol and symbol not in seen:
            out.append(symbol)
            seen.add(symbol)
    return out


def _sanitize(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", str(value).strip().lower()).strip("_")


def _candidate_id(row: Dict[str, object], idx: int) -> str:
    cid = str(row.get("candidate_id", "")).strip()
    if cid:
        return cid
    condition = str(row.get("condition", "")).strip()
    action = str(row.get("action", "")).strip()
    if condition and action:
        return f"{condition}__{action}"
    return f"candidate_{idx}"


def _load_phase2_table(run_id: str, event_type: str) -> pd.DataFrame:
    path = DATA_ROOT / "reports" / "phase2" / run_id / event_type / "phase2_candidates.csv"
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return df
    if "candidate_id" not in df.columns:
        ids = [_candidate_id(row, idx) for idx, row in enumerate(df.to_dict(orient="records"))]
        df = df.copy()
        df["candidate_id"] = ids
    return df


def _event_stats(run_id: str, event_type: str) -> Dict[str, np.ndarray]:
    report_dir, file_name = PHASE1_EVENT_FILES.get(event_type, (event_type, f"{event_type}_events.csv"))
    path = DATA_ROOT / "reports" / report_dir / run_id / file_name
    if not path.exists():
        return {"half_life": np.array([]), "adverse": np.array([]), "favorable": np.array([])}

    try:
        df = pd.read_csv(path)
    except Exception:
        return {"half_life": np.array([]), "adverse": np.array([]), "favorable": np.array([])}
    if df.empty:
        return {"half_life": np.array([]), "adverse": np.array([]), "favorable": np.array([])}

    def _pick(cols: Sequence[str]) -> np.ndarray:
        vals: List[float] = []
        for col in cols:
            if col not in df.columns:
                continue
            arr = pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna().to_numpy(dtype=float)
            if arr.size == 0:
                continue
            vals.extend(arr.tolist())
        if not vals:
            return np.array([])
        out = np.asarray(vals, dtype=float)
        out = out[np.isfinite(out)]
        return out

    half_life = _pick(["rv_decay_half_life", "timing_landmark", "time_to_relax", "parent_time_to_relax", "time_to_secondary_shock"])
    half_life = half_life[half_life > 0.0] if half_life.size else half_life

    adverse = _pick(["mae_96", "range_pct_96", "adverse_proxy_excess", "adverse_proxy", "secondary_shock_within_h"])
    adverse = np.abs(adverse)
    adverse = adverse[adverse > 0.0] if adverse.size else adverse

    favorable = _pick(["mfe_post_end", "forward_abs_return_h", "opportunity_value_excess", "opportunity_proxy_excess", "relaxed_within_96"])
    favorable = np.abs(favorable)
    favorable = favorable[favorable > 0.0] if favorable.size else favorable
    return {"half_life": half_life, "adverse": adverse, "favorable": favorable}


def _parse_symbol_scope(
    row: Dict[str, object],
    run_symbols: List[str],
    condition_symbol_override: str | None = None,
) -> SymbolScopeSpec:
    candidate_symbol = (
        str(condition_symbol_override).strip().upper()
        if condition_symbol_override is not None
        else str(row.get("candidate_symbol", "")).strip().upper()
    ) or "ALL"
    if candidate_symbol != "ALL":
        target = candidate_symbol if candidate_symbol in run_symbols else (run_symbols[0] if run_symbols else candidate_symbol)
        return SymbolScopeSpec(mode="single_symbol", symbols=[target], candidate_symbol=candidate_symbol)
    rollout = _as_bool(row.get("rollout_eligible", False))
    if rollout and len(run_symbols) > 1:
        return SymbolScopeSpec(mode="multi_symbol", symbols=list(run_symbols), candidate_symbol="ALL")
    return SymbolScopeSpec(
        mode="all" if len(run_symbols) > 1 else "single_symbol",
        symbols=list(run_symbols) if run_symbols else ["ALL"],
        candidate_symbol="ALL",
    )


def _derive_time_stop(half_life: np.ndarray, row: Dict[str, object]) -> int:
    if half_life.size:
        val = int(round(float(np.nanmedian(half_life))))
        return int(min(192, max(4, val)))
    base = _safe_int(row.get("sample_size", row.get("n_events", 24)), 24)
    return int(min(96, max(8, int(round(base * 0.1)))))


def _derive_stop_target(stats: Dict[str, np.ndarray], row: Dict[str, object]) -> Tuple[float, float]:
    adverse = stats.get("adverse", np.array([]))
    favorable = stats.get("favorable", np.array([]))

    if adverse.size:
        stop = float(np.nanpercentile(adverse, 75))
    else:
        stop = max(0.001, abs(_safe_float(row.get("delta_adverse_mean"), 0.01)) * 1.5)

    if favorable.size:
        target = float(np.nanpercentile(favorable, 60))
    else:
        target = max(stop * 1.1, abs(_safe_float(row.get("delta_opportunity_mean"), 0.02)) * 1.25)

    stop = float(min(5.0, max(0.0005, stop)))
    target = float(min(8.0, max(0.0005, target)))
    return stop, target


def _derive_delay(action: str, robustness: float, time_stop_bars: int) -> int:
    act = str(action).strip().lower()
    if act.startswith("delay_"):
        return max(0, _safe_int(act.split("_")[-1], 0))
    if act == "reenable_at_half_life":
        return max(1, int(round(time_stop_bars / 2.0)))
    if robustness < 0.5:
        return 12
    if robustness < 0.7:
        return 8
    if robustness < 0.85:
        return 4
    return 0


def _passes_quality_floor(row: Dict[str, object], *, strict_cost_fields: bool = True, min_events: int = QUALITY_MIN_EVENTS) -> bool:
    robustness = _safe_float(row.get("robustness_score"), 0.0)
    n_events = _safe_int(row.get("n_events", row.get("sample_size", 0)), 0)
    if robustness < QUALITY_MIN_ROBUSTNESS:
        return False
    if n_events < min_events:
        return False
    if strict_cost_fields:
        after_cost = _safe_float(row.get("after_cost_expectancy_per_trade"), np.nan)
        stressed_after_cost = _safe_float(row.get("stressed_after_cost_expectancy_per_trade"), np.nan)
        cost_ratio = _safe_float(row.get("cost_ratio"), np.nan)
        if np.isnan(after_cost) or np.isnan(stressed_after_cost) or np.isnan(cost_ratio):
            return False
    else:
        after_cost = _safe_float(row.get("after_cost_expectancy_per_trade"), _safe_float(row.get("expectancy_per_trade"), 0.0))
        stressed_after_cost = _safe_float(row.get("stressed_after_cost_expectancy_per_trade"), after_cost)
        cost_ratio = _safe_float(row.get("cost_ratio"), 0.0)
    if after_cost <= 0.0:
        return False
    if stressed_after_cost <= 0.0:
        return False
    if cost_ratio >= QUALITY_MAX_COST_RATIO:
        return False
    if "gate_bridge_tradable" in row and not _as_bool(row.get("gate_bridge_tradable", False)):
        return False
    return True


def _normalize_entry_condition(
    raw_condition: object,
    *,
    event_type: str,
    candidate_id: str,
    run_symbols: List[str],
) -> Tuple[str, List[ConditionNodeSpec], str | None]:
    return normalize_entry_condition_v1(
        raw_condition,
        event_type=event_type,
        candidate_id=candidate_id,
        run_symbols=run_symbols,
    )



def _entry_from_row(
    row: Dict[str, object],
    event_type: str,
    time_stop_bars: int,
    run_symbols: List[str],
    candidate_id: str,
) -> Tuple[EntrySpec, str | None]:
    policy = event_policy(event_type)
    robustness = _safe_float(row.get("robustness_score"), 0.0)
    action = str(row.get("action", "no_action"))
    delay = _derive_delay(action=action, robustness=robustness, time_stop_bars=time_stop_bars)
    cooldown = max(8, delay * 3, int(round(time_stop_bars / 3.0)))
    if robustness < 0.75:
        cooldown = max(cooldown, 12)
    if robustness < 0.60:
        cooldown = max(cooldown, 16)
    confirmations = [str(x) for x in policy.get("confirmations", [])]
    oos_gate = _as_bool(row.get("gate_oos_validation", row.get("gate_oos_validation_test", True)))
    if not oos_gate and "oos_validation_pass" in confirmations:
        confirmations = [x for x in confirmations if x != "oos_validation_pass"]
    condition, condition_nodes, condition_symbol = _normalize_entry_condition(
        row.get("condition", "all"),
        event_type=event_type,
        candidate_id=candidate_id,
        run_symbols=run_symbols,
    )

    return (
        EntrySpec(
            triggers=[str(x) for x in policy.get("triggers", ["event_detected"])],
            conditions=[condition],
            confirmations=confirmations,
            delay_bars=delay,
            cooldown_bars=cooldown,
            condition_logic="all",
            condition_nodes=condition_nodes,
            arm_bars=delay,
            reentry_lockout_bars=max(cooldown, delay),
        ),
        condition_symbol,
    )


def _sizing_from_row(row: Dict[str, object]) -> SizingSpec:
    robustness = _safe_float(row.get("robustness_score"), 0.0)
    capacity = _safe_float(row.get("capacity_proxy"), 0.0)
    if robustness >= 0.75 and capacity >= 0.5:
        return SizingSpec(
            mode="vol_target",
            risk_per_trade=None,
            target_vol=0.12,
            max_gross_leverage=1.0,
            max_position_scale=1.0,
            portfolio_risk_budget=1.0,
            symbol_risk_budget=1.0,
        )
    risk = 0.004 if robustness >= 0.7 else 0.003
    return SizingSpec(
        mode="fixed_risk",
        risk_per_trade=risk,
        target_vol=None,
        max_gross_leverage=1.0,
        max_position_scale=1.0,
        portfolio_risk_budget=1.0,
        symbol_risk_budget=1.0,
    )


def _evaluation_from_row(row: Dict[str, object], fees_bps: float, slippage_bps: float) -> EvaluationSpec:
    n_events = _safe_int(row.get("n_events", row.get("sample_size", 0)), 0)
    min_trades = int(max(20, min(200, n_events // 2 if n_events else 20)))
    return EvaluationSpec(
        min_trades=min_trades,
        cost_model={
            "fees_bps": float(fees_bps),
            "slippage_bps": float(slippage_bps),
            "funding_included": True,
        },
        robustness_flags={
            "oos_required": _as_bool(row.get("gate_oos_validation", row.get("gate_oos_validation_test", True))),
            "multiplicity_required": _as_bool(row.get("gate_multiplicity", True)),
            "regime_stability_required": _as_bool(row.get("gate_c_regime_stable", True)),
        },
    )


def _action_overlays(action: str) -> List[OverlaySpec]:
    normalized = str(action or "").strip().lower()
    if normalized == "entry_gate_skip":
        return [OverlaySpec(name="risk_throttle", params={"size_scale": 0.0})]
    if normalized.startswith("risk_throttle_"):
        try:
            scale = float(normalized.split("_")[-1])
        except ValueError:
            scale = 1.0
        scale = max(0.0, min(1.0, scale))
        return [OverlaySpec(name="risk_throttle", params={"size_scale": scale})]
    return []


def _merge_overlays(policy_overlays: List[OverlaySpec], action_overlays: List[OverlaySpec]) -> List[OverlaySpec]:
    by_name: Dict[str, OverlaySpec] = {}
    order: List[str] = []

    for overlay in policy_overlays:
        if overlay.name not in by_name:
            order.append(overlay.name)
        by_name[overlay.name] = overlay
    for overlay in action_overlays:
        if overlay.name not in by_name:
            order.append(overlay.name)
        by_name[overlay.name] = overlay

    return [by_name[name] for name in order]


def _blueprint_behavior_fingerprint(blueprint: Blueprint) -> str:
    payload = {
        "event_type": blueprint.event_type,
        "direction": blueprint.direction,
        "symbol_scope": {
            "mode": blueprint.symbol_scope.mode,
            "symbols": list(blueprint.symbol_scope.symbols),
            "candidate_symbol": blueprint.symbol_scope.candidate_symbol,
        },
        "entry": {
            "triggers": list(blueprint.entry.triggers),
            "conditions": list(blueprint.entry.conditions),
            "confirmations": list(blueprint.entry.confirmations),
            "delay_bars": int(blueprint.entry.delay_bars),
            "cooldown_bars": int(blueprint.entry.cooldown_bars),
            "condition_logic": blueprint.entry.condition_logic,
            "condition_nodes": [
                {
                    "feature": node.feature,
                    "operator": node.operator,
                    "value": node.value,
                    "value_high": node.value_high,
                    "lookback_bars": node.lookback_bars,
                    "window_bars": node.window_bars,
                }
                for node in blueprint.entry.condition_nodes
            ],
            "arm_bars": int(blueprint.entry.arm_bars),
            "reentry_lockout_bars": int(blueprint.entry.reentry_lockout_bars),
        },
        "exit": {
            "time_stop_bars": blueprint.exit.time_stop_bars,
            "invalidation": dict(blueprint.exit.invalidation),
            "stop_type": blueprint.exit.stop_type,
            "stop_value": blueprint.exit.stop_value,
            "target_type": blueprint.exit.target_type,
            "target_value": blueprint.exit.target_value,
            "trailing_stop_type": blueprint.exit.trailing_stop_type,
            "trailing_stop_value": blueprint.exit.trailing_stop_value,
            "break_even_r": blueprint.exit.break_even_r,
        },
        "sizing": {
            "mode": blueprint.sizing.mode,
            "risk_per_trade": blueprint.sizing.risk_per_trade,
            "target_vol": blueprint.sizing.target_vol,
            "max_gross_leverage": blueprint.sizing.max_gross_leverage,
            "max_position_scale": blueprint.sizing.max_position_scale,
            "portfolio_risk_budget": blueprint.sizing.portfolio_risk_budget,
            "symbol_risk_budget": blueprint.sizing.symbol_risk_budget,
        },
        "overlays": sorted(
            [{"name": overlay.name, "params": dict(overlay.params)} for overlay in blueprint.overlays],
            key=lambda row: str(row.get("name", "")),
        ),
        "evaluation": {
            "min_trades": blueprint.evaluation.min_trades,
            "cost_model": dict(blueprint.evaluation.cost_model),
            "robustness_flags": dict(blueprint.evaluation.robustness_flags),
        },
    }
    return json.dumps(payload, sort_keys=True)


def _dedupe_blueprints_by_behavior(
    blueprints: List[Blueprint],
) -> Tuple[List[Blueprint], Dict[str, object]]:
    seen: Dict[str, str] = {}
    keep_map: Dict[str, List[str]] = {}
    dropped_ids: List[str] = []
    deduped: List[Blueprint] = []

    for blueprint in blueprints:
        fingerprint = _blueprint_behavior_fingerprint(blueprint)
        keep_id = seen.get(fingerprint)
        if keep_id is None:
            seen[fingerprint] = blueprint.id
            deduped.append(blueprint)
            continue
        dropped_ids.append(blueprint.id)
        keep_map.setdefault(keep_id, []).append(blueprint.id)

    return deduped, {
        "behavior_duplicate_count": int(len(dropped_ids)),
        "behavior_duplicate_dropped_ids": dropped_ids,
        "behavior_duplicate_keep_map": {key: sorted(value) for key, value in keep_map.items()},
    }


def _rank_key(row: Dict[str, object]) -> Tuple[float, float, float, float, str]:
    oos_gate = _as_bool(row.get("gate_oos_validation", row.get("gate_oos_validation_test", False)))
    after_cost = _safe_float(row.get("after_cost_expectancy_per_trade"), _safe_float(row.get("expectancy_per_trade"), 0.0))
    stressed_after_cost = _safe_float(row.get("stressed_after_cost_expectancy_per_trade"), after_cost)
    cost_ratio = _safe_float(row.get("cost_ratio"), 1.0)
    return (
        -after_cost,
        -stressed_after_cost,
        -_safe_float(row.get("robustness_score"), 0.0),
        -float(oos_gate),
        -float(_as_bool(row.get("gate_multiplicity", False))),
        cost_ratio,
        str(row.get("candidate_id", "")),
    )


def _choose_event_rows(
    run_id: str,
    event_type: str,
    edge_rows: List[Dict[str, object]],
    phase2_df: pd.DataFrame,
    max_per_event: int,
    allow_fallback_blueprints: bool,
    strict_cost_fields: bool,
    min_events: int,
    naive_validation: Dict[Tuple[str, str], bool] = None,
    allow_naive_entry_fail: bool = False,
) -> Tuple[List[Dict[str, object]], Dict[str, object], pd.DataFrame]:
    phase2_lookup: Dict[str, Dict[str, object]] = {}
    if not phase2_df.empty:
        for idx, row in enumerate(phase2_df.to_dict(orient="records")):
            cid = _candidate_id(row, idx)
            phase2_lookup[cid] = dict(row)

    def _enrich(row: Dict[str, object], idx: int, status_default: str) -> Dict[str, object]:
        cid = str(row.get("candidate_id", "")).strip() or _candidate_id(row, idx)
        merged = dict(phase2_lookup.get(cid, {}))
        merged.update(dict(row))
        merged["candidate_id"] = cid
        merged["event"] = event_type
        merged["status"] = str(merged.get("status", status_default))
        if not str(merged.get("run_id", "")).strip():
            merged["run_id"] = run_id
        if "candidate_symbol" not in merged:
            merged["candidate_symbol"] = str(merged.get("symbol", "ALL")).upper() if "symbol" in merged else "ALL"
        if "source_path" not in merged or not str(merged.get("source_path", "")).strip():
            merged["source_path"] = str(DATA_ROOT / "reports" / "phase2" / run_id / event_type / "phase2_candidates.csv")
        return merged

    enriched_edge_rows = [_enrich(row, idx, str(row.get("status", "DRAFT"))) for idx, row in enumerate(edge_rows)]
    
    # Pre-filter for eligibility
    eligible_rows = []
    ineligible_rows = [] # Track for logging
    
    for row in enriched_edge_rows:
        is_eligible = True
        reason = ""
        
        # Naive Entry Gate
        if not allow_naive_entry_fail and naive_validation is not None:
            cid = str(row.get("candidate_id", "")).strip()
            if not naive_validation.get((event_type, cid), False):
                is_eligible = False
                reason = "naive_entry_fail"
        
        if is_eligible:
            eligible_rows.append(row)
        else:
            # Mark as ineligible in selection tracking
            row["_ineligible_reason"] = reason
            ineligible_rows.append(row)

    # Use eligible_rows for promotion/fallback logic instead of enriched_edge_rows
    # ... but we still want to log ineligible ones in selection_df
    
    selection_data = []
    
    # Log ineligible first (rank 0)
    for c in ineligible_rows:
        selection_data.append({
            "candidate_id": c.get("candidate_id"),
            "event_type": event_type,
            "rank": 0,
            "selected": False,
            "reason": f"ineligible_{c.get('_ineligible_reason')}",
            "status": c.get("status"),
            "robustness_score": c.get("robustness_score"),
            "n_events": c.get("n_events"),
        })

    diagnostics: Dict[str, object] = {
        "event_type": event_type,
        "selected_count": 0,
        "rejected_quality_floor_count": 0,
        "reason": "no_candidates",
        "used_fallback": False,
    }
    
    rejected_quality_floor_count = 0
    
    promoted = [row for row in eligible_rows if str(row.get("status", "")).upper() == "PROMOTED"]
    if promoted:
        promoted_sorted = sorted(promoted, key=_rank_key)
        promoted_quality = [row for row in promoted_sorted if _passes_quality_floor(row, strict_cost_fields=strict_cost_fields, min_events=min_events)]
        rejected_quality_floor_count += max(0, len(promoted_sorted) - len(promoted_quality))
        
        # Log promoted candidates not selected due to quality
        for c in promoted_sorted:
            if c not in promoted_quality:
                selection_data.append({"candidate_id": c.get("candidate_id"), "event_type": event_type, "rank": 0, "selected": False, "reason": "quality_floor_fail_promoted", "status": "PROMOTED", "robustness_score": c.get("robustness_score"), "n_events": c.get("n_events")})

        if promoted_quality:
            selected = promoted_quality[:max_per_event]
            for i, c in enumerate(selected):
                selection_data.append({"candidate_id": c.get("candidate_id"), "event_type": event_type, "rank": i+1, "selected": True, "reason": "promoted_quality", "status": "PROMOTED", "robustness_score": c.get("robustness_score"), "n_events": c.get("n_events")})
            
            # Log cap exclusions
            for i, c in enumerate(promoted_quality[max_per_event:]):
                selection_data.append({"candidate_id": c.get("candidate_id"), "event_type": event_type, "rank": max_per_event+i+1, "selected": False, "reason": "excluded_by_cap", "status": "PROMOTED", "robustness_score": c.get("robustness_score"), "n_events": c.get("n_events")})

            diagnostics.update(
                {
                    "selected_count": int(len(selected)),
                    "rejected_quality_floor_count": int(rejected_quality_floor_count),
                    "reason": "promoted_quality",
                    "used_fallback": False,
                }
            )
            return selected, diagnostics, pd.DataFrame(selection_data)
            
        if not allow_fallback_blueprints:
            diagnostics.update(
                {
                    "selected_count": 0,
                    "rejected_quality_floor_count": int(rejected_quality_floor_count),
                    "reason": "rejected_all_promoted_quality_floor",
                    "used_fallback": False,
                }
            )
            return [], diagnostics, pd.DataFrame(selection_data)

        non_promoted_rows = [row for row in enriched_edge_rows if str(row.get("status", "")).upper() != "PROMOTED"]
        if non_promoted_rows:
            non_promoted_sorted = sorted(non_promoted_rows, key=_rank_key)
            non_promoted_quality = [row for row in non_promoted_sorted if _passes_quality_floor(row, strict_cost_fields=strict_cost_fields, min_events=min_events)]
            rejected_quality_floor_count += max(0, len(non_promoted_sorted) - len(non_promoted_quality))
            
            for c in non_promoted_sorted:
                if c not in non_promoted_quality:
                     selection_data.append({"candidate_id": c.get("candidate_id"), "event_type": event_type, "rank": 0, "selected": False, "reason": "quality_floor_fail_fallback_non_promoted", "status": c.get("status"), "robustness_score": c.get("robustness_score"), "n_events": c.get("n_events")})

            if non_promoted_quality:
                selected = non_promoted_quality[:max_per_event]
                for i, c in enumerate(selected):
                    selection_data.append({"candidate_id": c.get("candidate_id"), "event_type": event_type, "rank": i+1, "selected": True, "reason": "fallback_non_promoted_quality", "status": c.get("status"), "robustness_score": c.get("robustness_score"), "n_events": c.get("n_events")})
                
                for i, c in enumerate(non_promoted_quality[max_per_event:]):
                    selection_data.append({"candidate_id": c.get("candidate_id"), "event_type": event_type, "rank": max_per_event+i+1, "selected": False, "reason": "excluded_by_cap", "status": c.get("status"), "robustness_score": c.get("robustness_score"), "n_events": c.get("n_events")})

                diagnostics.update(
                    {
                        "selected_count": int(len(selected)),
                        "rejected_quality_floor_count": int(rejected_quality_floor_count),
                        "reason": "fallback_non_promoted_quality",
                        "used_fallback": True,
                    }
                )
                return selected, diagnostics, pd.DataFrame(selection_data)

        diagnostics.update(
            {
                "selected_count": 0,
                "rejected_quality_floor_count": int(rejected_quality_floor_count),
                "reason": "fallback_enabled_but_no_quality_rows",
                "used_fallback": True,
            }
        )
        return [], diagnostics, pd.DataFrame(selection_data)

    if not allow_fallback_blueprints:
        diagnostics.update(
            {
                "selected_count": 0,
                "rejected_quality_floor_count": int(rejected_quality_floor_count),
                "reason": "no_promoted_and_fallback_disabled",
                "used_fallback": False,
            }
        )
        return [], diagnostics, pd.DataFrame(selection_data)

    if enriched_edge_rows:
        edge_sorted = sorted(enriched_edge_rows, key=_rank_key)
        edge_quality = [row for row in edge_sorted if _passes_quality_floor(row, strict_cost_fields=strict_cost_fields, min_events=min_events)]
        rejected_quality_floor_count += max(0, len(edge_sorted) - len(edge_quality))
        
        for c in edge_sorted:
            if c not in edge_quality:
                 selection_data.append({"candidate_id": c.get("candidate_id"), "event_type": event_type, "rank": 0, "selected": False, "reason": "quality_floor_fail_fallback_edge", "status": c.get("status"), "robustness_score": c.get("robustness_score"), "n_events": c.get("n_events")})

        if edge_quality:
            selected = edge_quality[:max_per_event]
            for i, c in enumerate(selected):
                selection_data.append({"candidate_id": c.get("candidate_id"), "event_type": event_type, "rank": i+1, "selected": True, "reason": "fallback_edge_quality", "status": c.get("status"), "robustness_score": c.get("robustness_score"), "n_events": c.get("n_events")})
            for i, c in enumerate(edge_quality[max_per_event:]):
                selection_data.append({"candidate_id": c.get("candidate_id"), "event_type": event_type, "rank": max_per_event+i+1, "selected": False, "reason": "excluded_by_cap", "status": c.get("status"), "robustness_score": c.get("robustness_score"), "n_events": c.get("n_events")})

            diagnostics.update(
                {
                    "selected_count": int(len(selected)),
                    "rejected_quality_floor_count": int(rejected_quality_floor_count),
                    "reason": "fallback_edge_quality",
                    "used_fallback": True,
                }
            )
            return selected, diagnostics, pd.DataFrame(selection_data)

    if not phase2_df.empty:
        fallback_df = phase2_df.copy()
        for col in ("robustness_score", "profit_density_score"):
            if col not in fallback_df.columns:
                fallback_df[col] = 0.0
        if "candidate_id" not in fallback_df.columns:
            fallback_df["candidate_id"] = [
                _candidate_id(row, idx) for idx, row in enumerate(fallback_df.to_dict(orient="records"))
            ]
        ordered_rows = fallback_df.sort_values(
            by=["robustness_score", "profit_density_score", "candidate_id"],
            ascending=[False, False, True],
        ).to_dict(orient="records")
        parsed_rows: List[Dict[str, object]] = []
        for idx, row in enumerate(ordered_rows):
            parsed_rows.append(_enrich(row, idx, "DRAFT"))
        phase2_quality = [row for row in parsed_rows if _passes_quality_floor(row, strict_cost_fields=strict_cost_fields, min_events=min_events)]
        rejected_quality_floor_count += max(0, len(parsed_rows) - len(phase2_quality))
        
        for c in parsed_rows:
            if c not in phase2_quality:
                 selection_data.append({"candidate_id": c.get("candidate_id"), "event_type": event_type, "rank": 0, "selected": False, "reason": "quality_floor_fail_phase2", "status": "DRAFT", "robustness_score": c.get("robustness_score"), "n_events": c.get("n_events")})

        if phase2_quality:
            selected = phase2_quality[:max_per_event]
            for i, c in enumerate(selected):
                selection_data.append({"candidate_id": c.get("candidate_id"), "event_type": event_type, "rank": i+1, "selected": True, "reason": "fallback_phase2_quality", "status": "DRAFT", "robustness_score": c.get("robustness_score"), "n_events": c.get("n_events")})
            for i, c in enumerate(phase2_quality[max_per_event:]):
                selection_data.append({"candidate_id": c.get("candidate_id"), "event_type": event_type, "rank": max_per_event+i+1, "selected": False, "reason": "excluded_by_cap", "status": "DRAFT", "robustness_score": c.get("robustness_score"), "n_events": c.get("n_events")})

            diagnostics.update(
                {
                    "selected_count": int(len(selected)),
                    "rejected_quality_floor_count": int(rejected_quality_floor_count),
                    "reason": "fallback_phase2_quality",
                    "used_fallback": True,
                }
            )
            return selected, diagnostics, pd.DataFrame(selection_data)

    diagnostics.update(
        {
            "selected_count": 0,
            "rejected_quality_floor_count": int(rejected_quality_floor_count),
            "reason": "fallback_enabled_but_no_quality_rows",
            "used_fallback": True,
        }
    )
    return [], diagnostics, pd.DataFrame(selection_data)


def _build_blueprint(
    run_id: str,
    run_symbols: List[str],
    event_type: str,
    row: Dict[str, object],
    phase2_lookup: Dict[str, Dict[str, object]],
    stats: Dict[str, np.ndarray],
    fees_bps: float,
    slippage_bps: float,
    min_events: int = 0,
) -> Blueprint:
    candidate_id = str(row.get("candidate_id", "")).strip() or _candidate_id(row, 0)
    detail = phase2_lookup.get(candidate_id, {})
    merged = dict(row)
    merged.update({k: v for k, v in detail.items() if k not in merged or pd.isna(merged.get(k))})

    time_stop_bars = _derive_time_stop(stats.get("half_life", np.array([])), merged)
    stop_value, target_value = _derive_stop_target(stats=stats, row=merged)
    entry, condition_symbol_override = _entry_from_row(
        merged,
        event_type=event_type,
        time_stop_bars=time_stop_bars,
        run_symbols=run_symbols,
        candidate_id=candidate_id,
    )
    symbol_scope = _parse_symbol_scope(
        merged,
        run_symbols=run_symbols,
        condition_symbol_override=condition_symbol_override,
    )
    sizing = _sizing_from_row(merged)
    evaluation = _evaluation_from_row(merged, fees_bps=fees_bps, slippage_bps=slippage_bps)

    policy = event_policy(event_type)
    overlay_rows = overlay_defaults(
        names=[str(x) for x in policy.get("overlays", [])],
        robustness_score=_safe_float(merged.get("robustness_score"), 0.0),
    )
    policy_overlays = [OverlaySpec(name=str(item["name"]), params=dict(item["params"])) for item in overlay_rows]
    overlays = _merge_overlays(policy_overlays, _action_overlays(str(merged.get("action", ""))))

    bp_id = _sanitize(f"bp_{run_id}_{event_type}_{candidate_id}_{symbol_scope.mode}")
    blueprint = Blueprint(
        id=bp_id,
        run_id=run_id,
        event_type=event_type,
        candidate_id=candidate_id,
        symbol_scope=symbol_scope,
        direction=str(policy.get("direction", "conditional")),  # type: ignore[arg-type]
        entry=entry,
        exit=ExitSpec(
            time_stop_bars=time_stop_bars,
            invalidation={"metric": "adverse_proxy", "operator": ">", "value": round(stop_value, 6)},
            stop_type=str(policy.get("stop_type", "range_pct")),  # type: ignore[arg-type]
            stop_value=round(stop_value, 6),
            target_type=str(policy.get("target_type", "range_pct")),  # type: ignore[arg-type]
            target_value=round(target_value, 6),
            trailing_stop_type=str(policy.get("stop_type", "range_pct")),  # type: ignore[arg-type]
            trailing_stop_value=round(stop_value * 0.75, 6),
            break_even_r=1.0,
        ),
        sizing=sizing,
        overlays=overlays,
        evaluation=evaluation,
        lineage=LineageSpec(
            source_path=str(merged.get("source_path", "")),
            compiler_version=COMPILER_VERSION,
            generated_at_utc=DETERMINISTIC_TS,
            events_count_used_for_gate=_safe_int(merged.get("n_events", merged.get("sample_size", 0)), 0),
            min_events_threshold=int(min_events),
        ),
    )
    blueprint.validate()
    return blueprint


def _load_walkforward_strategy_metrics(run_id: str) -> Tuple[Dict[str, Dict[str, object]], str]:
    """Returns (per_strategy_split_metrics, sha256_digest_of_file). Hash is '' if file absent."""
    path = DATA_ROOT / "reports" / "eval" / run_id / "walkforward_summary.json"
    if not path.exists():
        return {}, ""
    raw_bytes = path.read_bytes()
    file_hash = "sha256:" + hashlib.sha256(raw_bytes).hexdigest()
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except Exception:
        return {}, file_hash
    raw = payload.get("per_strategy_split_metrics", {})
    if not isinstance(raw, dict):
        return {}, file_hash
    out: Dict[str, Dict[str, object]] = {}
    for key, value in raw.items():
        if isinstance(key, str) and isinstance(value, dict):
            out[key] = value
    return out, file_hash


def _load_naive_entry_validation(run_id: str) -> Dict[Tuple[str, str], bool]:
    path = DATA_ROOT / "reports" / "naive_entry" / run_id / "naive_entry_validation.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing naive-entry validation artifact: {path}")
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        raise ValueError(f"Failed reading naive-entry validation artifact: {path}") from exc
    required = {"event_type", "candidate_id", "naive_pass"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Naive-entry validation artifact missing columns {sorted(missing)}: {path}")
    out: Dict[Tuple[str, str], bool] = {}
    for _, row in df.iterrows():
        event_type = str(row.get("event_type", "")).strip()
        candidate_id = str(row.get("candidate_id", "")).strip()
        if not event_type or not candidate_id:
            continue
        out[(event_type, candidate_id)] = _as_bool(row.get("naive_pass", False))
    return out


def _annotate_blueprints_with_walkforward_evidence(
    blueprints: List[Blueprint],
    run_id: str,
    evidence_hash: str,
) -> Tuple[List[Blueprint], Dict[str, object]]:
    """Annotate blueprints with WF status. All blueprints returned; underperformers flagged in lineage."""
    metrics_by_strategy, _ = _load_walkforward_strategy_metrics(run_id=run_id)
    if not metrics_by_strategy or not blueprints:
        annotated = [
            dataclasses.replace(bp, lineage=dataclasses.replace(bp.lineage, wf_status="pass", wf_evidence_hash=evidence_hash))
            for bp in blueprints
        ]
        return annotated, {
            "wf_evidence_used": False,
            "trim_split": "validation",
            "trimmed_zero_trade": 0,
            "trimmed_worst_negative": 0,
            "wf_trimmed_all": False,
            "dropped_blueprint_ids": [],
        }

    by_id = {bp.id: bp for bp in blueprints}
    zero_trade_ids: set[str] = set()
    negative_rows: List[Tuple[float, str]] = []

    for strategy_id, split_map in metrics_by_strategy.items():
        if not strategy_id.startswith("dsl_interpreter_v1__"):
            continue
        bp_id = strategy_id.replace("dsl_interpreter_v1__", "", 1)
        if bp_id not in by_id:
            continue
        validation = split_map.get("validation", {})
        if not isinstance(validation, dict):
            continue
        trades = _safe_int(validation.get("total_trades", 0), 0)
        stressed = _safe_float(validation.get("stressed_net_pnl", 0.0), 0.0)
        if trades <= 0:
            zero_trade_ids.add(bp_id)
        elif stressed < 0.0:
            negative_rows.append((stressed, bp_id))

    trim_ids: set[str] = set(zero_trade_ids)
    negative_rows = sorted(negative_rows, key=lambda row: (row[0], row[1]))
    worst_negative_ids: set[str] = {bp_id for _, bp_id in negative_rows[:TRIM_WF_WORST_K]}
    trim_ids.update(worst_negative_ids)

    annotated: List[Blueprint] = []
    for bp in blueprints:
        if bp.id in zero_trade_ids:
            wf_status = "trimmed_zero_trade"
        elif bp.id in worst_negative_ids:
            wf_status = "trimmed_worst_negative"
        else:
            wf_status = "pass"
        new_lineage = dataclasses.replace(bp.lineage, wf_status=wf_status, wf_evidence_hash=evidence_hash)
        annotated.append(dataclasses.replace(bp, lineage=new_lineage))

    trimmed_count = sum(1 for bp in annotated if bp.lineage.wf_status.startswith("trimmed"))
    return annotated, {
        "wf_evidence_used": True,
        "trim_split": "validation",
        "trimmed_zero_trade": int(sum(1 for bp_id in trim_ids if bp_id in zero_trade_ids)),
        "trimmed_worst_negative": int(sum(1 for bp_id in trim_ids if bp_id in worst_negative_ids)),
        "wf_trimmed_all": bool(trimmed_count == len(annotated) and trimmed_count > 0),
        "dropped_blueprint_ids": sorted(trim_ids),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Compile deterministic strategy blueprints from enriched edge candidates")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--max_per_event", type=int, default=2)
    parser.add_argument("--fees_bps", type=float, default=None)
    parser.add_argument("--slippage_bps", type=float, default=None)
    parser.add_argument("--cost_bps", type=float, default=None)
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--strict_cost_fields", type=int, default=1)
    parser.add_argument("--ignore_checklist", type=int, default=0)
    parser.add_argument("--allow_fallback_blueprints", type=int, default=0)
    parser.add_argument("--allow_non_executable_conditions", type=int, default=0)
    parser.add_argument("--allow_naive_entry_fail", type=int, default=0)
    parser.add_argument("--min_events_floor", type=int, default=QUALITY_MIN_EVENTS)
    parser.add_argument("--candidates_file", default=None)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--max_total_compiles_per_run", type=int, default=100)
    args = parser.parse_args()

    run_symbols = _parse_symbols(args.symbols)
    if not run_symbols:
        print("--symbols must include at least one symbol", file=sys.stderr)
        return 1

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "strategy_blueprints" / args.run_id
    ensure_dir(out_dir)
    resolved_costs = resolve_execution_costs(
        project_root=PROJECT_ROOT,
        config_paths=args.config,
        fees_bps=args.fees_bps,
        slippage_bps=args.slippage_bps,
        cost_bps=args.cost_bps,
    )

    params = {
        "run_id": args.run_id,
        "symbols": run_symbols,
        "max_per_event": int(args.max_per_event),
        "fees_bps": float(resolved_costs.fee_bps_per_side),
        "slippage_bps": float(resolved_costs.slippage_bps_per_fill),
        "cost_bps": float(resolved_costs.cost_bps),
        "execution_cost_config": dict(resolved_costs.execution_model),
        "execution_cost_config_digest": resolved_costs.config_digest,
        "strict_cost_fields": int(args.strict_cost_fields),
        "ignore_checklist": int(args.ignore_checklist),
        "allow_fallback_blueprints": int(args.allow_fallback_blueprints),
        "allow_non_executable_conditions": int(args.allow_non_executable_conditions),
        "allow_naive_entry_fail": int(args.allow_naive_entry_fail),
        "min_events_floor": int(args.min_events_floor),
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("compile_strategy_blueprints", args.run_id, params, inputs, outputs)

    try:
        checklist_path = DATA_ROOT / "runs" / args.run_id / "research_checklist" / "checklist.json"
        inputs.append({"path": str(checklist_path), "rows": None, "start_ts": None, "end_ts": None})
        if not int(args.ignore_checklist):
            decision = _checklist_decision(run_id=args.run_id)
            if decision != "PROMOTE":
                raise ValueError(
                    f"Checklist decision must be PROMOTE before blueprint compilation (run_id={args.run_id}, decision={decision}). "
                    "Use --ignore_checklist 1 only for explicit override."
                )

        if args.candidates_file:
            edge_path = Path(args.candidates_file)
            inputs.append({"path": str(edge_path), "rows": None, "start_ts": None, "end_ts": None})
            if edge_path.exists():
                if edge_path.suffix == ".parquet":
                    edge_df = pd.read_parquet(edge_path)
                else:
                    edge_df = pd.read_csv(edge_path)
            else:
                edge_df = pd.DataFrame()
        else:
            edge_path = DATA_ROOT / "reports" / "edge_candidates" / args.run_id / "edge_candidates_normalized.csv"
            inputs.append({"path": str(edge_path), "rows": None, "start_ts": None, "end_ts": None})
            if edge_path.exists():
                edge_df = pd.read_csv(edge_path)
            else:
                edge_df = pd.DataFrame()

        if not edge_df.empty:
            if "gate_bridge_tradable" not in edge_df.columns:
                _msg = (
                    "gate_bridge_tradable column missing from candidates — bridge stage may not have run "
                    f"(run_id={args.run_id}, path={edge_path}). "
                    "All candidates would silently pass the bridge gate. "
                    "Re-run the bridge scoring stage or use --ignore_checklist 1 to bypass for non-production use."
                )
                if not int(args.ignore_checklist):
                    raise ValueError(_msg)
                print(f"WARNING: {_msg}", file=sys.stderr)
            if "candidate_id" not in edge_df.columns:
                edge_df["candidate_id"] = [
                    _candidate_id(row, idx) for idx, row in enumerate(edge_df.to_dict(orient="records"))
                ]
            edge_rows = edge_df.to_dict(orient="records")
        else:
            edge_rows = []

        naive_validation: Dict[Tuple[str, str], bool] = {}
        if not int(args.allow_naive_entry_fail):
            naive_validation = _load_naive_entry_validation(run_id=args.run_id)
            inputs.append(
                {
                    "path": str(DATA_ROOT / "reports" / "naive_entry" / args.run_id / "naive_entry_validation.csv"),
                    "rows": int(len(naive_validation)),
                    "start_ts": None,
                    "end_ts": None,
                }
            )

        phase2_root = DATA_ROOT / "reports" / "phase2" / args.run_id
        event_types = sorted([p.name for p in phase2_root.iterdir() if p.is_dir()]) if phase2_root.exists() else []
        if not event_types and edge_rows:
            event_types = sorted({str(row.get("event", "")).strip() for row in edge_rows if str(row.get("event", "")).strip()})

        blueprints: List[Blueprint] = []
        fallback_count = 0
        rejected_non_executable_condition_count = 0
        rejected_naive_entry_count = 0
        rejected_quality_floor_count = 0
        per_event_rejections: Dict[str, Dict[str, object]] = {}
        selection_records: List[Dict[str, object]] = []
        attempt_records: List[Dict[str, object]] = []
        selected_for_build_count = 0

        for event_type in event_types:
            event_edge_rows = [row for row in edge_rows if str(row.get("event", "")).strip() == event_type]
            phase2_df = _load_phase2_table(run_id=args.run_id, event_type=event_type)
            phase2_lookup = {}
            if not phase2_df.empty:
                for idx, row in enumerate(phase2_df.to_dict(orient="records")):
                    cid = _candidate_id(row, idx)
                    phase2_lookup[cid] = dict(row)

            # Updated _choose_event_rows returns (selected, diagnostics, selection_df)
            selected, selection_diag, selection_df = _choose_event_rows(
                run_id=args.run_id,
                event_type=event_type,
                edge_rows=event_edge_rows,
                phase2_df=phase2_df,
                max_per_event=int(args.max_per_event),
                allow_fallback_blueprints=bool(int(args.allow_fallback_blueprints)),
                strict_cost_fields=bool(int(args.strict_cost_fields)),
                min_events=int(args.min_events_floor),
            )
            if not selection_df.empty:
                selection_records.extend(selection_df.to_dict(orient="records"))

            rejected_quality_floor_count += int(selection_diag.get("rejected_quality_floor_count", 0))
            per_event_rejections[event_type] = {
                "selection_reason": str(selection_diag.get("reason", "unknown")),
                "rejected_quality_floor_count": int(selection_diag.get("rejected_quality_floor_count", 0)),
                "selected_count": int(selection_diag.get("selected_count", 0)),
                "used_fallback": bool(selection_diag.get("used_fallback", False)),
                "rejected_non_executable_condition_count": 0,
                "rejected_naive_entry_count": 0,
            }
            if not selected:
                continue
            if bool(selection_diag.get("used_fallback", False)):
                fallback_count += 1

            # Log attempts
            for row in selected:
                attempt_records.append({
                    "candidate_id": row.get("candidate_id"),
                    "attempted": True,
                    "success": False, # Default
                    "fail_reason": "unknown", # Default
                    "exception_type": ""
                })

            strict_selected_rows: List[Dict[str, object]] = []
            if not int(args.allow_naive_entry_fail):
                for row in selected:
                    cid = str(row.get("candidate_id", "")).strip() or _candidate_id(row, 0)
                    passed = naive_validation.get((event_type, cid))
                    if passed is True:
                        strict_selected_rows.append(row)
                    else:
                        rejected_naive_entry_count += 1
                        per_event_rejections[event_type]["rejected_naive_entry_count"] = int(
                            per_event_rejections[event_type]["rejected_naive_entry_count"]
                        ) + 1
                        for rec in attempt_records:
                            if rec["candidate_id"] == cid:
                                rec["fail_reason"] = "naive_entry_fail"
            else:
                strict_selected_rows = selected

            selected_for_build_count += len(strict_selected_rows)
            stats = _event_stats(run_id=args.run_id, event_type=event_type)
            event_non_exec_errors: List[str] = []
            event_blueprint_count_before = len(blueprints)

            for row in strict_selected_rows:
                cid = str(row.get("candidate_id", "")).strip() or _candidate_id(row, 0)
                try:
                    bp = _build_blueprint(
                        run_id=args.run_id,
                        run_symbols=run_symbols,
                        event_type=event_type,
                        row=row,
                        phase2_lookup=phase2_lookup,
                        stats=stats,
                        fees_bps=float(resolved_costs.fee_bps_per_side),
                        slippage_bps=float(resolved_costs.slippage_bps_per_fill),
                        min_events=int(args.min_events_floor),
                    )
                    blueprints.append(bp)
                    for rec in attempt_records:
                        if rec["candidate_id"] == cid:
                            rec["success"] = True
                            rec["fail_reason"] = ""
                except NonExecutableConditionError as exc:
                    rejected_non_executable_condition_count += 1
                    per_event_rejections[event_type]["rejected_non_executable_condition_count"] = int(
                        per_event_rejections[event_type]["rejected_non_executable_condition_count"]
                    ) + 1
                    
                    for rec in attempt_records:
                        if rec["candidate_id"] == cid:
                            rec["fail_reason"] = "non_executable_condition"
                            rec["exception_type"] = "NonExecutableConditionError"

                    if int(args.allow_non_executable_conditions):
                        continue
                    event_non_exec_errors.append(str(exc))
                    continue
                except Exception as exc:
                    for rec in attempt_records:
                        if rec["candidate_id"] == cid:
                            rec["fail_reason"] = "build_exception"
                            rec["exception_type"] = type(exc).__name__
                    continue

            if event_non_exec_errors and (len(blueprints) == event_blueprint_count_before):
                pass
            if (len(selected) == 0) and (not int(args.allow_naive_entry_fail)):
                pass

        # Write attempts artifact
        if attempt_records:
            pd.DataFrame(attempt_records).to_parquet(out_dir / "compile_attempts.parquet", index=False)

        # Global budget enforcement
        max_total = int(args.max_total_compiles_per_run)
        if len(blueprints) > max_total:
            # Sort by quality metrics to keep best
            # We don't have easy access to metrics here inside Blueprint object without parsing, 
            # but we can look up in phase2_lookup or selection_records.
            # Easier: blueprints are already created. We can sort them by some criteria if we stored it.
            # Blueprints don't store "robustness_score" directly in top level.
            # But we can rely on the fact they were selected in order of quality per event.
            # Stratified sampling: Round robin from each event type?
            # Existing order is by event type (chunks).
            # Simple approach: Shuffle and take N? Or take top N/num_events from each?
            # Let's take top K by rank if we can recover it.
            # Actually, let's just slice for now to satisfy the budget hard constraint.
            # Better: Stratified selection.
            # Group by event_type, take 1 from each, then 2nd from each, until budget full.
            
            by_event: Dict[str, List[Blueprint]] = {}
            for bp in blueprints:
                by_event.setdefault(bp.event_type, []).append(bp)
            
            kept_blueprints = []
            while len(kept_blueprints) < max_total and any(by_event.values()):
                for et in sorted(by_event.keys()):
                    if by_event[et]:
                        kept_blueprints.append(by_event[et].pop(0))
                        if len(kept_blueprints) >= max_total:
                            break
            
            dropped_count = len(blueprints) - len(kept_blueprints)
            print(f"Global budget enforcement: Dropped {dropped_count} blueprints to fit limit {max_total}")
            blueprints = kept_blueprints

        if not blueprints:
            raise ValueError(
                "No blueprints were produced from promoted evidence. "
                "Use --allow_fallback_blueprints 1 only for explicit non-production fallback."
            )

        # Load WF evidence hash (used for annotation)
        _, wf_evidence_hash = _load_walkforward_strategy_metrics(run_id=args.run_id)

        # Dedupe first, then annotate with WF status
        blueprints, duplicate_stats = _dedupe_blueprints_by_behavior(blueprints)
        blueprints = sorted(blueprints, key=lambda b: (b.event_type, b.candidate_id, b.id))
        if not blueprints:
            raise ValueError("No blueprints remained after behavior-level deduplication.")

        blueprints, trim_stats = _annotate_blueprints_with_walkforward_evidence(
            blueprints=blueprints,
            run_id=args.run_id,
            evidence_hash=wf_evidence_hash,
        )
        active_blueprints = [bp for bp in blueprints if not bp.lineage.wf_status.startswith("trimmed")]
        if not active_blueprints:
            if bool(trim_stats.get("wf_trimmed_all", False)):
                # Write all-trimmed blueprints for audit before failing
                out_jsonl = out_dir / "blueprints.jsonl"
                lines = [json.dumps(bp.to_dict(), sort_keys=True) for bp in blueprints]
                out_jsonl.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
                raise ValueError("Walkforward trimming flagged all blueprints; strict mode fails closed.")
            raise ValueError("No blueprints remained after compile filters.")

        out_jsonl = out_dir / "blueprints.jsonl"
        lines = [json.dumps(bp.to_dict(), sort_keys=True) for bp in blueprints]
        out_jsonl.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

        active_count = sum(1 for bp in blueprints if not bp.lineage.wf_status.startswith("trimmed"))
        trimmed_count = len(blueprints) - active_count

        summary = {
            "run_id": args.run_id,
            "compiler_version": COMPILER_VERSION,
            "event_types": event_types,
            "blueprint_count": int(len(blueprints)),
            "fallback_event_count": int(fallback_count),
            "quality_floor": {
                "min_robustness_score": QUALITY_MIN_ROBUSTNESS,
                "min_events": int(args.min_events_floor),
                "after_cost_expectancy_positive_required": True,
                "stressed_after_cost_expectancy_positive_required": True,
                "max_cost_ratio": QUALITY_MAX_COST_RATIO,
                "strict_cost_fields": bool(int(args.strict_cost_fields)),
            },
            "compile_funnel": {
                "selected_for_build": int(selected_for_build_count),
                "build_success": int(len(blueprints)),
                "build_fail_non_executable": int(rejected_non_executable_condition_count),
                "build_fail_naive_entry": int(rejected_naive_entry_count),
                "build_fail_exception": int(sum(
                    1 for r in attempt_records if r.get("fail_reason") == "build_exception"
                )),
                "behavior_dedup_drop": int(duplicate_stats.get("behavior_duplicate_count", 0)),
                "wf_trim_zero_trade": int(trim_stats.get("trimmed_zero_trade", 0)),
                "wf_trim_worst_negative": int(trim_stats.get("trimmed_worst_negative", 0)),
                "written_total": int(len(blueprints)),
                "written_active": int(active_count),
                "written_trimmed": int(trimmed_count),
            },
            "wf_evidence_hash": str(wf_evidence_hash),
            "wf_evidence_source": str(DATA_ROOT / "reports" / "eval" / args.run_id / "walkforward_summary.json"),
            "min_events_threshold_used": int(args.min_events_floor),
            "historical_trim": trim_stats,
            "rejected_non_executable_condition_count": int(rejected_non_executable_condition_count),
            "rejected_naive_entry_count": int(rejected_naive_entry_count),
            "rejected_quality_floor_count": int(rejected_quality_floor_count),
            "wf_trimmed_all": bool(trim_stats.get("wf_trimmed_all", False)),
            "behavior_duplicate_count": int(duplicate_stats.get("behavior_duplicate_count", 0)),
            "behavior_duplicate_dropped_ids": list(duplicate_stats.get("behavior_duplicate_dropped_ids", [])),
            "behavior_duplicate_keep_map": dict(duplicate_stats.get("behavior_duplicate_keep_map", {})),
            "per_event_rejections": per_event_rejections,
            "per_event_counts": {
                event_type: int(sum(1 for bp in blueprints if bp.event_type == event_type)) for event_type in event_types
            },
        }
        out_summary = out_dir / "blueprints_summary.json"
        out_summary.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        
        # Write selection ledger
        if selection_records:
            pd.DataFrame(selection_records).to_parquet(out_dir / "compile_selection.parquet", index=False)

        append_selection_log(
            data_root=DATA_ROOT,
            run_id=args.run_id,
            stage="compile_strategy_blueprints",
            details={
                "selection_split": "validation",
                "test_usage": "read_only",
                "wf_trim_split": str(trim_stats.get("trim_split", "validation")),
                "wf_evidence_used": bool(trim_stats.get("wf_evidence_used", False)),
                "dropped_blueprint_ids": list(trim_stats.get("dropped_blueprint_ids", [])),
                "behavior_duplicate_dropped_ids": list(duplicate_stats.get("behavior_duplicate_dropped_ids", [])),
                "blueprint_count": int(len(blueprints)),
                "strict_cost_fields": bool(int(args.strict_cost_fields)),
                "execution_cost_config_digest": resolved_costs.config_digest,
            },
        )

        outputs.append({"path": str(out_jsonl), "rows": int(len(blueprints)), "start_ts": None, "end_ts": None})
        outputs.append({"path": str(out_summary), "rows": int(len(blueprints)), "start_ts": None, "end_ts": None})
        finalize_manifest(
            manifest,
            "success",
            stats={
                "event_count": int(len(event_types)),
                "blueprint_count": int(len(blueprints)),
                "fallback_event_count": int(fallback_count),
                "trimmed_zero_trade": int(trim_stats.get("trimmed_zero_trade", 0)),
                "trimmed_worst_negative": int(trim_stats.get("trimmed_worst_negative", 0)),
                "rejected_non_executable_condition_count": int(rejected_non_executable_condition_count),
                "rejected_naive_entry_count": int(rejected_naive_entry_count),
                "rejected_quality_floor_count": int(rejected_quality_floor_count),
                "wf_trimmed_all": bool(trim_stats.get("wf_trimmed_all", False)),
            },
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
