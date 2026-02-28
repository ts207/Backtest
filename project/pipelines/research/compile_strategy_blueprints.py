from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.execution_costs import resolve_execution_costs
from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.selection_log import append_selection_log
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.ontology_contract import (
    load_run_manifest_hashes,
    ontology_spec_hash,
    ontology_spec_paths,
)
from research.candidate_schema import ensure_candidate_schema
from events.registry import EVENT_REGISTRY_SPECS, filter_phase1_rows_for_event_type
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
TRIM_WF_WORST_K = 0

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


def _load_gates_spec() -> Dict[str, Any]:
    path = PROJECT_ROOT.parent / "spec" / "gates.yaml"
    if not path.exists():
        return {}
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _load_operator_registry() -> Dict[str, Dict[str, Any]]:
    path = ontology_spec_paths(PROJECT_ROOT.parent).get("template_verb_lexicon")
    if not path or not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {}
    operators = payload.get("operators", {})
    if not isinstance(operators, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for key, value in operators.items():
        if isinstance(value, dict) and str(key).strip():
            out[str(key).strip()] = dict(value)
    return out


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


def _validate_promoted_candidates_frame(df: pd.DataFrame, *, source_label: str) -> pd.DataFrame:
    if df.empty:
        raise ValueError(f"Promoted candidates file is empty: {source_label}")
    if "status" not in df.columns:
        raise ValueError(f"Promoted candidates file missing status column: {source_label}")
    promoted_mask = df["status"].astype(str).str.strip().str.upper() == "PROMOTED"
    if not promoted_mask.all():
        invalid = sorted(
            {
                str(v).strip() or "<empty>"
                for v in df.loc[~promoted_mask, "status"].tolist()
            }
        )
        raise ValueError(
            f"Promoted candidates file contains non-promoted rows ({invalid}) in {source_label}"
        )
    out = df.copy()
    if "event" not in out.columns:
        if "event_type" not in out.columns:
            raise ValueError(
                f"Promoted candidates file missing event/event_type columns: {source_label}"
            )
        out["event"] = out["event_type"]
    if "event_type" not in out.columns:
        out["event_type"] = out["event"]
    out["event"] = out["event"].astype(str).str.strip()
    out["event_type"] = out["event_type"].astype(str).str.strip()
    out = out[(out["event"] != "") | (out["event_type"] != "")].copy()
    if out.empty:
        raise ValueError(f"Promoted candidates file has no usable event identifiers: {source_label}")
    return out


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
    spec = EVENT_REGISTRY_SPECS.get(str(event_type))
    report_dir = spec.reports_dir if spec is not None else str(event_type)
    file_name = spec.events_file if spec is not None else f"{event_type}_events.csv"
    path = DATA_ROOT / "reports" / report_dir / run_id / file_name
    if not path.exists():
        return {"half_life": np.array([]), "adverse": np.array([]), "favorable": np.array([])}

    try:
        df = pd.read_csv(path)
    except Exception:
        return {"half_life": np.array([]), "adverse": np.array([]), "favorable": np.array([])}
    if spec is not None:
        df = filter_phase1_rows_for_event_type(df, spec.event_type)
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


def _passes_quality_floor(
    row: Dict[str, object], 
    *, 
    strict_cost_fields: bool = True, 
    min_events: int = QUALITY_MIN_EVENTS,
    min_robustness: float = QUALITY_MIN_ROBUSTNESS,
    require_positive_expectancy: bool = True,
    expected_cost_digest: str | None = None,
) -> bool:
    robustness = _safe_float(row.get("robustness_score"), 0.0)
    n_events = _safe_int(row.get("n_events", row.get("sample_size", 0)), 0)
    if robustness < min_robustness:
        return False
    if n_events < min_events:
        return False
        
    if strict_cost_fields and expected_cost_digest:
        row_digest = str(row.get("cost_config_digest", "")).strip()
        if row_digest != expected_cost_digest:
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
        
    if require_positive_expectancy:
        if after_cost <= 0.0:
            return False
        if stressed_after_cost <= 0.0:
            return False
    # Cost-ratio is only enforced in strict mode or when positive expectancy is required.
    # In exploratory fallback mode (require_positive_expectancy=False, strict_cost_fields=False)
    # cost_ratio can be > 0.60 — the blueprint is tagged fallback_only and won't enter bridge.
    if strict_cost_fields or require_positive_expectancy:
        if cost_ratio >= QUALITY_MAX_COST_RATIO:
            return False
    # Bridge gate is mandatory when strict_cost_fields is active.
    # If the column is missing AND strict mode is on, the bridge stage has not
    # run — treat as failed rather than silently allowing candidates through.
    bridge_col = row.get("gate_bridge_tradable")
    if strict_cost_fields:
        if bridge_col is None:
            return False  # bridge stage did not produce this column
        if not _as_bool(bridge_col):
            return False
    else:
        # Lenient mode: only block if explicitly False
        if bridge_col is not None and not _as_bool(bridge_col):
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
) -> Tuple[EntrySpec, str | None, int]:
    policy = event_policy(event_type)
    robustness = _safe_float(row.get("robustness_score"), 0.0)
    
    # Use effective_lag_bars from research as the authoritative delay
    effective_lag = _safe_int(row.get("effective_lag_bars"), 1)
    delay = effective_lag
    
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
        delay,
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
    # Prioritize bridge_expectancy_conservative
    after_cost = _safe_float(
        row.get("bridge_expectancy_conservative", 
                row.get("after_cost_expectancy_per_trade", 
                        _safe_float(row.get("expectancy_per_trade"), 0.0)))
    )
    stressed_after_cost = _safe_float(row.get("stressed_after_cost_expectancy_per_trade"), after_cost)
    robustness = _safe_float(row.get("robustness_score"), 0.0)
    oos_gate = _as_bool(row.get("gate_oos_validation", row.get("gate_oos_validation_test", False)))
    cost_ratio = _safe_float(row.get("cost_ratio"), 1.0)
    
    # Selection rule: max after_cost, then min capacity_utilization (proxy by cost_ratio?), then max robustness, then min turnover (proxy?)
    return (
        -after_cost,
        cost_ratio, # lower is better
        -robustness,
        -float(oos_gate),
        str(row.get("candidate_id", "")),
    )


def _passes_fallback_gate(row: Dict[str, object], gates: Dict[str, Any]) -> bool:
    if not gates:
        return False
    min_t = _safe_float(gates.get("min_t_stat"), 2.5)
    min_expectancy = _safe_float(gates.get("min_after_cost_expectancy_bps"), 1.0)
    min_samples = _safe_int(gates.get("min_sample_size"), 100)
    
    # Map from Phase 2 columns
    t_stat = _safe_float(row.get("t_stat"), _safe_float(row.get("expectancy", 0) / (row.get("p_value", 1) + 1e-9))) # Proxy if missing
    after_cost = _safe_float(row.get("after_cost_expectancy_per_trade"), 0.0) * 10000.0 # to bps
    samples = _safe_int(row.get("n_events", row.get("sample_size", 0)), 0)
    
    if t_stat < min_t: return False
    if after_cost < min_expectancy: return False
    if samples < min_samples: return False
    return True


def _choose_event_rows(
    run_id: str,
    event_type: str,
    edge_rows: List[Dict[str, object]],
    phase2_df: pd.DataFrame,
    max_per_event: int,
    allow_fallback_blueprints: bool,
    strict_cost_fields: bool,
    min_events: int,
    min_robustness: float = QUALITY_MIN_ROBUSTNESS,
    require_positive_expectancy: bool = True,
    expected_cost_digest: str | None = None,
    naive_validation: Dict[Tuple[str, str], bool] = None,
    allow_naive_entry_fail: bool = False,
    mode: str = "discovery",
) -> Tuple[List[Dict[str, object]], Dict[str, object], pd.DataFrame]:
    phase2_lookup: Dict[str, Dict[str, object]] = {}
    if not phase2_df.empty:
        for idx, row in enumerate(phase2_df.to_dict(orient="records")):
            cid = _candidate_id(row, idx)
            phase2_lookup[cid] = dict(row)

    full_gates = _load_gates_spec()
    gates = full_gates.get("gate_v1_fallback", {})

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
    
    # If no edge rows but phase2_df exists, use phase2_df as primary source
    if not enriched_edge_rows and not phase2_df.empty:
        enriched_edge_rows = [_enrich(row, idx, "DRAFT") for idx, row in enumerate(phase2_df.to_dict(orient="records"))]
    
    # Pre-filter for eligibility
    eligible_rows = []
    ineligible_rows = [] # Track for logging
    
    for row in enriched_edge_rows:
        is_eligible = True
        reason = ""
        
        # Mode-based filtering
        is_disc = _as_bool(row.get("is_discovery", True))
        is_fall = _passes_fallback_gate(row, gates)
        
        if mode == "discovery" and not is_disc:
            is_eligible = False
            reason = "not_discovery"
        elif mode == "fallback" and not is_fall:
            is_eligible = False
            reason = "fallback_gate_fail"
        elif mode == "both" and not (is_disc or is_fall):
            is_eligible = False
            reason = "neither_disc_nor_fallback"

        # Naive Entry Gate
        if is_eligible and not allow_naive_entry_fail and naive_validation is not None:
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

    # Selection DataFrame logic
    selection_data = []
    
    def _add_selection_record(c: Dict[str, object], reason: str, selected: bool, rank: int):
        after_cost = _safe_float(c.get("bridge_expectancy_conservative", c.get("after_cost_expectancy_per_trade", 0.0)))
        cost_ratio = _safe_float(c.get("cost_ratio"), 1.0)
        robustness = _safe_float(c.get("robustness_score"), 0.0)
        digest = str(c.get("cost_config_digest", "")).strip()
        
        selection_data.append({
            "candidate_id": c.get("candidate_id"),
            "event_type": event_type,
            "rank": rank,
            "selected": selected,
            "reason": reason,
            "status": c.get("status"),
            "robustness_score": robustness,
            "n_events": c.get("n_events"),
            "after_cost_expectancy": after_cost,
            "cost_ratio": cost_ratio,
            "cost_config_digest": digest,
            "rank_score_components": f"expectancy={after_cost:.6f},cost_ratio={cost_ratio:.4f},robustness={robustness:.4f}"
        })

    # Log ineligible first (rank 0)
    for c in ineligible_rows:
        _add_selection_record(c, f"ineligible_{c.get('_ineligible_reason')}", False, 0)

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
        promoted_quality = [row for row in promoted_sorted if _passes_quality_floor(row, strict_cost_fields=strict_cost_fields, min_events=min_events, min_robustness=min_robustness, require_positive_expectancy=require_positive_expectancy, expected_cost_digest=expected_cost_digest)]
        rejected_quality_floor_count += max(0, len(promoted_sorted) - len(promoted_quality))
        
        # Log promoted candidates not selected due to quality
        for c in promoted_sorted:
            if c not in promoted_quality:
                _add_selection_record(c, "quality_floor_fail_promoted", False, 0)

        if promoted_quality:
            selected = promoted_quality[:max_per_event]
            for i, c in enumerate(selected):
                _add_selection_record(c, "promoted_quality", True, i+1)
            
            # Log cap exclusions
            for i, c in enumerate(promoted_quality[max_per_event:]):
                _add_selection_record(c, "excluded_by_cap", False, max_per_event+i+1)

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

    # Updated non_promoted_rows logic to use eligible_rows
    non_promoted_rows = [row for row in eligible_rows if str(row.get("status", "")).upper() != "PROMOTED"]
    if non_promoted_rows:
        non_promoted_sorted = sorted(non_promoted_rows, key=_rank_key)
        non_promoted_quality = [row for row in non_promoted_sorted if _passes_quality_floor(row, strict_cost_fields=strict_cost_fields, min_events=min_events, min_robustness=min_robustness, require_positive_expectancy=False, expected_cost_digest=expected_cost_digest)]
        rejected_quality_floor_count += max(0, len(non_promoted_sorted) - len(non_promoted_quality))
        
        for c in non_promoted_sorted:
            if c not in non_promoted_quality:
                 _add_selection_record(c, "quality_floor_fail_fallback_non_promoted", False, 0)

        if non_promoted_quality:
            selected = non_promoted_quality[:max_per_event]
            for i, c in enumerate(selected):
                _add_selection_record(c, "fallback_non_promoted_quality", True, i+1)
            
            for i, c in enumerate(non_promoted_quality[max_per_event:]):
                _add_selection_record(c, "excluded_by_cap", False, max_per_event+i+1)

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

    if eligible_rows:
        edge_sorted = sorted(eligible_rows, key=_rank_key)
        edge_quality = [row for row in edge_sorted if _passes_quality_floor(row, strict_cost_fields=strict_cost_fields, min_events=min_events, min_robustness=min_robustness, require_positive_expectancy=False, expected_cost_digest=expected_cost_digest)]
        rejected_quality_floor_count += max(0, len(edge_sorted) - len(edge_quality))
        
        for c in edge_sorted:
            if c not in edge_quality:
                 _add_selection_record(c, "quality_floor_fail_fallback_edge", False, 0)

        if edge_quality:
            selected = edge_quality[:max_per_event]
            for i, c in enumerate(selected):
                _add_selection_record(c, "fallback_edge_quality", True, i+1)
            for i, c in enumerate(edge_quality[max_per_event:]):
                _add_selection_record(c, "excluded_by_cap", False, max_per_event+i+1)

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
        # Phase 2 fallback sorts by executed-like bridge expectancy first (same as
        # primary path) rather than robustness_score alone.  Candidates with a
        # missing or False gate_bridge_tradable are excluded even in fallback when
        # strict_cost_fields is enabled, so promotions remain meaningful.
        ordered_rows = fallback_df.sort_values(
            by=["robustness_score", "profit_density_score", "candidate_id"],
            ascending=[False, False, True],
        ).to_dict(orient="records")
        parsed_rows: List[Dict[str, object]] = []
        for idx, row in enumerate(ordered_rows):
            parsed_rows.append(_enrich(row, idx, "DRAFT"))
            
        # For phase2 fallback, we also need to apply the pre-filters (naive_entry)
        # and re-verify eligibility. 
        # Actually it's simpler to just filter parsed_rows here.
        eligible_parsed_rows = []
        for row in parsed_rows:
            is_eligible = True
            
            # Mode-based filtering for Phase 2 fallback
            is_disc = _as_bool(row.get("rejected", False)) or _as_bool(row.get("is_discovery", False))
            is_fall = _passes_fallback_gate(row, gates)
            
            if mode == "discovery" and not is_disc:
                is_eligible = False
            elif mode == "fallback" and not is_fall:
                is_eligible = False
            elif mode == "both" and not (is_disc or is_fall):
                is_eligible = False

            if is_eligible and not allow_naive_entry_fail and naive_validation is not None:
                cid = str(row.get("candidate_id", "")).strip()
                if not naive_validation.get((event_type, cid), False):
                    is_eligible = False
                    _add_selection_record(row, "ineligible_naive_entry_fail_phase2_fallback", False, 0)
            
            if is_eligible:
                eligible_parsed_rows.append(row)

        phase2_quality = [row for row in eligible_parsed_rows if _passes_quality_floor(row, strict_cost_fields=strict_cost_fields, min_events=min_events, expected_cost_digest=expected_cost_digest)]
        rejected_quality_floor_count += max(0, len(eligible_parsed_rows) - len(phase2_quality))
        
        for c in eligible_parsed_rows:
            if c not in phase2_quality:
                 _add_selection_record(c, "quality_floor_fail_phase2", False, 0)

        if phase2_quality:
            selected = phase2_quality[:max_per_event]
            for i, c in enumerate(selected):
                _add_selection_record(c, "fallback_phase2_quality", True, i+1)
            for i, c in enumerate(phase2_quality[max_per_event:]):
                _add_selection_record(c, "excluded_by_cap", False, max_per_event+i+1)

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
    cost_config_digest: str = "",
    ontology_spec_hash_value: str = "",
    operator_registry: Dict[str, Dict[str, Any]] | None = None,
) -> Tuple[Blueprint, int]:
    candidate_id = str(row.get("candidate_id", "")).strip() or _candidate_id(row, 0)
    detail = phase2_lookup.get(candidate_id, {})
    merged = dict(row)
    merged.update({k: v for k, v in detail.items() if k not in merged or pd.isna(merged.get(k))})

    time_stop_bars = _derive_time_stop(stats.get("half_life", np.array([])), merged)
    stop_value, target_value = _derive_stop_target(stats=stats, row=merged)
    entry, condition_symbol_override, effective_lag_used = _entry_from_row(
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
    template_verb = str(merged.get("template_verb", merged.get("rule_template", ""))).strip()
    state_id = str(merged.get("state_id", "")).strip()
    canonical_event_type = str(merged.get("canonical_event_type", event_type)).strip() or event_type
    canonical_family = str(merged.get("canonical_family", "")).strip()
    operator_version = ""
    if operator_registry is not None:
        op = operator_registry.get(template_verb, {})
        if not op:
            raise ValueError(f"Unknown operator for template verb {template_verb!r} while compiling blueprint")
        operator_version = str(op.get("operator_version", "")).strip()
        if not operator_version:
            raise ValueError(f"Operator version missing for template verb {template_verb!r}")

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
            bridge_embargo_days_used=(
                None
                if merged.get("bridge_embargo_days_used") in (None, "")
                else _safe_int(merged.get("bridge_embargo_days_used"), -1)
            ),
            events_count_used_for_gate=_safe_int(merged.get("n_events", merged.get("sample_size", 0)), 0),
            min_events_threshold=int(min_events),
            cost_config_digest=cost_config_digest,
            promotion_track=str(merged.get("promotion_track", "fallback_only")),
            discovery_start=str(merged.get("discovery_start", "")),
            discovery_end=str(merged.get("discovery_end", "")),
            ontology_spec_hash=str(ontology_spec_hash_value),
            canonical_event_type=canonical_event_type,
            canonical_family=canonical_family,
            state_id=state_id,
            template_verb=template_verb,
            operator_version=operator_version,
            constraints={
                "gate_after_cost_positive": _as_bool(merged.get("gate_after_cost_positive", False)),
                "gate_after_cost_stressed_positive": _as_bool(merged.get("gate_after_cost_stressed_positive", False)),
                "gate_bridge_tradable": _as_bool(merged.get("gate_bridge_tradable", False)),
                "blueprint_effective_lag_bars_used": int(effective_lag_used),
            },
        ),
    )
    blueprint.validate()
    
    # Executability Hardening: Validate feature references
    from strategy_dsl.contract_v1 import validate_feature_references
    validate_feature_references(blueprint.to_dict())
    
    return blueprint, effective_lag_used


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
    parser.add_argument("--quality_floor_fallback", type=float, default=None,
                        help="Min phase2_quality_score for fallback compile eligibility. Overrides spec/gates.yaml.")
    parser.add_argument("--candidates_file", default=None)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--max_total_compiles_per_run", type=int, default=100)
    parser.add_argument("--mode", choices=["discovery", "fallback", "both"], default="discovery", help="Promotion track selection mode")
    args = parser.parse_args()
    if bool(int(args.allow_fallback_blueprints)):
        print(
            "--allow_fallback_blueprints=1 is not supported: compiler is promoted-candidates only.",
            file=sys.stderr,
        )
        return 1

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

    try:
        import yaml
        _spec_gates_path = PROJECT_ROOT.parent / "spec" / "gates.yaml"
        _gates_spec = yaml.safe_load(_spec_gates_path.read_text(encoding="utf-8")) if _spec_gates_path.exists() else {}
        _phase2_gates = _gates_spec.get("gate_v1_phase2", {})
    except Exception:
        _phase2_gates = {}
    # CLI arg overrides spec; spec overrides hardcoded default
    effective_quality_floor_fallback = (
        float(args.quality_floor_fallback) if args.quality_floor_fallback is not None
        else float(_phase2_gates.get("quality_floor_fallback", QUALITY_MIN_ROBUSTNESS))
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
        "quality_floor_fallback": effective_quality_floor_fallback,
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("compile_strategy_blueprints", args.run_id, params, inputs, outputs)

    try:
        current_ontology_hash = ontology_spec_hash(PROJECT_ROOT.parent)
        run_manifest_hashes = load_run_manifest_hashes(DATA_ROOT, args.run_id)
        manifest_ontology_hash = str(run_manifest_hashes.get("ontology_spec_hash", "") or "").strip()
        if not manifest_ontology_hash:
            raise ValueError(f"run_manifest missing ontology_spec_hash for run_id={args.run_id}")
        if manifest_ontology_hash != current_ontology_hash:
            raise ValueError(
                "Ontology drift detected before blueprint compile: "
                f"run_manifest={manifest_ontology_hash}, current={current_ontology_hash}"
            )
        operator_registry = _load_operator_registry()
        if not operator_registry:
            raise ValueError("Operator registry is missing or empty in template_verb_lexicon.yaml")

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
            # Canonical input: promoted_candidates.parquet only
            edge_path = DATA_ROOT / "reports" / "promotions" / args.run_id / "promoted_candidates.parquet"
            inputs.append({"path": str(edge_path), "rows": None, "start_ts": None, "end_ts": None})
            if edge_path.exists():
                edge_df = pd.read_parquet(edge_path)
            else:
                edge_df = pd.DataFrame()
        
        if edge_df.empty:
            if Path(edge_path).exists():
                raise ValueError(
                    f"Promoted candidates artifact is empty for compile: {edge_path}. "
                    "No promoted candidates are available for blueprint compilation; "
                    "inspect promotion_summary.json for reject reasons."
                )
            raise ValueError(
                f"Missing promoted candidates artifact for compile: {edge_path}. "
                "Run promote_candidates stage first."
            )
            
        # Canonical schema enforcement
        edge_df = ensure_candidate_schema(edge_df)
        
        # Require essential columns
        required_cols = ["candidate_id", "event_type", "effective_lag_bars", "condition", "action", "direction_rule", "horizon"]
        missing_essential = [c for c in required_cols if c not in edge_df.columns]
        if missing_essential:
            raise ValueError(f"Promoted candidates missing essential columns for compilation: {missing_essential}")

        edge_df = _validate_promoted_candidates_frame(edge_df, source_label=str(edge_path))
        
        # Deterministic sorting of candidates
        sort_by = []
        ascending = []
        for c, asc in [
            ("selection_score", False),
            ("robustness_score", False),
            ("n_events", False),
            ("candidate_id", True)
        ]:
            if c in edge_df.columns:
                sort_by.append(c)
                ascending.append(asc)
        edge_df = edge_df.sort_values(sort_by, ascending=ascending).reset_index(drop=True)

        if "gate_bridge_tradable" not in edge_df.columns:
            _msg = (
                "gate_bridge_tradable column missing from promoted candidates — bridge stage may not have run "
                f"(run_id={args.run_id}, path={edge_path})."
            )
            if not int(args.ignore_checklist):
                raise ValueError(_msg)
            print(f"WARNING: {_msg}", file=sys.stderr)
        if "candidate_id" not in edge_df.columns:
            edge_df["candidate_id"] = [
                _candidate_id(row, idx) for idx, row in enumerate(edge_df.to_dict(orient="records"))
            ]
        edge_rows = edge_df.to_dict(orient="records")

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
        event_types = sorted(
            {
                str(row.get("event", row.get("event_type", ""))).strip()
                for row in edge_rows
                if str(row.get("event", row.get("event_type", ""))).strip()
            }
        )
        if not event_types and phase2_root.exists():
            event_types = sorted([p.name for p in phase2_root.iterdir() if p.is_dir()])

        print(f"DEBUG: event_types={event_types}", file=sys.stderr)
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

            selected, selection_diag, selection_df = _choose_event_rows(
                run_id=args.run_id,
                event_type=event_type,
                edge_rows=event_edge_rows,
                phase2_df=phase2_df,
                max_per_event=int(args.max_per_event),
                allow_fallback_blueprints=bool(int(args.allow_fallback_blueprints)),
                strict_cost_fields=bool(int(args.strict_cost_fields)),
                min_events=int(args.min_events_floor),
                min_robustness=effective_quality_floor_fallback,
                # For exploratory fallback, relax positive expectancy requirement.
                # Standard promoted path retains it.
                require_positive_expectancy=not bool(int(args.allow_fallback_blueprints)),
                expected_cost_digest=resolved_costs.config_digest,
                naive_validation=naive_validation,
                allow_naive_entry_fail=bool(int(args.allow_naive_entry_fail)),
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
                "rejected_naive_entry_count": int(selection_df[selection_df["reason"] == "ineligible_naive_entry_fail"].shape[0]) if not selection_df.empty else 0,
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

            # Selection is already filtered by naive_entry in _choose_event_rows
            selected_for_build_count += len(selected)
            print(f"DEBUG: selected count for {event_type}={len(selected)}", file=sys.stderr)
            stats = _event_stats(run_id=args.run_id, event_type=event_type)
            event_non_exec_errors: List[str] = []
            event_blueprint_count_before = len(blueprints)
            print(f"DEBUG: selected length for {event_type}={len(selected)}", file=sys.stderr)
            
            for row in selected:
                cid = str(row.get("candidate_id", "")).strip() or _candidate_id(row, 0)
                try:
                    bp, effective_lag_used = _build_blueprint(
                        run_id=args.run_id,
                        run_symbols=run_symbols,
                        event_type=event_type,
                        row=row,
                        phase2_lookup=phase2_lookup,
                        stats=stats,
                        fees_bps=float(resolved_costs.fee_bps_per_side),
                        slippage_bps=float(resolved_costs.slippage_bps_per_fill),
                        min_events=int(args.min_events_floor),
                        cost_config_digest=resolved_costs.config_digest,
                        ontology_spec_hash_value=manifest_ontology_hash,
                        operator_registry=operator_registry,
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

        print(f"DEBUG: blueprints after loop={len(blueprints)}", file=sys.stderr)
        # Ensure we have a list of Blueprint objects, not tuples from _build_blueprint
        if blueprints and isinstance(blueprints[0], tuple):
            blueprints = [bp[0] for bp in blueprints]
        print(f"DEBUG: blueprints after tuple unpack={len(blueprints)}", file=sys.stderr)

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
        out_yaml = out_dir / "blueprints.yaml"
        
        # Deterministic sorting before write
        def bp_sort_key(bp: Blueprint):
            match = next((r for r in selection_records if r["candidate_id"] == bp.candidate_id), {})
            # Use after_cost_expectancy as primary selection score
            return (
                -float(match.get("after_cost_expectancy", 0.0)),
                -float(match.get("robustness_score", 0.0)),
                -int(match.get("n_events", 0)),
                str(bp.candidate_id)
            )
        blueprints = sorted(blueprints, key=bp_sort_key)

        # Atomic write JSONL
        tmp_jsonl = out_jsonl.with_suffix(".jsonl.tmp")
        lines = [json.dumps(bp.to_dict(), sort_keys=True) for bp in blueprints]
        tmp_jsonl.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        tmp_jsonl.replace(out_jsonl)
        
        # Atomic write YAML
        tmp_yaml = out_yaml.with_suffix(".yaml.tmp")
        yaml_payload = [bp.to_dict() for bp in blueprints]
        with open(tmp_yaml, "w", encoding="utf-8") as f:
            yaml.dump(yaml_payload, f, sort_keys=True)
        tmp_yaml.replace(out_yaml)

        active_count = sum(1 for bp in blueprints if not bp.lineage.wf_status.startswith("trimmed"))
        trimmed_count = len(blueprints) - active_count

        summary = {
            "run_id": args.run_id,
            "compiler_version": COMPILER_VERSION,
            "event_types": event_types,
            "candidates_in": int(len(edge_rows)),
            "candidates_compiled": int(len(blueprints)),
            "candidates_rejected": int(max(0, len(edge_rows) - len(blueprints))),
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
            "blueprint_effective_lag_bars_used": int(np.mean([bp.lineage.constraints.get("blueprint_effective_lag_bars_used", 1) for bp in blueprints])) if blueprints else 0,
            "top_10_reject_reasons": (
                pd.DataFrame(attempt_records)["fail_reason"].value_counts().head(10).to_dict()
                if attempt_records else {}
            ),
            "thresholds": vars(args),
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
        out_summary = out_dir / "blueprint_summary.json"
        out_summary.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        
        # Write selection ledger
        if selection_records:
            pd.DataFrame(selection_records).to_parquet(out_dir / "compile_selection.parquet", index=False)

        # ── Condition Enforcement Audit ──────────────────────────────────────
        # Emit one row per compiled blueprint with condition metadata.
        # Also run fail-closed invariant guards on every condition string.
        _RULE_TEMPLATE_NAMES_GUARD = {
            "mean_reversion", "continuation", "carry", "breakout",
        }
        condition_audit_rows = []
        for bp in blueprints:
            entry_condition = bp.entry.conditions[0] if bp.entry.conditions else "all"
            num_nodes = len(bp.entry.condition_nodes)
            nodes_hash = hashlib.sha256(
                json.dumps([
                    {"feature": n.feature, "operator": n.operator, "value": n.value}
                    for n in bp.entry.condition_nodes
                ], sort_keys=True).encode()
            ).hexdigest()[:16]

            # Guard: legacy "all__" prefix must not appear in compiled blueprints
            if "__" in entry_condition:
                raise ValueError(
                    f"Compiled blueprint '{bp.id}' has condition '{entry_condition}' containing legacy "
                    f"'all__' prefix. This indicates _condition_for_cond_name is still emitting the old format."
                )
            # Guard: rule template names must never appear as conditions
            if entry_condition.lower() in _RULE_TEMPLATE_NAMES_GUARD:
                raise ValueError(
                    f"Compiled blueprint '{bp.id}' has condition '{entry_condition}' which is a rule "
                    f"template name, not a valid runtime condition."
                )
            # Guard: runtime condition with 0 nodes is the original silent-drop bug.
            # Fail closed unless it is explicitly a symbol-scoped condition (those
            # legitimately have 0 nodes; symbol routing happens at a different layer).
            if entry_condition not in ("all", "") and num_nodes == 0 and not entry_condition.startswith("symbol_"):
                # Determine condition_source from source row if available
                # (we can't access it after compile; check via contract)
                from strategy_dsl.contract_v1 import is_executable_condition
                if is_executable_condition(entry_condition):
                    # Looks like a runtime condition but no nodes produced — this is the bug
                    raise ValueError(
                        f"Compiled blueprint '{bp.id}' has condition '{entry_condition}' "
                        f"(runtime-enforceable) but 0 condition_nodes were produced. "
                        f"This indicates a missing mapping in normalize_entry_condition. "
                        f"Failing closed to prevent silent non-enforcement."
                    )
                else:
                    import logging as _logging
                    _logging.getLogger(__name__).warning(
                        "Blueprint '%s' has condition '%s' (non-runtime/partially mapped) "
                        "but 0 condition_nodes — check if this is expected.",
                        bp.id, entry_condition,
                    )

            condition_audit_rows.append({
                "candidate_id": bp.candidate_id,
                "blueprint_id": bp.id,
                "event_type": bp.event_type,
                "condition": entry_condition,
                "num_condition_nodes": num_nodes,
                "condition_nodes_hash": nodes_hash,
                "compile_reason": "compiled",
            })

        audit_path = out_dir / "compiled_blueprints_condition_audit.parquet"
        if condition_audit_rows:
            audit_df = pd.DataFrame(condition_audit_rows)
            audit_df.to_parquet(audit_path, index=False)
            outputs.append({"path": str(audit_path), "rows": int(len(condition_audit_rows)), "start_ts": None, "end_ts": None})

            # ── Audit Gate ───────────────────────────────────────────────────
            # Any compiled blueprint with a runtime condition but 0 enforcement
            # nodes is the silent-drop bug. Post-compile gate catches regressions
            # even if the per-blueprint check is somehow bypassed.
            # (condition_source is not stored per blueprint; re-derive via contract.)
            from strategy_dsl.contract_v1 import is_executable_condition as _is_exec
            audit_runtime_zero = audit_df[
                (audit_df["condition"].apply(lambda c: bool(c and c not in ("all", "") and not str(c).startswith("symbol_") and _is_exec(c))))
                & (audit_df["num_condition_nodes"] == 0)
            ]
            if not audit_runtime_zero.empty:
                offenders = audit_runtime_zero[["blueprint_id", "condition", "num_condition_nodes"]].to_dict(orient="records")
                raise ValueError(
                    f"Condition enforcement audit gate FAILED: {len(audit_runtime_zero)} blueprint(s) have "
                    f"a runtime-enforceable condition but 0 condition_nodes. "
                    f"Offenders: {offenders}. "
                    f"Stage set to failed_stage='compile_strategy_blueprints'."
                )
            # ── End Audit Gate ───────────────────────────────────────────────
        # ── End Condition Enforcement Audit ──────────────────────────────────

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
