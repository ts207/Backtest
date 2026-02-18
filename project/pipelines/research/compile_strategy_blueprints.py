from __future__ import annotations

import argparse
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

SESSION_CONDITION_MAP: Dict[str, Tuple[float, float]] = {
    "session_asia": (0.0, 7.0),
    "session_eu": (8.0, 15.0),
    "session_us": (16.0, 23.0),
}

BULL_BEAR_CONDITION_MAP: Dict[str, float] = {
    "bull_bear_bull": 1.0,
    "bull_bear_bear": -1.0,
}

VOL_REGIME_CONDITION_MAP: Dict[str, float] = {
    "vol_regime_low": 0.0,
    "vol_regime_mid": 1.0,
    "vol_regime_medium": 1.0,
    "vol_regime_high": 2.0,
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


def _passes_quality_floor(row: Dict[str, object], *, strict_cost_fields: bool = True) -> bool:
    robustness = _safe_float(row.get("robustness_score"), 0.0)
    n_events = _safe_int(row.get("n_events", row.get("sample_size", 0)), 0)
    if robustness < QUALITY_MIN_ROBUSTNESS:
        return False
    if n_events < QUALITY_MIN_EVENTS:
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
) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
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
    rejected_quality_floor_count = 0
    diagnostics: Dict[str, object] = {
        "event_type": event_type,
        "selected_count": 0,
        "rejected_quality_floor_count": 0,
        "reason": "no_candidates",
        "used_fallback": False,
    }
    promoted = [row for row in enriched_edge_rows if str(row.get("status", "")).upper() == "PROMOTED"]
    if promoted:
        promoted_sorted = sorted(promoted, key=_rank_key)
        promoted_quality = [row for row in promoted_sorted if _passes_quality_floor(row, strict_cost_fields=strict_cost_fields)]
        rejected_quality_floor_count += max(0, len(promoted_sorted) - len(promoted_quality))
        if promoted_quality:
            selected = promoted_quality[:max_per_event]
            diagnostics.update(
                {
                    "selected_count": int(len(selected)),
                    "rejected_quality_floor_count": int(rejected_quality_floor_count),
                    "reason": "promoted_quality",
                    "used_fallback": False,
                }
            )
            return selected, diagnostics
        if not allow_fallback_blueprints:
            diagnostics.update(
                {
                    "selected_count": 0,
                    "rejected_quality_floor_count": int(rejected_quality_floor_count),
                    "reason": "rejected_all_promoted_quality_floor",
                    "used_fallback": False,
                }
            )
            return [], diagnostics

        non_promoted_rows = [row for row in enriched_edge_rows if str(row.get("status", "")).upper() != "PROMOTED"]
        if non_promoted_rows:
            non_promoted_sorted = sorted(non_promoted_rows, key=_rank_key)
            non_promoted_quality = [row for row in non_promoted_sorted if _passes_quality_floor(row, strict_cost_fields=strict_cost_fields)]
            rejected_quality_floor_count += max(0, len(non_promoted_sorted) - len(non_promoted_quality))
            if non_promoted_quality:
                selected = non_promoted_quality[:max_per_event]
                diagnostics.update(
                    {
                        "selected_count": int(len(selected)),
                        "rejected_quality_floor_count": int(rejected_quality_floor_count),
                        "reason": "fallback_non_promoted_quality",
                        "used_fallback": True,
                    }
                )
                return selected, diagnostics

        diagnostics.update(
            {
                "selected_count": 0,
                "rejected_quality_floor_count": int(rejected_quality_floor_count),
                "reason": "fallback_enabled_but_no_quality_rows",
                "used_fallback": True,
            }
        )
        return [], diagnostics

    if not allow_fallback_blueprints:
        diagnostics.update(
            {
                "selected_count": 0,
                "rejected_quality_floor_count": int(rejected_quality_floor_count),
                "reason": "no_promoted_and_fallback_disabled",
                "used_fallback": False,
            }
        )
        return [], diagnostics

    if enriched_edge_rows:
        edge_sorted = sorted(enriched_edge_rows, key=_rank_key)
        edge_quality = [row for row in edge_sorted if _passes_quality_floor(row, strict_cost_fields=strict_cost_fields)]
        rejected_quality_floor_count += max(0, len(edge_sorted) - len(edge_quality))
        if edge_quality:
            selected = edge_quality[:max_per_event]
            diagnostics.update(
                {
                    "selected_count": int(len(selected)),
                    "rejected_quality_floor_count": int(rejected_quality_floor_count),
                    "reason": "fallback_edge_quality",
                    "used_fallback": True,
                }
            )
            return selected, diagnostics

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
        phase2_quality = [row for row in parsed_rows if _passes_quality_floor(row, strict_cost_fields=strict_cost_fields)]
        rejected_quality_floor_count += max(0, len(parsed_rows) - len(phase2_quality))
        if phase2_quality:
            selected = phase2_quality[:max_per_event]
            diagnostics.update(
                {
                    "selected_count": int(len(selected)),
                    "rejected_quality_floor_count": int(rejected_quality_floor_count),
                    "reason": "fallback_phase2_quality",
                    "used_fallback": True,
                }
            )
            return selected, diagnostics

    diagnostics.update(
        {
            "selected_count": 0,
            "rejected_quality_floor_count": int(rejected_quality_floor_count),
            "reason": "fallback_enabled_but_no_quality_rows",
            "used_fallback": True,
        }
    )
    return [], diagnostics


def _build_blueprint(
    run_id: str,
    run_symbols: List[str],
    event_type: str,
    row: Dict[str, object],
    phase2_lookup: Dict[str, Dict[str, object]],
    stats: Dict[str, np.ndarray],
    fees_bps: float,
    slippage_bps: float,
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
    overlays = [OverlaySpec(name=str(item["name"]), params=dict(item["params"])) for item in overlay_rows]

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
        ),
    )
    blueprint.validate()
    return blueprint


def _load_walkforward_strategy_metrics(run_id: str) -> Dict[str, Dict[str, object]]:
    path = DATA_ROOT / "reports" / "eval" / run_id / "walkforward_summary.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    raw = payload.get("per_strategy_split_metrics", {})
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, Dict[str, object]] = {}
    for key, value in raw.items():
        if isinstance(key, str) and isinstance(value, dict):
            out[key] = value
    return out


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


def _trim_blueprints_with_walkforward_evidence(blueprints: List[Blueprint], run_id: str) -> Tuple[List[Blueprint], Dict[str, object]]:
    metrics_by_strategy = _load_walkforward_strategy_metrics(run_id=run_id)
    if not metrics_by_strategy or not blueprints:
        return blueprints, {
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

    drop_ids: set[str] = set(zero_trade_ids)
    negative_rows = sorted(negative_rows, key=lambda row: (row[0], row[1]))
    drop_ids.update(bp_id for _, bp_id in negative_rows[:TRIM_WF_WORST_K])
    if not drop_ids:
        return blueprints, {
            "wf_evidence_used": True,
            "trim_split": "validation",
            "trimmed_zero_trade": 0,
            "trimmed_worst_negative": 0,
            "wf_trimmed_all": False,
            "dropped_blueprint_ids": [],
        }

    trimmed = [bp for bp in blueprints if bp.id not in drop_ids]
    return trimmed, {
        "wf_evidence_used": True,
        "trim_split": "validation",
        "trimmed_zero_trade": int(sum(1 for bp_id in drop_ids if bp_id in zero_trade_ids)),
        "trimmed_worst_negative": int(sum(1 for bp_id in drop_ids if bp_id not in zero_trade_ids)),
        "wf_trimmed_all": bool(len(trimmed) == 0 and len(drop_ids) > 0),
        "dropped_blueprint_ids": sorted(drop_ids),
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
    parser.add_argument("--out_dir", default=None)
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

        edge_path = DATA_ROOT / "reports" / "edge_candidates" / args.run_id / "edge_candidates_normalized.csv"
        inputs.append({"path": str(edge_path), "rows": None, "start_ts": None, "end_ts": None})
        if edge_path.exists():
            edge_df = pd.read_csv(edge_path)
        else:
            edge_df = pd.DataFrame()

        if not edge_df.empty:
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
        for event_type in event_types:
            event_edge_rows = [row for row in edge_rows if str(row.get("event", "")).strip() == event_type]
            phase2_df = _load_phase2_table(run_id=args.run_id, event_type=event_type)
            phase2_lookup = {}
            if not phase2_df.empty:
                for idx, row in enumerate(phase2_df.to_dict(orient="records")):
                    cid = _candidate_id(row, idx)
                    phase2_lookup[cid] = dict(row)

            selected, selection_diag = _choose_event_rows(
                run_id=args.run_id,
                event_type=event_type,
                edge_rows=event_edge_rows,
                phase2_df=phase2_df,
                max_per_event=int(args.max_per_event),
                allow_fallback_blueprints=bool(int(args.allow_fallback_blueprints)),
                strict_cost_fields=bool(int(args.strict_cost_fields)),
            )
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

            if not int(args.allow_naive_entry_fail):
                strict_selected: List[Dict[str, object]] = []
                for row in selected:
                    cid = str(row.get("candidate_id", "")).strip() or _candidate_id(row, 0)
                    passed = naive_validation.get((event_type, cid))
                    if passed is True:
                        strict_selected.append(row)
                    else:
                        rejected_naive_entry_count += 1
                        per_event_rejections[event_type]["rejected_naive_entry_count"] = int(
                            per_event_rejections[event_type]["rejected_naive_entry_count"]
                        ) + 1
                selected = strict_selected

            stats = _event_stats(run_id=args.run_id, event_type=event_type)
            event_non_exec_errors: List[str] = []
            event_blueprint_count_before = len(blueprints)
            for row in selected:
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
                    )
                except NonExecutableConditionError as exc:
                    rejected_non_executable_condition_count += 1
                    per_event_rejections[event_type]["rejected_non_executable_condition_count"] = int(
                        per_event_rejections[event_type]["rejected_non_executable_condition_count"]
                    ) + 1
                    if int(args.allow_non_executable_conditions):
                        continue
                    event_non_exec_errors.append(str(exc))
                    continue
                blueprints.append(bp)
            if event_non_exec_errors and (len(blueprints) == event_blueprint_count_before):
                raise ValueError(
                    f"All selected candidates for event={event_type} were non-executable: {event_non_exec_errors[0]}"
                )
            if (len(selected) == 0) and (not int(args.allow_naive_entry_fail)):
                raise ValueError(f"All selected candidates for event={event_type} failed naive-entry validation.")

        blueprints = sorted(blueprints, key=lambda b: (b.event_type, b.candidate_id, b.id))
        if not blueprints:
            raise ValueError(
                "No blueprints were produced from promoted evidence. "
                "Use --allow_fallback_blueprints 1 only for explicit non-production fallback."
            )
        blueprints, trim_stats = _trim_blueprints_with_walkforward_evidence(blueprints=blueprints, run_id=args.run_id)
        blueprints = sorted(blueprints, key=lambda b: (b.event_type, b.candidate_id, b.id))
        if not blueprints:
            if bool(trim_stats.get("wf_trimmed_all", False)):
                raise ValueError("Walkforward trimming removed all blueprints; strict mode fails closed.")
            raise ValueError("No blueprints remained after compile filters.")

        out_jsonl = out_dir / "blueprints.jsonl"
        lines = [json.dumps(bp.to_dict(), sort_keys=True) for bp in blueprints]
        out_jsonl.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

        summary = {
            "run_id": args.run_id,
            "compiler_version": COMPILER_VERSION,
            "event_types": event_types,
            "blueprint_count": int(len(blueprints)),
            "fallback_event_count": int(fallback_count),
            "quality_floor": {
                "min_robustness_score": QUALITY_MIN_ROBUSTNESS,
                "min_events": QUALITY_MIN_EVENTS,
                "after_cost_expectancy_positive_required": True,
                "stressed_after_cost_expectancy_positive_required": True,
                "max_cost_ratio": QUALITY_MAX_COST_RATIO,
                "strict_cost_fields": bool(int(args.strict_cost_fields)),
            },
            "historical_trim": trim_stats,
            "rejected_non_executable_condition_count": int(rejected_non_executable_condition_count),
            "rejected_naive_entry_count": int(rejected_naive_entry_count),
            "rejected_quality_floor_count": int(rejected_quality_floor_count),
            "wf_trimmed_all": bool(trim_stats.get("wf_trimmed_all", False)),
            "per_event_rejections": per_event_rejections,
            "per_event_counts": {
                event_type: int(sum(1 for bp in blueprints if bp.event_type == event_type)) for event_type in event_types
            },
        }
        out_summary = out_dir / "blueprints_summary.json"
        out_summary.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
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
