from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Sequence

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.run_manifest import finalize_manifest, start_manifest

PHASE2_EVENT_CHAIN = [
    ('VOL_SHOCK', 'analyze_vol_shock_relaxation.py', ['--timeframe', '5m']),
    ('LIQUIDITY_VACUUM', 'analyze_liquidity_vacuum.py', ['--timeframe', '5m']),
    ('FORCED_FLOW_EXHAUSTION', 'analyze_directional_exhaustion_after_forced_flow.py', []),
    ('CROSS_VENUE_DESYNC', 'analyze_cross_venue_desync.py', []),
    ('FUNDING_EXTREME_ONSET', 'analyze_funding_episode_events.py', []),
    ('FUNDING_PERSISTENCE_TRIGGER', 'analyze_funding_episode_events.py', []),
    ('FUNDING_NORMALIZATION_TRIGGER', 'analyze_funding_episode_events.py', []),
    ('OI_SPIKE_POSITIVE', 'analyze_oi_shock_events.py', []),
    ('OI_SPIKE_NEGATIVE', 'analyze_oi_shock_events.py', []),
    ('OI_FLUSH', 'analyze_oi_shock_events.py', []),
    ('LIQUIDATION_CASCADE', 'analyze_liquidation_cascade.py', []),
    ('DEPTH_COLLAPSE', 'analyze_liquidity_dislocation_events.py', ['--event_type', 'DEPTH_COLLAPSE', '--timeframe', '5m']),
    ('SPREAD_BLOWOUT', 'analyze_liquidity_dislocation_events.py', ['--event_type', 'SPREAD_BLOWOUT', '--timeframe', '5m']),
    ('ORDERFLOW_IMBALANCE_SHOCK', 'analyze_liquidity_dislocation_events.py', ['--event_type', 'ORDERFLOW_IMBALANCE_SHOCK', '--timeframe', '5m']),
    ('SWEEP_STOPRUN', 'analyze_liquidity_dislocation_events.py', ['--event_type', 'SWEEP_STOPRUN', '--timeframe', '5m']),
    ('ABSORPTION_EVENT', 'analyze_liquidity_dislocation_events.py', ['--event_type', 'ABSORPTION_EVENT', '--timeframe', '5m']),
    ('LIQUIDITY_GAP_PRINT', 'analyze_liquidity_dislocation_events.py', ['--event_type', 'LIQUIDITY_GAP_PRINT', '--timeframe', '5m']),
    ('VOL_SPIKE', 'analyze_volatility_transition_events.py', ['--event_type', 'VOL_SPIKE', '--timeframe', '5m']),
    ('VOL_RELAXATION_START', 'analyze_volatility_transition_events.py', ['--event_type', 'VOL_RELAXATION_START', '--timeframe', '5m']),
    ('VOL_CLUSTER_SHIFT', 'analyze_volatility_transition_events.py', ['--event_type', 'VOL_CLUSTER_SHIFT', '--timeframe', '5m']),
    ('RANGE_COMPRESSION_END', 'analyze_volatility_transition_events.py', ['--event_type', 'RANGE_COMPRESSION_END', '--timeframe', '5m']),
    ('BREAKOUT_TRIGGER', 'analyze_volatility_transition_events.py', ['--event_type', 'BREAKOUT_TRIGGER', '--timeframe', '5m']),
    ('FUNDING_FLIP', 'analyze_positioning_extremes_events.py', ['--event_type', 'FUNDING_FLIP', '--timeframe', '5m']),
    ('DELEVERAGING_WAVE', 'analyze_positioning_extremes_events.py', ['--event_type', 'DELEVERAGING_WAVE', '--timeframe', '5m']),
    ('TREND_EXHAUSTION_TRIGGER', 'analyze_forced_flow_and_exhaustion_events.py', ['--event_type', 'TREND_EXHAUSTION_TRIGGER', '--timeframe', '5m']),
    ('MOMENTUM_DIVERGENCE_TRIGGER', 'analyze_forced_flow_and_exhaustion_events.py', ['--event_type', 'MOMENTUM_DIVERGENCE_TRIGGER', '--timeframe', '5m']),
    ('CLIMAX_VOLUME_BAR', 'analyze_forced_flow_and_exhaustion_events.py', ['--event_type', 'CLIMAX_VOLUME_BAR', '--timeframe', '5m']),
    ('FAILED_CONTINUATION', 'analyze_forced_flow_and_exhaustion_events.py', ['--event_type', 'FAILED_CONTINUATION', '--timeframe', '5m']),
    ('RANGE_BREAKOUT', 'analyze_trend_structure_events.py', ['--event_type', 'RANGE_BREAKOUT', '--timeframe', '5m']),
    ('FALSE_BREAKOUT', 'analyze_trend_structure_events.py', ['--event_type', 'FALSE_BREAKOUT', '--timeframe', '5m']),
    ('TREND_ACCELERATION', 'analyze_trend_structure_events.py', ['--event_type', 'TREND_ACCELERATION', '--timeframe', '5m']),
    ('TREND_DECELERATION', 'analyze_trend_structure_events.py', ['--event_type', 'TREND_DECELERATION', '--timeframe', '5m']),
    ('PULLBACK_PIVOT', 'analyze_trend_structure_events.py', ['--event_type', 'PULLBACK_PIVOT', '--timeframe', '5m']),
    ('SUPPORT_RESISTANCE_BREAK', 'analyze_trend_structure_events.py', ['--event_type', 'SUPPORT_RESISTANCE_BREAK', '--timeframe', '5m']),
    ('ZSCORE_STRETCH', 'analyze_statistical_dislocation_events.py', ['--event_type', 'ZSCORE_STRETCH', '--timeframe', '5m']),
    ('BAND_BREAK', 'analyze_statistical_dislocation_events.py', ['--event_type', 'BAND_BREAK', '--timeframe', '5m']),
    ('OVERSHOOT_AFTER_SHOCK', 'analyze_statistical_dislocation_events.py', ['--event_type', 'OVERSHOOT_AFTER_SHOCK', '--timeframe', '5m']),
    ('GAP_OVERSHOOT', 'analyze_statistical_dislocation_events.py', ['--event_type', 'GAP_OVERSHOOT', '--timeframe', '5m']),
    ('VOL_REGIME_SHIFT_EVENT', 'analyze_regime_transition_events.py', ['--event_type', 'VOL_REGIME_SHIFT_EVENT', '--timeframe', '5m']),
    ('TREND_TO_CHOP_SHIFT', 'analyze_regime_transition_events.py', ['--event_type', 'TREND_TO_CHOP_SHIFT', '--timeframe', '5m']),
    ('CHOP_TO_TREND_SHIFT', 'analyze_regime_transition_events.py', ['--event_type', 'CHOP_TO_TREND_SHIFT', '--timeframe', '5m']),
    ('CORRELATION_BREAKDOWN_EVENT', 'analyze_regime_transition_events.py', ['--event_type', 'CORRELATION_BREAKDOWN_EVENT', '--timeframe', '5m']),
    ('BETA_SPIKE_EVENT', 'analyze_regime_transition_events.py', ['--event_type', 'BETA_SPIKE_EVENT', '--timeframe', '5m']),
    ('INDEX_COMPONENT_DIVERGENCE', 'analyze_information_desync_events.py', ['--event_type', 'INDEX_COMPONENT_DIVERGENCE', '--timeframe', '5m']),
    ('SPOT_PERP_BASIS_SHOCK', 'analyze_information_desync_events.py', ['--event_type', 'SPOT_PERP_BASIS_SHOCK', '--timeframe', '5m']),
    ('LEAD_LAG_BREAK', 'analyze_information_desync_events.py', ['--event_type', 'LEAD_LAG_BREAK', '--timeframe', '5m']),
    ('SESSION_OPEN_EVENT', 'analyze_temporal_structure_events.py', ['--event_type', 'SESSION_OPEN_EVENT', '--timeframe', '5m']),
    ('SESSION_CLOSE_EVENT', 'analyze_temporal_structure_events.py', ['--event_type', 'SESSION_CLOSE_EVENT', '--timeframe', '5m']),
    ('FUNDING_TIMESTAMP_EVENT', 'analyze_temporal_structure_events.py', ['--event_type', 'FUNDING_TIMESTAMP_EVENT', '--timeframe', '5m']),
    ('SCHEDULED_NEWS_WINDOW_EVENT', 'analyze_temporal_structure_events.py', ['--event_type', 'SCHEDULED_NEWS_WINDOW_EVENT', '--timeframe', '5m']),
    ('SPREAD_REGIME_WIDENING_EVENT', 'analyze_execution_friction_events.py', ['--event_type', 'SPREAD_REGIME_WIDENING_EVENT', '--timeframe', '5m']),
    ('SLIPPAGE_SPIKE_EVENT', 'analyze_execution_friction_events.py', ['--event_type', 'SLIPPAGE_SPIKE_EVENT', '--timeframe', '5m']),
    ('FEE_REGIME_CHANGE_EVENT', 'analyze_execution_friction_events.py', ['--event_type', 'FEE_REGIME_CHANGE_EVENT', '--timeframe', '5m']),
]





def _parse_symbols_csv(symbols_csv: str) -> List[str]:
    symbols = [s.strip().upper() for s in str(symbols_csv).split(",") if s.strip()]
    ordered: List[str] = []
    seen = set()
    for symbol in symbols:
        if symbol not in seen:
            ordered.append(symbol)
            seen.add(symbol)
    return ordered


def _infer_symbol_tag(row: Dict[str, object], run_symbols: Sequence[str]) -> str:
    symbol_value = str(row.get("symbol", "")).strip().upper()
    if symbol_value:
        return symbol_value
    condition = str(row.get("condition", "")).strip().lower()
    if condition.startswith("symbol_"):
        inferred = condition.removeprefix("symbol_").upper()
        if inferred:
            return inferred
    if len(run_symbols) == 1:
        return str(run_symbols[0]).upper()
    return "ALL"

def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        out = float(value)
        if pd.isna(out):
            return default
        return out
    except (TypeError, ValueError):
        return default


def _safe_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        out = int(float(value))
        return out
    except (TypeError, ValueError):
        return default


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return False
        return bool(value)
    text = str(value).strip().lower()
    return text in {"1", "true", "t", "yes", "y"}


def _candidate_type_from_action(action_name: str) -> str:
    action = str(action_name or "").strip().lower()
    if action == "entry_gate_skip" or action.startswith("risk_throttle_"):
        return "overlay"
    if action == "no_action" or action.startswith("delay_") or action == "reenable_at_half_life":
        return "standalone"
    return "standalone"


def _phase2_row_to_candidate(
    run_id: str,
    event: str,
    row: Dict[str, object],
    idx: int,
    source_path: Path,
    default_status: str,
    run_symbols: Sequence[str],
) -> Dict[str, object]:
    risk_reduction = max(0.0, -_safe_float(row.get("delta_adverse_mean"), 0.0))
    opp_delta = _safe_float(row.get("delta_opportunity_mean"), 0.0)
    edge_score = _safe_float(row.get("edge_score"), risk_reduction + max(0.0, opp_delta))
    expected_return_proxy = _safe_float(row.get("expected_return_proxy"), opp_delta)
    expectancy_per_trade = _safe_float(
        row.get(
            "after_cost_expectancy_per_trade",
            row.get("expectancy_after_multiplicity", row.get("expectancy_per_trade")),
        ),
        expected_return_proxy,
    )
    after_cost_expectancy = _safe_float(row.get("after_cost_expectancy_per_trade"), expectancy_per_trade)
    stressed_after_cost_expectancy = _safe_float(row.get("stressed_after_cost_expectancy_per_trade"), after_cost_expectancy)
    cost_ratio = _safe_float(row.get("cost_ratio"), 0.0)
    turnover_proxy_mean = _safe_float(row.get("turnover_proxy_mean"), 0.0)
    avg_dynamic_cost_bps = _safe_float(row.get("avg_dynamic_cost_bps"), 0.0)
    selection_score_executed = _safe_float(
        row.get("selection_score_executed"),
        _safe_float(row.get("quality_score"), _safe_float(row.get("profit_density_score"), 0.0)),
    )

    gate_cols = [
        "gate_a_ci_separated",
        "gate_b_time_stable",
        "gate_c_regime_stable",
        "gate_d_friction_floor",
        "gate_f_exposure_guard",
        "gate_e_simplicity",
    ]
    gates_present = [g for g in gate_cols if g in row]
    if gates_present:
        stability_proxy = float(sum(1 for g in gates_present if _as_bool(row.get(g))) / len(gates_present))
    else:
        stability_proxy = _safe_float(row.get("stability_proxy"), 0.0)

    robustness_score = _safe_float(row.get("robustness_score"), stability_proxy)
    event_frequency = _safe_float(row.get("event_frequency"), 0.0)
    capacity_proxy = _safe_float(row.get("capacity_proxy"), 0.0)
    profit_density_score = _safe_float(
        row.get("profit_density_score"),
        max(0.0, expectancy_per_trade) * max(0.0, robustness_score) * max(0.0, event_frequency),
    )

    candidate_symbol = _infer_symbol_tag(row=row, run_symbols=run_symbols)

    return {
        "run_id": run_id,
        "candidate_symbol": candidate_symbol,
        "run_symbols": list(run_symbols),
        "symbol_scores": row.get("symbol_scores", "{}"),
        "rollout_eligible": _as_bool(row.get("rollout_eligible", False)),
        "event": event,
        "candidate_id": str(row.get("candidate_id", f"{event}_{idx}")),
        "status": str(row.get("status", default_status)),
        "candidate_type": str(row.get("candidate_type", _candidate_type_from_action(str(row.get("action", ""))))),
        "overlay_base_candidate_id": str(row.get("overlay_base_candidate_id", "")),
        "edge_score": edge_score,
        "expected_return_proxy": expected_return_proxy,
        "expectancy_per_trade": expectancy_per_trade,
        "after_cost_expectancy_per_trade": after_cost_expectancy,
        "stressed_after_cost_expectancy_per_trade": stressed_after_cost_expectancy,
        "selection_score_executed": selection_score_executed,
        "cost_ratio": cost_ratio,
        "turnover_proxy_mean": turnover_proxy_mean,
        "avg_dynamic_cost_bps": avg_dynamic_cost_bps,
        "bridge_eval_status": str(row.get("bridge_eval_status", "")),
        "bridge_train_after_cost_bps": _safe_float(row.get("bridge_train_after_cost_bps"), 0.0),
        "bridge_validation_after_cost_bps": _safe_float(row.get("bridge_validation_after_cost_bps"), 0.0),
        "bridge_validation_stressed_after_cost_bps": _safe_float(row.get("bridge_validation_stressed_after_cost_bps"), 0.0),
        "bridge_validation_trades": _safe_int(row.get("bridge_validation_trades"), 0),
        "bridge_effective_cost_bps_per_trade": _safe_float(row.get("bridge_effective_cost_bps_per_trade"), 0.0),
        "bridge_gross_edge_bps_per_trade": _safe_float(row.get("bridge_gross_edge_bps_per_trade"), 0.0),
        "gate_bridge_has_trades_validation": _as_bool(row.get("gate_bridge_has_trades_validation", False)),
        "gate_bridge_after_cost_positive_validation": _as_bool(row.get("gate_bridge_after_cost_positive_validation", False)),
        "gate_bridge_after_cost_stressed_positive_validation": _as_bool(row.get("gate_bridge_after_cost_stressed_positive_validation", False)),
        "gate_bridge_edge_cost_ratio": _as_bool(row.get("gate_bridge_edge_cost_ratio", False)),
        "gate_bridge_turnover_controls": _as_bool(row.get("gate_bridge_turnover_controls", False)),
        "gate_bridge_tradable": _as_bool(row.get("gate_bridge_tradable", False)),
        "gate_all_research": _as_bool(row.get("gate_all_research", False)),
        "variance": _safe_float(row.get("variance"), 0.0),
        "stability_proxy": stability_proxy,
        "robustness_score": robustness_score,
        "event_frequency": event_frequency,
        "capacity_proxy": capacity_proxy,
        "profit_density_score": profit_density_score,
        "n_events": _safe_int(row.get("sample_size", row.get("n_events", row.get("count", 0))), 0),
        "source_path": str(source_path),
        # Phase 2 semantic columns — must propagate so compiler can use them
        "is_discovery": _as_bool(row.get("is_discovery", False)),
        "phase2_quality_score": _safe_float(row.get("phase2_quality_score"), _safe_float(row.get("robustness_score"), 0.0)),
        "phase2_quality_components": str(row.get("phase2_quality_components", "{}")),
        "compile_eligible_phase2_fallback": _as_bool(row.get("compile_eligible_phase2_fallback", False)),
        "promotion_track": str(row.get("promotion_track", "fallback_only")),
    }


def _build_symbol_eval_lookup(event_dir: Path) -> Dict[str, Dict[str, object]]:
    path = event_dir / "phase2_symbol_evaluation.csv"
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}
    if df.empty:
        return {}

    grouped: Dict[str, List[Dict[str, object]]] = {}
    for _, row in df.iterrows():
        cid = str(row.get("candidate_id", "")).strip()
        if not cid:
            continue
        symbol = str(row.get("symbol", "ALL")).strip().upper() or "ALL"
        deployable = _as_bool(row.get("deployable", False))
        ev = _safe_float(row.get("ev"), 0.0)
        variance = _safe_float(row.get("variance"), 0.0)
        sharpe_like = _safe_float(row.get("sharpe_like"), 0.0)
        stability_score = _safe_float(row.get("stability_score"), 0.0)
        capacity_proxy = _safe_float(row.get("capacity_proxy"), 0.0)
        row_score = ev * max(0.0, sharpe_like) * max(0.0, stability_score)
        grouped.setdefault(cid, []).append(
            {
                "symbol": symbol,
                "deployable": deployable,
                "ev": ev,
                "variance": variance,
                "stability_score": stability_score,
                "capacity_proxy": capacity_proxy,
                "row_score": row_score,
            }
        )

    lookup: Dict[str, Dict[str, object]] = {}
    for cid, items in grouped.items():
        if not items:
            continue
        best = max(items, key=lambda item: float(item.get("row_score", -1e18)))
        symbol_scores = {
            str(item.get("symbol", "ALL")).strip().upper() or "ALL": _safe_float(item.get("row_score"), 0.0)
            for item in items
        }
        positive_scores = [score for score in symbol_scores.values() if score > 0.0]
        similar_score_band = True
        if len(positive_scores) > 1:
            max_score = max(positive_scores)
            min_score = min(positive_scores)
            similar_score_band = bool(min_score >= (0.75 * max_score))
        deployable_symbols = [item for item in items if bool(item.get("deployable", False))]
        rollout_eligible = bool(len(deployable_symbols) > 1 and similar_score_band)
        lookup[cid] = {
            "candidate_symbol": str(best.get("symbol", "ALL")).strip().upper() or "ALL",
            "symbol": str(best.get("symbol", "ALL")).strip().upper() or "ALL",
            "symbol_scores": json.dumps(symbol_scores),
            "rollout_eligible": rollout_eligible,
            "expectancy_per_trade": _safe_float(best.get("ev"), 0.0),
            "variance": _safe_float(best.get("variance"), 0.0),
            "stability_proxy": _safe_float(best.get("stability_score"), 0.0),
            "robustness_score": _safe_float(best.get("stability_score"), 0.0),
            "capacity_proxy": _safe_float(best.get("capacity_proxy"), 0.0),
            "profit_density_score": _safe_float(best.get("row_score"), 0.0),
            "status": "PROMOTED" if bool(best.get("deployable", False)) else "DRAFT",
        }
    return lookup


def _run_research_chain(
    run_id: str,
    symbols: str,
    run_hypothesis_generator: bool,
    hypothesis_datasets: str,
    hypothesis_max_fused: int,
) -> None:
    if run_hypothesis_generator:
        hypothesis_script = PROJECT_ROOT / "pipelines" / "research" / "generate_hypothesis_queue.py"
        if hypothesis_script.exists():
            hypothesis_cmd = [
                sys.executable,
                str(hypothesis_script),
                "--run_id",
                run_id,
                "--symbols",
                symbols,
                "--datasets",
                str(hypothesis_datasets),
                "--max_fused",
                str(int(hypothesis_max_fused)),
            ]
            hypothesis_result = subprocess.run(hypothesis_cmd)
            if hypothesis_result.returncode != 0:
                logging.warning("Hypothesis generator failed (non-blocking)")
        else:
            logging.warning("Missing hypothesis generator script (skipping): %s", hypothesis_script)

    phase2_script_path = PROJECT_ROOT / "pipelines" / "research" / "phase2_candidate_discovery.py"
    registry_script_path = PROJECT_ROOT / "pipelines" / "research" / "build_event_registry.py"
    bridge_script_path = PROJECT_ROOT / "pipelines" / "research" / "bridge_evaluate_phase2.py"
    for event_type, script, extra_args in PHASE2_EVENT_CHAIN:
        script_path = PROJECT_ROOT / "pipelines" / "research" / script
        if not script_path.exists():
            logging.warning("Missing phase1 script (skipping): %s", script_path)
            continue

        cmd = [sys.executable, str(script_path), "--run_id", run_id, "--symbols", symbols, *extra_args]
        result = subprocess.run(cmd)
        if result.returncode != 0:
            logging.warning("Phase1 stage failed (non-blocking): %s", script)
            continue

        if not phase2_script_path.exists():
            logging.warning("Missing phase2 script (skipping): %s", phase2_script_path)
            continue
        if registry_script_path.exists():
            registry_cmd = [
                sys.executable,
                str(registry_script_path),
                "--run_id",
                run_id,
                "--symbols",
                symbols,
                "--event_type",
                event_type,
                "--timeframe",
                "5m",
            ]
            registry_result = subprocess.run(registry_cmd)
            if registry_result.returncode != 0:
                logging.warning("Event registry stage failed (non-blocking): %s", event_type)
                continue
        else:
            logging.warning("Missing event-registry script (skipping): %s", registry_script_path)
            continue

        phase2_cmd = [
            sys.executable,
            str(phase2_script_path),
            "--run_id",
            run_id,
            "--event_type",
            event_type,
            "--symbols",
            symbols,
            "--mode",
            "research",
        ]
        phase2_result = subprocess.run(phase2_cmd)
        if phase2_result.returncode != 0:
            logging.warning("Phase2 stage failed (non-blocking): %s", event_type)
            continue
        if bridge_script_path.exists():
            bridge_cmd = [
                sys.executable,
                str(bridge_script_path),
                "--run_id",
                run_id,
                "--event_type",
                event_type,
                "--symbols",
                symbols,
            ]
            bridge_result = subprocess.run(bridge_cmd)
            if bridge_result.returncode != 0:
                logging.warning("Bridge stage failed (non-blocking): %s", event_type)


def _collect_phase2_candidates(run_id: str, run_symbols: Sequence[str]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    phase2_root = DATA_ROOT / "reports" / "phase2" / run_id
    if not phase2_root.exists():
        return rows

    for event_dir in sorted([p for p in phase2_root.iterdir() if p.is_dir()]):
        promoted_json = event_dir / "promoted_candidates.json"
        candidate_csv = event_dir / "phase2_candidates.csv"
        symbol_eval_lookup = _build_symbol_eval_lookup(event_dir)
        event_rows: List[Dict[str, object]] = []
        phase2_lookup: Dict[str, Dict[str, object]] = {}
        if candidate_csv.exists():
            try:
                phase2_df = pd.read_csv(candidate_csv)
            except Exception:
                phase2_df = pd.DataFrame()
            if not phase2_df.empty:
                for idx, payload in enumerate(phase2_df.to_dict(orient="records")):
                    cid = str(payload.get("candidate_id", "")).strip()
                    if not cid:
                        cond = str(payload.get("condition", "")).strip()
                        act = str(payload.get("action", "")).strip()
                        if cond and act:
                            cid = f"{cond}__{act}"
                            payload["candidate_id"] = cid
                    if cid:
                        phase2_lookup[cid] = payload

        if promoted_json.exists():
            payload = json.loads(promoted_json.read_text(encoding="utf-8"))
            promoted = payload.get("candidates", []) if isinstance(payload, dict) else []
            for idx, candidate in enumerate(promoted):
                if not isinstance(candidate, dict):
                    continue
                candidate_row = dict(candidate)
                cid = str(candidate_row.get("candidate_id", "")).strip()
                if not cid:
                    cond = str(candidate_row.get("condition", "")).strip()
                    act = str(candidate_row.get("action", "")).strip()
                    if cond and act:
                        cid = f"{cond}__{act}"
                        candidate_row["candidate_id"] = cid
                if cid and cid in phase2_lookup:
                    merged = dict(phase2_lookup[cid])
                    merged.update(candidate_row)
                    candidate_row = merged
                if ("gate_bridge_tradable" in candidate_row) and (not _as_bool(candidate_row.get("gate_bridge_tradable", False))):
                    continue
                if cid and cid in symbol_eval_lookup:
                    candidate_row.update(symbol_eval_lookup[cid])
                event_rows.append(
                    _phase2_row_to_candidate(
                        run_id=run_id,
                        event=event_dir.name,
                        row=candidate_row,
                        idx=idx,
                        source_path=promoted_json,
                        default_status="PROMOTED",
                        run_symbols=run_symbols,
                    )
                )

        if not event_rows and candidate_csv.exists():
            df = pd.read_csv(candidate_csv)
            if not df.empty:
                if "gate_all_research" in df.columns:
                    df = df[df["gate_all_research"].map(_as_bool)].copy()
                elif "gate_all" in df.columns:
                    df = df[df["gate_all"].map(_as_bool)].copy()
                if "gate_bridge_tradable" in df.columns:
                    df = df[df["gate_bridge_tradable"].map(_as_bool)].copy()
                if not df.empty:
                    for idx, row in df.iterrows():
                        row_payload = row.to_dict()
                        row_payload["status"] = str(row_payload.get("status", "PROMOTED_RESEARCH")).strip() or "PROMOTED_RESEARCH"
                        cid = str(row_payload.get("candidate_id", "")).strip()
                        if not cid:
                            cond = str(row_payload.get("condition", "")).strip()
                            act = str(row_payload.get("action", "")).strip()
                            if cond and act:
                                cid = f"{cond}__{act}"
                                row_payload["candidate_id"] = cid
                        if cid and cid in symbol_eval_lookup:
                            row_payload.update(symbol_eval_lookup[cid])
                        event_rows.append(
                            _phase2_row_to_candidate(
                                run_id=run_id,
                                event=event_dir.name,
                                row=row_payload,
                                idx=idx,
                                source_path=candidate_csv,
                                default_status="PROMOTED_RESEARCH",
                                run_symbols=run_symbols,
                            )
                        )

        rows.extend(event_rows)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Expand and normalize edge candidate universe")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True, help="Comma-separated discovery symbols for this run")
    parser.add_argument("--execute", type=int, default=0)
    parser.add_argument("--run_hypothesis_generator", type=int, default=1)
    parser.add_argument("--hypothesis_datasets", default="auto")
    parser.add_argument("--hypothesis_max_fused", type=int, default=24)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    run_symbols = _parse_symbols_csv(args.symbols)
    if not run_symbols:
        print("--symbols must include at least one symbol", file=sys.stderr)
        return 1

    params = {
        "run_id": args.run_id,
        "symbols": run_symbols,
        "execute": int(args.execute),
        "run_hypothesis_generator": int(args.run_hypothesis_generator),
        "hypothesis_datasets": str(args.hypothesis_datasets),
        "hypothesis_max_fused": int(args.hypothesis_max_fused),
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("export_edge_candidates", args.run_id, params, inputs, outputs)

    try:
        if int(args.execute):
            _run_research_chain(
                run_id=args.run_id,
                symbols=args.symbols,
                run_hypothesis_generator=bool(int(args.run_hypothesis_generator)),
                hypothesis_datasets=str(args.hypothesis_datasets),
                hypothesis_max_fused=int(args.hypothesis_max_fused),
            )

        rows = _collect_phase2_candidates(args.run_id, run_symbols=run_symbols)

        out_dir = DATA_ROOT / "reports" / "edge_candidates" / args.run_id
        ensure_dir(out_dir)
        out_csv = out_dir / "edge_candidates_normalized.parquet"
        out_json = out_dir / "edge_candidates_normalized.json"

        df = pd.DataFrame(
            rows,
            columns=[
                "run_id",
                "candidate_symbol",
                "run_symbols",
                "event",
                "candidate_id",
                "status",
                "candidate_type",
                "overlay_base_candidate_id",
                "edge_score",
                "expected_return_proxy",
                "expectancy_per_trade",
                "after_cost_expectancy_per_trade",
                "stressed_after_cost_expectancy_per_trade",
                "selection_score_executed",
                "bridge_eval_status",
                "bridge_train_after_cost_bps",
                "bridge_validation_after_cost_bps",
                "bridge_validation_stressed_after_cost_bps",
                "bridge_validation_trades",
                "bridge_effective_cost_bps_per_trade",
                "bridge_gross_edge_bps_per_trade",
                "gate_bridge_has_trades_validation",
                "gate_bridge_after_cost_positive_validation",
                "gate_bridge_after_cost_stressed_positive_validation",
                "gate_bridge_edge_cost_ratio",
                "gate_bridge_turnover_controls",
                "gate_bridge_tradable",
                "gate_all_research",
                "cost_ratio",
                "turnover_proxy_mean",
                "avg_dynamic_cost_bps",
                "variance",
                "stability_proxy",
                "robustness_score",
                "event_frequency",
                "capacity_proxy",
                "profit_density_score",
                "n_events",
                "source_path",
                # Phase 2 semantic columns
                "is_discovery",
                "phase2_quality_score",
                "phase2_quality_components",
                "compile_eligible_phase2_fallback",
                "promotion_track",
                "discovery_start",
                "discovery_end",
            ],
        )
        if not df.empty:
            df["selection_score_executed"] = pd.to_numeric(df.get("selection_score_executed"), errors="coerce").fillna(0.0)
            df = df.sort_values(
                ["selection_score_executed", "profit_density_score", "edge_score", "stability_proxy"],
                ascending=[False, False, False, False],
            ).reset_index(drop=True)
        df.to_parquet(out_csv, index=False)
        out_json.write_text(df.to_json(orient="records", indent=2), encoding="utf-8")

        outputs.append({"path": str(out_csv), "rows": int(len(df)), "start_ts": None, "end_ts": None})
        outputs.append({"path": str(out_json), "rows": int(len(df)), "start_ts": None, "end_ts": None})
        finalize_manifest(manifest, "success", stats={"candidate_count": int(len(df))})
        return 0
    except Exception as exc:  # pragma: no cover
        logging.exception("Edge candidate export failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
