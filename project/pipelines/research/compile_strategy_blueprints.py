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

from pipelines._lib.io_utils import ensure_dir
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


def _parse_symbol_scope(row: Dict[str, object], run_symbols: List[str]) -> SymbolScopeSpec:
    candidate_symbol = str(row.get("candidate_symbol", "")).strip().upper() or "ALL"
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


def _entry_from_row(row: Dict[str, object], event_type: str, time_stop_bars: int) -> EntrySpec:
    policy = event_policy(event_type)
    robustness = _safe_float(row.get("robustness_score"), 0.0)
    action = str(row.get("action", "no_action"))
    delay = _derive_delay(action=action, robustness=robustness, time_stop_bars=time_stop_bars)
    cooldown = max(4, delay * 2, int(round(time_stop_bars / 4.0)))
    confirmations = [str(x) for x in policy.get("confirmations", [])]
    if not _as_bool(row.get("gate_oos_validation_test", True)) and "oos_validation_pass" in confirmations:
        confirmations = [x for x in confirmations if x != "oos_validation_pass"]
    condition = str(row.get("condition", "all"))
    condition_nodes: List[ConditionNodeSpec] = []
    for operator in [">=", "<=", "==", ">", "<"]:
        if operator in condition and condition.strip() != "all":
            col, raw_val = condition.split(operator, 1)
            threshold = _safe_float(raw_val.strip(), np.nan)
            if col.strip() and not np.isnan(threshold):
                condition_nodes = [
                    ConditionNodeSpec(
                        feature=col.strip(),
                        operator=operator,  # type: ignore[arg-type]
                        value=float(threshold),
                    )
                ]
                condition = "all"
            break

    return EntrySpec(
        triggers=[str(x) for x in policy.get("triggers", ["event_detected"])],
        conditions=[condition],
        confirmations=confirmations,
        delay_bars=delay,
        cooldown_bars=cooldown,
        condition_logic="all",
        condition_nodes=condition_nodes,
        arm_bars=delay,
        reentry_lockout_bars=max(cooldown, delay),
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
            "oos_required": _as_bool(row.get("gate_oos_validation_test", True)),
            "multiplicity_required": _as_bool(row.get("gate_multiplicity", True)),
            "regime_stability_required": _as_bool(row.get("gate_c_regime_stable", True)),
        },
    )


def _rank_key(row: Dict[str, object]) -> Tuple[float, float, float, float, str]:
    return (
        -_safe_float(row.get("robustness_score"), 0.0),
        -float(_as_bool(row.get("gate_oos_validation_test", False))),
        -float(_as_bool(row.get("gate_multiplicity", False))),
        -_safe_float(row.get("profit_density_score"), 0.0),
        str(row.get("candidate_id", "")),
    )


def _choose_event_rows(event_type: str, edge_rows: List[Dict[str, object]], phase2_df: pd.DataFrame, max_per_event: int) -> List[Dict[str, object]]:
    promoted = [row for row in edge_rows if str(row.get("status", "")).upper() == "PROMOTED"]
    if promoted:
        return sorted(promoted, key=_rank_key)[:max_per_event]

    if edge_rows:
        return [sorted(edge_rows, key=_rank_key)[0]]

    if not phase2_df.empty:
        fallback_df = phase2_df.copy()
        for col in ("robustness_score", "profit_density_score"):
            if col not in fallback_df.columns:
                fallback_df[col] = 0.0
        if "candidate_id" not in fallback_df.columns:
            fallback_df["candidate_id"] = [
                _candidate_id(row, idx) for idx, row in enumerate(fallback_df.to_dict(orient="records"))
            ]
        row = fallback_df.sort_values(
            by=["robustness_score", "profit_density_score", "candidate_id"],
            ascending=[False, False, True],
        ).iloc[0].to_dict()
        row["event"] = event_type
        row["status"] = "DRAFT"
        row["candidate_id"] = str(row.get("candidate_id", _candidate_id(row, 0)))
        row["candidate_symbol"] = str(row.get("symbol", "ALL")).upper() if "symbol" in row else "ALL"
        row["source_path"] = str(DATA_ROOT / "reports" / "phase2" / str(row.get("run_id", "")) / event_type / "phase2_candidates.csv")
        return [row]

    return [
        {
            "event": event_type,
            "status": "DRAFT",
            "candidate_id": f"{event_type}_default",
            "candidate_symbol": "ALL",
            "condition": "all",
            "action": "no_action",
            "n_events": 0,
            "source_path": str(DATA_ROOT / "reports" / "phase2"),
        }
    ]


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

    symbol_scope = _parse_symbol_scope(merged, run_symbols=run_symbols)
    time_stop_bars = _derive_time_stop(stats.get("half_life", np.array([])), merged)
    stop_value, target_value = _derive_stop_target(stats=stats, row=merged)
    entry = _entry_from_row(merged, event_type=event_type, time_stop_bars=time_stop_bars)
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Compile deterministic strategy blueprints from enriched edge candidates")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--max_per_event", type=int, default=2)
    parser.add_argument("--fees_bps", type=float, default=3.0)
    parser.add_argument("--slippage_bps", type=float, default=1.0)
    parser.add_argument("--out_dir", default=None)
    args = parser.parse_args()

    run_symbols = _parse_symbols(args.symbols)
    if not run_symbols:
        print("--symbols must include at least one symbol", file=sys.stderr)
        return 1

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "strategy_blueprints" / args.run_id
    ensure_dir(out_dir)

    params = {
        "run_id": args.run_id,
        "symbols": run_symbols,
        "max_per_event": int(args.max_per_event),
        "fees_bps": float(args.fees_bps),
        "slippage_bps": float(args.slippage_bps),
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("compile_strategy_blueprints", args.run_id, params, inputs, outputs)

    try:
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

        phase2_root = DATA_ROOT / "reports" / "phase2" / args.run_id
        event_types = sorted([p.name for p in phase2_root.iterdir() if p.is_dir()]) if phase2_root.exists() else []
        if not event_types and edge_rows:
            event_types = sorted({str(row.get("event", "")).strip() for row in edge_rows if str(row.get("event", "")).strip()})

        blueprints: List[Blueprint] = []
        fallback_count = 0
        for event_type in event_types:
            event_edge_rows = [row for row in edge_rows if str(row.get("event", "")).strip() == event_type]
            phase2_df = _load_phase2_table(run_id=args.run_id, event_type=event_type)
            phase2_lookup = {}
            if not phase2_df.empty:
                for idx, row in enumerate(phase2_df.to_dict(orient="records")):
                    cid = _candidate_id(row, idx)
                    phase2_lookup[cid] = dict(row)

            selected = _choose_event_rows(
                event_type=event_type,
                edge_rows=event_edge_rows,
                phase2_df=phase2_df,
                max_per_event=int(args.max_per_event),
            )
            if selected and str(selected[0].get("status", "")).upper() != "PROMOTED":
                fallback_count += 1

            stats = _event_stats(run_id=args.run_id, event_type=event_type)
            for row in selected:
                bp = _build_blueprint(
                    run_id=args.run_id,
                    run_symbols=run_symbols,
                    event_type=event_type,
                    row=row,
                    phase2_lookup=phase2_lookup,
                    stats=stats,
                    fees_bps=float(args.fees_bps),
                    slippage_bps=float(args.slippage_bps),
                )
                blueprints.append(bp)

        blueprints = sorted(blueprints, key=lambda b: (b.event_type, b.candidate_id, b.id))

        out_jsonl = out_dir / "blueprints.jsonl"
        lines = [json.dumps(bp.to_dict(), sort_keys=True) for bp in blueprints]
        out_jsonl.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

        summary = {
            "run_id": args.run_id,
            "compiler_version": COMPILER_VERSION,
            "event_types": event_types,
            "blueprint_count": int(len(blueprints)),
            "fallback_event_count": int(fallback_count),
            "per_event_counts": {
                event_type: int(sum(1 for bp in blueprints if bp.event_type == event_type)) for event_type in event_types
            },
        }
        out_summary = out_dir / "blueprints_summary.json"
        out_summary.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

        outputs.append({"path": str(out_jsonl), "rows": int(len(blueprints)), "start_ts": None, "end_ts": None})
        outputs.append({"path": str(out_summary), "rows": int(len(blueprints)), "start_ts": None, "end_ts": None})
        finalize_manifest(
            manifest,
            "success",
            stats={
                "event_count": int(len(event_types)),
                "blueprint_count": int(len(blueprints)),
                "fallback_event_count": int(fallback_count),
            },
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
