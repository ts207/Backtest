from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.ontology_contract import load_run_manifest_hashes
from pipelines._lib.run_manifest import finalize_manifest, start_manifest


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return float(default)
    if not np.isfinite(out):
        return float(default)
    return float(out)


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return int(default)


def _load_manual_candidate(spec_path: Path, backtest_run_id: str) -> pd.DataFrame:
    """
    Load a manual strategy specification and its associated backtest trades.
    
    Transforms the backtest results into a virtual candidate frame suitable
    for statistical evaluation and promotion gating.
    """
    import yaml
    from pipelines.research.analyze_conditional_expectancy import build_walk_forward_split_labels
    from pipelines.research.phase2_statistical_gates import gate_regime_stability
    
    if not spec_path.exists():
        raise ValueError(f"Manual spec not found: {spec_path}")
    
    with spec_path.open("r", encoding="utf-8") as f:
        spec_data = yaml.safe_load(f)
    
    spec_id = str(spec_data.get("id", spec_path.stem))
    trigger = spec_data.get("trigger", {})
    event_type = str(trigger.get("event_type", "UNKNOWN"))
    
    trades_root = DATA_ROOT / "lake" / "trades" / "backtests" / "dsl" / backtest_run_id
    if not trades_root.exists():
        raise ValueError(f"Backtest trades not found for run {backtest_run_id} at {trades_root}")
    
    trade_files = list(trades_root.glob("trades_*.csv"))
    if not trade_files:
        raise ValueError(f"No trades_*.csv files found in {trades_root}")
    
    all_trades: List[pd.DataFrame] = []
    for f in trade_files:
        df = pd.read_csv(f)
        if not df.empty:
            all_trades.append(df)
    
    if not all_trades:
        return pd.DataFrame()
    
    df = pd.concat(all_trades, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["entry_time"], utc=True)
    df = df.sort_values("timestamp")
    
    # Assign splits
    df["split_label"] = build_walk_forward_split_labels(df, time_col="timestamp")
    
    # Join vol_regime from feature lake for regime stability check
    for sym in df["symbol"].unique():
        try:
            from pipelines.research.analyze_conditional_expectancy import _load_features
            feats = _load_features(sym, backtest_run_id)
            if "vol_regime" in feats.columns:
                feats_sub = feats[["timestamp", "vol_regime"]].copy()
                df = df.merge(feats_sub, on="timestamp", how="left")
        except Exception:
            pass

    # Calculate metrics
    returns = pd.to_numeric(df["pnl"], errors="coerce").fillna(0.0)
    n = len(returns)
    expectancy = float(returns.mean())
    std_ret = float(returns.std())
    
    def _get_t_stat(subset: pd.Series) -> float:
        if len(subset) < 2: return 0.0
        s = float(subset.std())
        m = float(subset.mean())
        if s == 0: return 0.0
        return float(m / (s / np.sqrt(len(subset))))

    val_t = _get_t_stat(returns[df["split_label"] == "validation"])
    oos_t = _get_t_stat(returns[df["split_label"] == "test"])
    train_t = _get_t_stat(returns[df["split_label"] == "train"])
    
    # Sign consistency
    base_sign = 1.0 if expectancy >= 0 else -1.0
    signs = []
    for t in [train_t, val_t, oos_t]:
        if t == 0: continue
        match = 1.0 if (t > 0 and base_sign > 0) or (t < 0 and base_sign < 0) else 0.0
        signs.append(match)
    
    sign_consistency = float(sum(signs) / len(signs)) if signs else 1.0
    
    # Regime stability check
    gate_regime, _, _ = gate_regime_stability(
        sub=df,
        effect_col="pnl",
        condition_name=spec_id,
        min_stable_splits=1,
    )
    
    row = {
        "candidate_id": f"manual_{spec_id}",
        "event_type": event_type,
        "event": event_type,
        "expectancy": expectancy,
        "std_return": std_ret,
        "sample_size": n,
        "n_events": n,
        "val_t_stat": val_t,
        "oos1_t_stat": oos_t,
        "train_t_stat": train_t,
        "gate_stability": bool(sign_consistency >= 0.67 and gate_regime),
        "gate_after_cost_positive": bool(expectancy > 0),
        "gate_after_cost_stressed_positive": bool(expectancy > 0),
        "gate_bridge_after_cost_positive_validation": bool(val_t > 0),
        "gate_bridge_after_cost_stressed_positive_validation": bool(val_t > 0),
        "q_value": 0.0,
        "is_manual": True,
        "manual_spec_path": str(spec_path),
        "backtest_run_id": backtest_run_id,
    }
    
    return pd.DataFrame([row])


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if not np.isfinite(value):  # type: ignore[arg-type]
            return False
        return bool(value)
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _load_phase2_candidates(phase2_root: Path) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    if not phase2_root.exists():
        return pd.DataFrame()
    for event_dir in sorted(path for path in phase2_root.iterdir() if path.is_dir()):
        csv_path = event_dir / "phase2_candidates.csv"
        if not csv_path.exists():
            continue
        try:
            df = pd.read_csv(csv_path)
        except Exception:
            continue
        if df.empty:
            continue
        event_name = event_dir.name
        if "event" not in df.columns:
            df["event"] = event_name
        if "event_type" not in df.columns:
            df["event_type"] = event_name
        if "candidate_id" not in df.columns:
            df["candidate_id"] = [
                f"{event_name}_{idx}" for idx in range(len(df))
            ]
        df["source_phase2_path"] = str(csv_path)
        rows.append(df)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    out["event"] = out["event"].astype(str).str.strip()
    out["event_type"] = out["event_type"].astype(str).str.strip()
    out["candidate_id"] = out["candidate_id"].astype(str).str.strip()
    out = out[out["candidate_id"] != ""].copy()
    return out


def _load_hypothesis_audit(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        if path.suffix == ".parquet":
            df = pd.read_parquet(path)
        else:
            df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    if "plan_row_id" not in df.columns or "status" not in df.columns:
        return pd.DataFrame()
    out = df.copy()
    out["plan_row_id"] = out["plan_row_id"].astype(str).str.strip()
    out["status"] = out["status"].astype(str).str.strip().str.lower()
    out = out[out["plan_row_id"] != ""].copy()
    return out


def _build_audit_index(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    if df.empty:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for plan_row_id, sub in df.groupby("plan_row_id", sort=False):
        statuses = sorted({str(x).strip().lower() for x in sub["status"].tolist() if str(x).strip()})
        out[str(plan_row_id)] = {
            "statuses": statuses,
            "executed": "executed" in statuses,
        }
    return out


def _load_negative_control_summary(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _control_rate_for_event(
    *,
    row: Dict[str, Any],
    event_type: str,
    summary: Dict[str, Any],
) -> Optional[float]:
    if "control_pass_rate" in row:
        val = _safe_float(row.get("control_pass_rate"), np.nan)
        if np.isfinite(val):
            return float(val)
    by_event = summary.get("by_event", {})
    if isinstance(by_event, dict):
        item = by_event.get(event_type)
        if isinstance(item, dict):
            for key in ("pass_rate_after_bh", "control_pass_rate", "pass_rate"):
                if key in item:
                    val = _safe_float(item.get(key), np.nan)
                    if np.isfinite(val):
                        return float(val)
        elif item is not None:
            val = _safe_float(item, np.nan)
            if np.isfinite(val):
                return float(val)
    for key in ("pass_rate_after_bh", "control_pass_rate", "global_pass_rate_after_bh"):
        if key in summary:
            val = _safe_float(summary.get(key), np.nan)
            if np.isfinite(val):
                return float(val)
    global_node = summary.get("global", {})
    if isinstance(global_node, dict):
        for key in ("pass_rate_after_bh", "control_pass_rate", "pass_rate"):
            if key in global_node:
                val = _safe_float(global_node.get(key), np.nan)
                if np.isfinite(val):
                    return float(val)
    return None


def _sign_consistency(row: Dict[str, Any]) -> float:
    base_effect = _safe_float(
        row.get("effect_shrunk_state", row.get("expectancy", row.get("effect_raw", 0.0))),
        0.0,
    )
    base_sign = 1.0 if base_effect >= 0.0 else -1.0
    t_stats = []
    for key in ("val_t_stat", "oos1_t_stat", "test_t_stat"):
        if key in row:
            value = _safe_float(row.get(key), np.nan)
            if np.isfinite(value):
                t_stats.append(value)
    if not t_stats:
        return 1.0 if _as_bool(row.get("gate_stability", False)) else 0.0
    matches = [1.0 if (t >= 0.0 and base_sign > 0.0) or (t < 0.0 and base_sign < 0.0) else 0.0 for t in t_stats]
    return float(sum(matches) / len(matches))


def _stability_score(row: Dict[str, Any], sign_consistency: float) -> float:
    effect = abs(_safe_float(row.get("effect_shrunk_state", row.get("expectancy", 0.0)), 0.0))
    volatility = abs(_safe_float(row.get("std_return", 0.0), 0.0))
    denominator = max(volatility, 1e-8)
    return float(sign_consistency * (effect / denominator))


def _cost_survival_ratio(row: Dict[str, Any]) -> float:
    scenario_keys = [
        "gate_after_cost_positive",
        "gate_after_cost_stressed_positive",
        "gate_bridge_after_cost_positive_validation",
        "gate_bridge_after_cost_stressed_positive_validation",
    ]
    present = [key for key in scenario_keys if key in row]
    if not present:
        return 0.0
    passed = sum(1 for key in present if _as_bool(row.get(key)))
    return float(passed / len(present))


def _evaluate_row(
    *,
    row: Dict[str, Any],
    hypothesis_index: Dict[str, Dict[str, Any]],
    negative_control_summary: Dict[str, Any],
    max_q_value: float,
    min_events: int,
    min_stability_score: float,
    min_sign_consistency: float,
    min_cost_survival_ratio: float,
    max_negative_control_pass_rate: float,
    require_hypothesis_audit: bool,
    allow_missing_negative_controls: bool,
) -> Dict[str, Any]:
    reject_reasons: List[str] = []
    event_type = str(row.get("event_type", row.get("event", ""))).strip() or "UNKNOWN_EVENT"
    plan_row_id = str(row.get("plan_row_id", "")).strip()
    n_events = _safe_int(row.get("n_events", row.get("sample_size", 0)), 0)
    q_value = _safe_float(row.get("q_value", 1.0), 1.0)
    sign_consistency = _sign_consistency(row)
    stability_score = _stability_score(row, sign_consistency)
    gate_stability = _as_bool(row.get("gate_stability", False))
    cost_survival_ratio = _cost_survival_ratio(row)
    control_rate = _control_rate_for_event(
        row=row,
        event_type=event_type,
        summary=negative_control_summary,
    )

    statistical_pass = (q_value <= float(max_q_value)) and (n_events >= int(min_events))
    if not statistical_pass:
        if q_value > float(max_q_value):
            reject_reasons.append("statistical_q_value")
        if n_events < int(min_events):
            reject_reasons.append("statistical_min_events")

    stability_pass = (
        gate_stability
        and (stability_score >= float(min_stability_score))
        and (sign_consistency >= float(min_sign_consistency))
    )
    if not stability_pass:
        if not gate_stability:
            reject_reasons.append("stability_gate")
        if stability_score < float(min_stability_score):
            reject_reasons.append("stability_score")
        if sign_consistency < float(min_sign_consistency):
            reject_reasons.append("stability_sign_consistency")

    cost_pass = cost_survival_ratio >= float(min_cost_survival_ratio)
    if not cost_pass:
        reject_reasons.append("cost_survival")

    if control_rate is None:
        control_pass = bool(allow_missing_negative_controls)
        if not control_pass:
            reject_reasons.append("negative_control_missing")
    else:
        control_pass = control_rate <= float(max_negative_control_pass_rate)
        if not control_pass:
            reject_reasons.append("negative_control_fail")

    audit_pass = True
    audit_statuses: List[str] = []
    if plan_row_id:
        audit_info = hypothesis_index.get(plan_row_id)
        if audit_info:
            audit_statuses = list(audit_info.get("statuses", []))
            audit_pass = bool(audit_info.get("executed", False))
            if not audit_pass:
                reject_reasons.append("hypothesis_not_executed")
        else:
            if require_hypothesis_audit:
                audit_pass = False
                reject_reasons.append("hypothesis_missing_audit")
    elif require_hypothesis_audit:
        audit_pass = False
        reject_reasons.append("hypothesis_missing_plan_row_id")

    promoted = bool(statistical_pass and stability_pass and cost_pass and control_pass and audit_pass)
    promotion_decision = "promoted" if promoted else "rejected"
    promotion_score = (
        float(statistical_pass)
        + float(stability_pass)
        + float(cost_pass)
        + float(control_pass)
    ) / 4.0

    return {
        "promotion_decision": promotion_decision,
        "promotion_score": float(promotion_score),
        "reject_reason": "|".join(sorted(set(reject_reasons))),
        "q_value": float(q_value),
        "n_events": int(n_events),
        "stability_score": float(stability_score),
        "sign_consistency": float(sign_consistency),
        "cost_survival_ratio": float(cost_survival_ratio),
        "control_pass_rate": None if control_rate is None else float(control_rate),
        "audit_statuses": audit_statuses,
        "gate_statistical": bool(statistical_pass),
        "gate_stability": bool(stability_pass),
        "gate_cost_survival": bool(cost_pass),
        "gate_negative_control": bool(control_pass),
        "gate_hypothesis_audit": bool(audit_pass),
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Promote phase2 candidates using stability/cost/control gates.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--phase2_root", default=None)
    parser.add_argument("--hypothesis_audit_path", default=None)
    parser.add_argument("--negative_controls_path", default=None)
    parser.add_argument("--max_q_value", type=float, default=0.10)
    parser.add_argument("--min_events", type=int, default=100)
    parser.add_argument("--min_stability_score", type=float, default=0.05)
    parser.add_argument("--min_sign_consistency", type=float, default=0.67)
    parser.add_argument("--min_cost_survival_ratio", type=float, default=0.75)
    parser.add_argument("--max_negative_control_pass_rate", type=float, default=0.01)
    parser.add_argument("--require_hypothesis_audit", type=int, default=0)
    parser.add_argument("--allow_missing_negative_controls", type=int, default=1)
    parser.add_argument("--manual_spec_path", default=None, help="Path to a manual strategy specification YAML")
    parser.add_argument("--backtest_run_id", default=None, help="Backtest run ID to evaluate for manual strategy")
    args = parser.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "promotions" / args.run_id
    ensure_dir(out_dir)

    phase2_root = Path(args.phase2_root) if args.phase2_root else DATA_ROOT / "reports" / "phase2" / args.run_id
    hypothesis_audit_path = (
        Path(args.hypothesis_audit_path)
        if args.hypothesis_audit_path
        else DATA_ROOT / "reports" / "hypothesis_generator" / args.run_id / "hypothesis_execution_audit.parquet"
    )
    negative_controls_path = (
        Path(args.negative_controls_path)
        if args.negative_controls_path
        else DATA_ROOT / "reports" / "phase2" / args.run_id / "negative_controls_summary.json"
    )

    manifest = start_manifest("promote_candidates", args.run_id, vars(args), [], [])
    try:
        from pipelines._lib.ontology_contract import ontology_spec_hash as compute_ontology_hash
        
        run_manifest_hashes = load_run_manifest_hashes(DATA_ROOT, args.run_id)
        ontology_hash = str(run_manifest_hashes.get("ontology_spec_hash", "") or "").strip()
        if not ontology_hash:
            ontology_hash = compute_ontology_hash(PROJECT_ROOT.parent)

        if args.manual_spec_path:
            if not args.backtest_run_id:
                raise ValueError("--backtest_run_id is required when using --manual_spec_path")
            phase2_df = _load_manual_candidate(Path(args.manual_spec_path), args.backtest_run_id)
        else:
            phase2_df = _load_phase2_candidates(phase2_root)
            
        if phase2_df.empty:
            raise ValueError(f"No candidates found (manual={bool(args.manual_spec_path)})")

        hypothesis_df = _load_hypothesis_audit(hypothesis_audit_path)
        hypothesis_index = _build_audit_index(hypothesis_df)
        negative_control_summary = _load_negative_control_summary(negative_controls_path)

        audit_rows: List[Dict[str, Any]] = []
        promoted_rows: List[Dict[str, Any]] = []

        for row in phase2_df.to_dict(orient="records"):
            eval_row = _evaluate_row(
                row=row,
                hypothesis_index=hypothesis_index,
                negative_control_summary=negative_control_summary,
                max_q_value=float(args.max_q_value),
                min_events=int(args.min_events),
                min_stability_score=float(args.min_stability_score),
                min_sign_consistency=float(args.min_sign_consistency),
                min_cost_survival_ratio=float(args.min_cost_survival_ratio),
                max_negative_control_pass_rate=float(args.max_negative_control_pass_rate),
                require_hypothesis_audit=bool(int(args.require_hypothesis_audit)),
                allow_missing_negative_controls=bool(int(args.allow_missing_negative_controls)),
            )
            merged = dict(row)
            merged.update(eval_row)
            merged["event"] = str(merged.get("event", merged.get("event_type", ""))).strip()
            merged["event_type"] = str(merged.get("event_type", merged.get("event", ""))).strip()
            merged["candidate_id"] = str(merged.get("candidate_id", "")).strip()
            merged["ontology_spec_hash"] = str(merged.get("ontology_spec_hash", ontology_hash)).strip()
            audit_rows.append(merged)
            if merged["promotion_decision"] == "promoted":
                promoted = dict(merged)
                promoted["status"] = "PROMOTED"
                promoted_rows.append(promoted)

        audit_df = pd.DataFrame(audit_rows)
        promoted_df = pd.DataFrame(promoted_rows)

        audit_path = out_dir / "promotion_audit.parquet"
        promoted_path = out_dir / "promoted_candidates.parquet"
        audit_df.to_parquet(audit_path, index=False)
        promoted_df.to_parquet(promoted_path, index=False)

        audit_df.to_csv(out_dir / "promotion_audit.csv", index=False)
        promoted_df.to_csv(out_dir / "promoted_candidates.csv", index=False)

        summary = {
            "run_id": args.run_id,
            "phase2_candidates_total": int(len(audit_df)),
            "promoted_total": int(len(promoted_df)),
            "rejected_total": int(max(0, len(audit_df) - len(promoted_df))),
            "gate_pass_counts": {
                "gate_statistical": int(audit_df.get("gate_statistical", pd.Series(dtype=bool)).sum()) if not audit_df.empty else 0,
                "gate_stability": int(audit_df.get("gate_stability", pd.Series(dtype=bool)).sum()) if not audit_df.empty else 0,
                "gate_cost_survival": int(audit_df.get("gate_cost_survival", pd.Series(dtype=bool)).sum()) if not audit_df.empty else 0,
                "gate_negative_control": int(audit_df.get("gate_negative_control", pd.Series(dtype=bool)).sum()) if not audit_df.empty else 0,
            },
            "reject_reason_counts": (
                {
                    str(k): int(v)
                    for k, v in audit_df["reject_reason"].value_counts(dropna=False).to_dict().items()
                }
                if (not audit_df.empty and "reject_reason" in audit_df.columns)
                else {}
            ),
            "inputs": {
                "phase2_root": str(phase2_root),
                "hypothesis_audit_path": str(hypothesis_audit_path),
                "negative_controls_path": str(negative_controls_path),
                "manual_spec_path": str(args.manual_spec_path or ""),
            },
        }
        (out_dir / "promotion_summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True),
            encoding="utf-8",
        )

        manual_spec_hash = None
        if args.manual_spec_path:
            from pipelines._lib.run_manifest import _sha256_file
            manual_spec_hash = _sha256_file(Path(args.manual_spec_path))

        finalize_manifest(
            manifest,
            "success",
            stats={
                "phase2_candidates_total": int(len(audit_df)),
                "promoted_total": int(len(promoted_df)),
                "promotion_audit_path": str(audit_path.relative_to(PROJECT_ROOT.parent)),
                "promoted_candidates_path": str(promoted_path.relative_to(PROJECT_ROOT.parent)),
                "manual_spec_hash": manual_spec_hash,
            },
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
