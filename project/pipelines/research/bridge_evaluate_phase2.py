from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from eval.splits import build_time_splits
from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.run_manifest import finalize_manifest, start_manifest

BRIDGE_FIELDS = [
    "candidate_type",
    "overlay_base_candidate_id",
    "bridge_eval_status",
    "bridge_train_after_cost_bps",
    "bridge_validation_after_cost_bps",
    "bridge_validation_stressed_after_cost_bps",
    "exp_costed_x0_5",
    "exp_costed_x1_0",
    "exp_costed_x1_5",
    "exp_costed_x2_0",
    "bridge_validation_trades",
    "bridge_effective_cost_bps_per_trade",
    "bridge_gross_edge_bps_per_trade",
    "bridge_edge_to_cost_ratio",
    "gate_bridge_has_trades_validation",
    "gate_bridge_after_cost_positive_validation",
    "gate_bridge_after_cost_stressed_positive_validation",
    "gate_bridge_edge_cost_ratio",
    "gate_bridge_turnover_controls",
    "gate_bridge_tradable",
    "selection_score_executed",
]


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    if not np.isfinite(out):
        return float(default)
    return float(out)


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _candidate_type_from_action(action_name: str) -> str:
    action = str(action_name or "").strip().lower()
    if action == "entry_gate_skip" or action.startswith("risk_throttle_"):
        return "overlay"
    if action == "no_action" or action.startswith("delay_") or action == "reenable_at_half_life":
        return "standalone"
    return "standalone"


def _add_reason(existing: str, reason: str) -> str:
    existing_text = str(existing).strip()
    if existing_text.lower() == "nan":
        existing_text = ""
    reasons = [x for x in existing_text.split(",") if x]
    if reason and reason not in reasons:
        reasons.append(reason)
    return ",".join(reasons)


def _load_candidates(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _resolve_overlay_bases(df: pd.DataFrame, event_type: str) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    action_series = out["action"] if "action" in out.columns else pd.Series("", index=out.index, dtype=str)
    if "candidate_type" not in out.columns:
        out["candidate_type"] = action_series.astype(str).map(_candidate_type_from_action)
    else:
        out["candidate_type"] = out["candidate_type"].astype(str).replace("", np.nan)
        out["candidate_type"] = out["candidate_type"].fillna(action_series.astype(str).map(_candidate_type_from_action))
    if "overlay_base_candidate_id" not in out.columns:
        out["overlay_base_candidate_id"] = ""
    out["overlay_base_candidate_id"] = out["overlay_base_candidate_id"].fillna("").astype(str)

    no_action_rows = out[action_series.astype(str) == "no_action"]
    base_by_condition: Dict[str, str] = {}
    for _, row in no_action_rows.iterrows():
        condition = str(row.get("condition", "")).strip()
        candidate_id = str(row.get("candidate_id", "")).strip()
        if condition and candidate_id and condition not in base_by_condition:
            base_by_condition[condition] = candidate_id
    fallback_base = f"BASE_TEMPLATE::{str(event_type).strip().lower()}"
    overlay_mask = out["candidate_type"].astype(str) == "overlay"
    for idx in out[overlay_mask].index:
        existing = str(out.at[idx, "overlay_base_candidate_id"]).strip()
        if existing:
            continue
        condition = str(out.at[idx, "condition"]).strip() if "condition" in out.columns else ""
        out.at[idx, "overlay_base_candidate_id"] = base_by_condition.get(condition, fallback_base)
    return out


def _effective_cost_bps(row: pd.Series) -> float:
    avg_dynamic = max(0.0, _safe_float(row.get("avg_dynamic_cost_bps"), np.nan))
    turnover = max(0.0, _safe_float(row.get("turnover_proxy_mean"), np.nan))
    if avg_dynamic > 0.0:
        return float(avg_dynamic * max(turnover, 0.10))
    cost_ratio = max(0.0, _safe_float(row.get("cost_ratio"), 0.0))
    after_cost = _safe_float(row.get("after_cost_expectancy_per_trade"), _safe_float(row.get("expectancy_per_trade"), 0.0))
    gross_proxy_bps = abs(after_cost) * 10_000.0
    if cost_ratio <= 0.0:
        return 0.0
    return float(max(0.0, gross_proxy_bps * min(1.0, cost_ratio)))


def _bridge_metrics_for_row(row: pd.Series, stressed_cost_multiplier: float) -> Dict[str, float]:
    expectancy_after = _safe_float(row.get("after_cost_expectancy_per_trade"), _safe_float(row.get("expectancy_per_trade"), 0.0))
    stressed_after = _safe_float(
        row.get("stressed_after_cost_expectancy_per_trade"),
        expectancy_after,
    )
    effective_cost = _effective_cost_bps(row)
    validation_after_bps = float(expectancy_after * 10_000.0)
    validation_stressed_bps = float(stressed_after * 10_000.0)
    if not np.isfinite(validation_stressed_bps):
        validation_stressed_bps = float(validation_after_bps - ((float(stressed_cost_multiplier) - 1.0) * effective_cost))
    gross_edge_bps = float(max(0.0, validation_after_bps + effective_cost))
    edge_to_cost_ratio = float(gross_edge_bps / max(effective_cost, 1e-9))
    costed_x0_5 = float(validation_after_bps + (0.5 * effective_cost))
    costed_x1_0 = float(validation_after_bps)
    costed_x1_5 = float(validation_after_bps - (0.5 * effective_cost))
    costed_x2_0 = float(validation_after_bps - effective_cost)
    train_after_bps = float(validation_after_bps)
    validation_trades = max(0, _safe_int(row.get("validation_samples"), _safe_int(row.get("sample_size"), 0)))
    return {
        "bridge_train_after_cost_bps": train_after_bps,
        "bridge_validation_after_cost_bps": validation_after_bps,
        "bridge_validation_stressed_after_cost_bps": validation_stressed_bps,
        "exp_costed_x0_5": costed_x0_5,
        "exp_costed_x1_0": costed_x1_0,
        "exp_costed_x1_5": costed_x1_5,
        "exp_costed_x2_0": costed_x2_0,
        "bridge_validation_trades": int(validation_trades),
        "bridge_effective_cost_bps_per_trade": effective_cost,
        "bridge_gross_edge_bps_per_trade": gross_edge_bps,
        "bridge_edge_to_cost_ratio": edge_to_cost_ratio,
    }


def _evaluate_bridge_row(
    row: pd.Series,
    *,
    event_type: str,
    base_lookup: Dict[str, Dict[str, float]],
    edge_cost_k: float,
    min_validation_trades: int,
    stressed_cost_multiplier: float,
) -> Tuple[Dict[str, object], Dict[str, object] | None]:
    candidate_id = str(row.get("candidate_id", "")).strip()
    candidate_type = str(row.get("candidate_type", "")).strip() or _candidate_type_from_action(str(row.get("action", "")))
    overlay_base_candidate_id = str(row.get("overlay_base_candidate_id", "")).strip()
    metrics = _bridge_metrics_for_row(row, stressed_cost_multiplier=float(stressed_cost_multiplier))
    overlay_delta: Dict[str, object] | None = None
    base_turnover = np.nan

    if candidate_type == "overlay":
        base_key = overlay_base_candidate_id or f"BASE_TEMPLATE::{str(event_type).strip().lower()}"
        base_metrics = base_lookup.get(base_key)
        if base_metrics is None:
            status = "rejected:missing_overlay_base"
            return (
                {
                    "candidate_id": candidate_id,
                    "candidate_type": candidate_type,
                    "overlay_base_candidate_id": base_key,
                    "bridge_eval_status": status,
                    **metrics,
                    "gate_bridge_has_trades_validation": False,
                    "gate_bridge_after_cost_positive_validation": False,
                    "gate_bridge_after_cost_stressed_positive_validation": False,
                    "gate_bridge_edge_cost_ratio": False,
                    "gate_bridge_turnover_controls": False,
                    "gate_bridge_tradable": False,
                    "selection_score_executed": 0.0,
                    "bridge_fail_reasons": "gate_bridge_missing_overlay_base",
                },
                None,
            )
        base_turnover = _safe_float(base_metrics.get("turnover_proxy_mean"), np.nan)
        metrics["bridge_train_after_cost_bps"] = float(metrics["bridge_train_after_cost_bps"] - _safe_float(base_metrics.get("bridge_train_after_cost_bps"), 0.0))
        metrics["bridge_validation_after_cost_bps"] = float(
            metrics["bridge_validation_after_cost_bps"] - _safe_float(base_metrics.get("bridge_validation_after_cost_bps"), 0.0)
        )
        metrics["bridge_validation_stressed_after_cost_bps"] = float(
            metrics["bridge_validation_stressed_after_cost_bps"]
            - _safe_float(base_metrics.get("bridge_validation_stressed_after_cost_bps"), 0.0)
        )
        metrics["bridge_gross_edge_bps_per_trade"] = float(
            max(0.0, metrics["bridge_validation_after_cost_bps"] + _safe_float(metrics["bridge_effective_cost_bps_per_trade"], 0.0))
        )
        metrics["bridge_edge_to_cost_ratio"] = float(
            max(0.0, metrics["bridge_gross_edge_bps_per_trade"]) / max(_safe_float(metrics["bridge_effective_cost_bps_per_trade"], 0.0), 1e-9)
        )
        overlay_delta = {
            "candidate_id": candidate_id,
            "overlay_base_candidate_id": base_key,
            "delta_validation_after_cost_bps": float(metrics["bridge_validation_after_cost_bps"]),
            "delta_validation_stressed_after_cost_bps": float(metrics["bridge_validation_stressed_after_cost_bps"]),
        }

    has_trades = int(metrics["bridge_validation_trades"]) >= int(min_validation_trades)
    after_positive = float(metrics["bridge_validation_after_cost_bps"]) > 0.0
    stressed_positive = float(metrics["bridge_validation_stressed_after_cost_bps"]) > 0.0
    effective_cost = max(0.0, float(metrics["bridge_effective_cost_bps_per_trade"]))
    gross_edge = max(0.0, float(metrics["bridge_gross_edge_bps_per_trade"]))
    metrics["bridge_edge_to_cost_ratio"] = float(gross_edge / max(effective_cost, 1e-9))
    edge_cost_ratio_gate = bool(gross_edge >= (float(edge_cost_k) * effective_cost))

    turnover_proxy = max(0.0, _safe_float(row.get("turnover_proxy_mean"), 0.0))
    turnover_improved = bool(np.isnan(base_turnover) or turnover_proxy <= (base_turnover + 1e-9))
    one_trade_per_event = turnover_proxy <= 1.0
    gate_turnover_controls = bool(turnover_improved and one_trade_per_event and stressed_positive)

    gate_tradable = bool(has_trades and after_positive and stressed_positive and edge_cost_ratio_gate and gate_turnover_controls)
    fail_reasons: List[str] = []
    if not has_trades:
        fail_reasons.append("gate_bridge_has_trades_validation")
    if not after_positive:
        fail_reasons.append("gate_bridge_after_cost_positive_validation")
    if not stressed_positive:
        fail_reasons.append("gate_bridge_after_cost_stressed_positive_validation")
    if not edge_cost_ratio_gate:
        fail_reasons.append("gate_bridge_edge_cost_ratio")
    if not gate_turnover_controls:
        fail_reasons.append("gate_bridge_turnover_controls")

    status = "tradable" if gate_tradable else ("rejected:" + ",".join(fail_reasons))
    return (
        {
            "candidate_id": candidate_id,
            "candidate_type": candidate_type,
            "overlay_base_candidate_id": overlay_base_candidate_id,
            "bridge_eval_status": status,
            **metrics,
            "gate_bridge_has_trades_validation": bool(has_trades),
            "gate_bridge_after_cost_positive_validation": bool(after_positive),
            "gate_bridge_after_cost_stressed_positive_validation": bool(stressed_positive),
            "gate_bridge_edge_cost_ratio": bool(edge_cost_ratio_gate),
            "gate_bridge_turnover_controls": bool(gate_turnover_controls),
            "gate_bridge_tradable": bool(gate_tradable),
            "selection_score_executed": float(metrics["bridge_validation_after_cost_bps"]),
            "bridge_fail_reasons": ",".join(fail_reasons),
        },
        overlay_delta,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bridge-evaluate Phase2 candidates for tradability.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--event_type", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", default="")
    parser.add_argument("--end", default="")
    parser.add_argument("--train_frac", type=float, default=0.6)
    parser.add_argument("--validation_frac", type=float, default=0.2)
    parser.add_argument("--embargo_days", type=int, default=1)
    parser.add_argument("--edge_cost_k", type=float, default=2.0)
    parser.add_argument("--stressed_cost_multiplier", type=float, default=1.5)
    parser.add_argument("--min_validation_trades", type=int, default=20)
    parser.add_argument(
        "--mode",
        choices=["research", "production", "certification"],
        default="production",
        help="Run mode. Research mode evaluates pre-discovery research survivors.",
    )
    parser.add_argument("--out_dir", default="")
    return parser.parse_args()


def _select_bridge_candidates(full_candidates: pd.DataFrame, mode: str) -> pd.DataFrame:
    if full_candidates.empty:
        return pd.DataFrame()
    if str(mode) == "research":
        if "gate_phase2_research" in full_candidates.columns:
            return full_candidates[full_candidates["gate_phase2_research"].map(_as_bool)].copy()
        # Backward-compatible fallback for older artifacts.
        return full_candidates[
            full_candidates["gate_phase2_final"].map(_as_bool) & full_candidates["is_discovery"].map(_as_bool)
        ].copy()
    return full_candidates[
        full_candidates["gate_phase2_final"].map(_as_bool) & full_candidates["is_discovery"].map(_as_bool)
    ].copy()


def main() -> int:
    args = _parse_args()
    event_type = str(args.event_type).strip()
    if not event_type:
        print("--event_type must be non-empty", file=sys.stderr)
        return 1

    phase2_dir = DATA_ROOT / "reports" / "phase2" / args.run_id / event_type
    phase2_candidates_path = phase2_dir / "phase2_candidates.csv"
    full_candidates = _load_candidates(phase2_candidates_path)
    
    survivors = _select_bridge_candidates(full_candidates=full_candidates, mode=str(args.mode))

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "bridge_eval" / args.run_id / event_type
    ensure_dir(out_dir)
    out_candidate_metrics = out_dir / "bridge_candidate_metrics.csv"
    out_overlay_metrics = out_dir / "bridge_overlay_delta_metrics.csv"
    out_summary_json = out_dir / "bridge_summary.json"
    out_summary_md = out_dir / "bridge_summary.md"

    params = {
        "run_id": args.run_id,
        "event_type": event_type,
        "symbols": [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()],
        "start": str(args.start),
        "end": str(args.end),
        "train_frac": float(args.train_frac),
        "validation_frac": float(args.validation_frac),
        "embargo_days": int(args.embargo_days),
        "edge_cost_k": float(args.edge_cost_k),
        "stressed_cost_multiplier": float(args.stressed_cost_multiplier),
        "min_validation_trades": int(args.min_validation_trades),
    }
    inputs: List[Dict[str, object]] = [{"path": str(phase2_candidates_path), "rows": len(full_candidates), "start_ts": None, "end_ts": None}]
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("bridge_evaluate_phase2", args.run_id, params, inputs, outputs)

    try:
        windows = []
        if str(args.start).strip() and str(args.end).strip():
            windows = [w.to_dict() for w in build_time_splits(
                start=str(args.start),
                end=str(args.end),
                train_frac=float(args.train_frac),
                validation_frac=float(args.validation_frac),
                embargo_days=int(args.embargo_days),
            )]

        if survivors.empty:
            empty_metrics = pd.DataFrame(columns=["candidate_id", *BRIDGE_FIELDS, "bridge_fail_reasons", "bridge_embargo_days_used"])
            empty_metrics.to_csv(out_candidate_metrics, index=False)
            pd.DataFrame(columns=["candidate_id", "overlay_base_candidate_id", "delta_validation_after_cost_bps", "delta_validation_stressed_after_cost_bps"]).to_csv(out_overlay_metrics, index=False)
            if not full_candidates.empty:
                updated = full_candidates.copy()
                updated["bridge_embargo_days_used"] = int(args.embargo_days)
                updated["gate_bridge_tradable"] = False
                updated["gate_bridge_has_trades_validation"] = False
                updated["gate_bridge_after_cost_positive_validation"] = False
                updated["gate_bridge_after_cost_stressed_positive_validation"] = False
                updated["gate_bridge_edge_cost_ratio"] = False
                updated["gate_bridge_turnover_controls"] = False
                updated["bridge_fail_reasons"] = "NO_BRIDGE_ELIGIBLE_CANDIDATES"
                updated["gate_all"] = False
                updated.to_csv(phase2_candidates_path, index=False)
            summary_payload = {
                "run_id": args.run_id,
                "event_type": event_type,
                "candidate_count": 0,
                "tradable_count": 0,
                "windows": windows,
            }
            out_summary_json.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
            out_summary_md.write_text(
                "\n".join(
                    [
                        "# Bridge Evaluation Summary",
                        "",
                        f"- Run ID: `{args.run_id}`",
                        f"- Event type: `{event_type}`",
                        "- Candidate count: `0` (no Phase 2 survivors)",
                        "- Tradable count: `0`",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            outputs.append({"path": str(out_candidate_metrics), "rows": 0, "start_ts": None, "end_ts": None})
            outputs.append({"path": str(out_overlay_metrics), "rows": 0, "start_ts": None, "end_ts": None})
            outputs.append({"path": str(out_summary_json), "rows": 1, "start_ts": None, "end_ts": None})
            outputs.append({"path": str(out_summary_md), "rows": 1, "start_ts": None, "end_ts": None})
            finalize_manifest(manifest, "success", stats={"candidate_count": 0, "tradable_count": 0})
            return 0

        survivors = _resolve_overlay_bases(survivors, event_type=event_type)
        metrics_rows: List[Dict[str, object]] = []
        overlay_rows: List[Dict[str, object]] = []

        # Base metrics from no_action rows are used for overlay delta scoring.
        base_lookup: Dict[str, Dict[str, float]] = {}
        for _, row in survivors.iterrows():
            candidate_id = str(row.get("candidate_id", "")).strip()
            if not candidate_id:
                continue
            if str(row.get("action", "")).strip() != "no_action":
                continue
            metrics = _bridge_metrics_for_row(row, stressed_cost_multiplier=float(args.stressed_cost_multiplier))
            metrics["turnover_proxy_mean"] = _safe_float(row.get("turnover_proxy_mean"), 0.0)
            base_lookup[candidate_id] = metrics
            condition = str(row.get("condition", "")).strip()
            if condition:
                base_lookup.setdefault(condition, metrics)

        for _, row in survivors.iterrows():
            result, overlay_delta = _evaluate_bridge_row(
                row,
                event_type=event_type,
                base_lookup=base_lookup,
                edge_cost_k=float(args.edge_cost_k),
                min_validation_trades=int(args.min_validation_trades),
                stressed_cost_multiplier=float(args.stressed_cost_multiplier),
            )
            metrics_rows.append(result)
            if overlay_delta is not None:
                overlay_rows.append(overlay_delta)

        metrics_df = pd.DataFrame(metrics_rows)
        metrics_df["bridge_embargo_days_used"] = int(args.embargo_days)
        overlay_df = pd.DataFrame(overlay_rows)
        metrics_df.to_csv(out_candidate_metrics, index=False)
        overlay_df.to_csv(out_overlay_metrics, index=False)

        updated = full_candidates.copy()
        updated = updated.merge(metrics_df, on="candidate_id", how="left", suffixes=("", "_bridge"))
        # If bridge_fail_reasons clashed with an existing column, consolidate.
        if "bridge_fail_reasons_bridge" in updated.columns:
            updated["bridge_fail_reasons"] = updated["bridge_fail_reasons_bridge"]
            updated = updated.drop(columns=["bridge_fail_reasons_bridge"])
        # Keep merged bridge columns canonical.
        for col in BRIDGE_FIELDS:
            bridge_col = f"{col}_bridge"
            if bridge_col in updated.columns:
                updated[col] = updated[bridge_col]
                updated = updated.drop(columns=[bridge_col])
            if col not in updated.columns:
                updated[col] = np.nan

        if "gate_all" in updated.columns:
            updated["gate_all"] = updated["gate_all"].astype(bool) & updated["gate_bridge_tradable"].astype(bool)
        else:
            updated["gate_all"] = updated["gate_bridge_tradable"].astype(bool)
        if "fail_reasons" not in updated.columns:
            updated["fail_reasons"] = ""
        else:
            updated["fail_reasons"] = updated["fail_reasons"].fillna("").astype(str)
        fail_lookup = {str(row.get("candidate_id", "")): str(row.get("bridge_fail_reasons", "")) for row in metrics_rows}
        for idx, row in updated.iterrows():
            cid = str(row.get("candidate_id", "")).strip()
            bridge_fail = fail_lookup.get(cid, "")
            if bridge_fail:
                for token in [x for x in bridge_fail.split(",") if x]:
                    updated.at[idx, "fail_reasons"] = _add_reason(str(updated.at[idx, "fail_reasons"]), token)

        updated.to_csv(phase2_candidates_path, index=False)

        promoted_path = phase2_dir / "promoted_candidates.json"
        if promoted_path.exists():
            try:
                payload = json.loads(promoted_path.read_text(encoding="utf-8"))
            except Exception:
                payload = {}
            if isinstance(payload, dict):
                tradable_ids = set(updated[updated["gate_bridge_tradable"].astype(bool)]["candidate_id"].astype(str).tolist())
                candidates_list = payload.get("candidates", [])
                if isinstance(candidates_list, list):
                    payload["candidates"] = [
                        row for row in candidates_list
                        if isinstance(row, dict) and str(row.get("candidate_id", "")).strip() in tradable_ids
                    ]
                    payload["promoted_count"] = int(len(payload["candidates"]))
                    payload["decision"] = "promote" if payload["promoted_count"] > 0 else "freeze"
                promoted_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

        tradable_count = int(metrics_df["gate_bridge_tradable"].astype(bool).sum()) if not metrics_df.empty else 0
        summary_payload = {
            "run_id": args.run_id,
            "event_type": event_type,
            "candidate_count": int(len(metrics_df)),
            "tradable_count": int(tradable_count),
            "overlay_candidate_count": int((metrics_df["candidate_type"].astype(str) == "overlay").sum()) if not metrics_df.empty else 0,
            "edge_cost_k": float(args.edge_cost_k),
            "min_validation_trades": int(args.min_validation_trades),
            "windows": windows,
        }
        out_summary_json.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
        out_summary_md.write_text(
            "\n".join(
                [
                    "# Bridge Evaluation Summary",
                    "",
                    f"- Run ID: `{args.run_id}`",
                    f"- Event type: `{event_type}`",
                    f"- Candidate count: `{int(len(metrics_df))}`",
                    f"- Tradable count: `{int(tradable_count)}`",
                    f"- Edge-cost multiplier k: `{float(args.edge_cost_k):.2f}`",
                    f"- Minimum validation trades: `{int(args.min_validation_trades)}`",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        outputs.append({"path": str(out_candidate_metrics), "rows": int(len(metrics_df)), "start_ts": None, "end_ts": None})
        outputs.append({"path": str(out_overlay_metrics), "rows": int(len(overlay_df)), "start_ts": None, "end_ts": None})
        outputs.append({"path": str(out_summary_json), "rows": 1, "start_ts": None, "end_ts": None})
        outputs.append({"path": str(out_summary_md), "rows": 1, "start_ts": None, "end_ts": None})
        finalize_manifest(
            manifest,
            "success",
            stats={
                "candidate_count": int(len(metrics_df)),
                "tradable_count": int(tradable_count),
                "overlay_candidate_count": int((metrics_df["candidate_type"].astype(str) == "overlay").sum()) if not metrics_df.empty else 0,
            },
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
