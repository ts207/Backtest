from __future__ import annotations

import argparse
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sanitize(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", str(value).strip().lower()).strip("_")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize phase2 discovery quality across event families.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument(
        "--phase2_root",
        default="",
        help="Optional phase2 root directory (default: data/reports/phase2/<run_id>).",
    )
    parser.add_argument(
        "--out_path",
        default="",
        help="Optional output path (default: <phase2_root>/discovery_quality_summary.json).",
    )
    parser.add_argument(
        "--funnel_out_path",
        default="",
        help="Optional output path for funnel summary (default: data/reports/<run_id>/funnel_summary.json).",
    )
    parser.add_argument("--top_fail_reasons", type=int, default=10)
    return parser.parse_args()


def _load_candidates(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _load_json_object(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _load_jsonl_rows(path: Path) -> List[Dict[str, object]]:
    if not path.exists():
        return []
    rows: List[Dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _split_fail_reasons(series: pd.Series) -> List[str]:
    out: List[str] = []
    for raw in series.fillna("").astype(str):
        for token in raw.split(","):
            reason = token.strip()
            if reason:
                out.append(reason)
    return out


def _gate_pass_series(df: pd.DataFrame) -> pd.Series:
    if "gate_phase2_final" in df.columns:
        return pd.to_numeric(df["gate_phase2_final"], errors="coerce").fillna(0.0).astype(float) > 0.0
    if "gate_pass" in df.columns:
        return pd.to_numeric(df["gate_pass"], errors="coerce").fillna(0.0).astype(float) > 0.0
    if "gate_all" in df.columns:
        return pd.to_numeric(df["gate_all"], errors="coerce").fillna(0.0).astype(float) > 0.0
    return pd.Series(False, index=df.index)


def _event_summary(df: pd.DataFrame) -> Dict[str, float | int]:
    total = int(len(df))
    gate_pass = _gate_pass_series(df)
    pass_count = int(gate_pass.sum()) if total else 0
    pass_rate = float(pass_count / total) if total else 0.0
    return {
        "total_candidates": total,
        "gate_pass_count": pass_count,
        "gate_pass_rate": pass_rate,
    }


def _family_defaults() -> Dict[str, object]:
    return {
        "total_candidates": 0,
        "gate_pass_count": 0,
        "gate_pass_rate": 0.0,
        "phase2_candidates": 0,
        "phase2_gate_all_pass": 0,
        "bridge_evaluable": 0,
        "bridge_pass_val": 0,
        "overlay_kill_by_missing_base_count": 0,
        "compiled_bases": 0,
        "compiled_overlays": 0,
        "wf_tested": 0,
        "wf_survivors": 0,
        "top_failure_reasons": [],
    }


def build_summary(*, run_id: str, phase2_root: Path, top_fail_reasons: int) -> dict:
    event_dirs = sorted([p for p in phase2_root.iterdir() if p.is_dir()]) if phase2_root.exists() else []
    if not event_dirs:
        raise FileNotFoundError(f"No phase2 event directories found for run_id={run_id}: {phase2_root}")

    by_event_family: Dict[str, Dict[str, object]] = {}
    source_files: Dict[str, str] = {}
    global_fail_counter: Counter[str] = Counter()
    family_fail_counter: Dict[str, Counter[str]] = defaultdict(Counter)

    bridge_root = DATA_ROOT / "reports" / "bridge_eval" / run_id
    for event_dir in event_dirs:
        family = str(event_dir.name)
        candidates_path = event_dir / "phase2_candidates.csv"
        frame = _load_candidates(candidates_path) if candidates_path.exists() else pd.DataFrame()
        if frame.empty:
            frame = pd.DataFrame(
                columns=[
                    "candidate_id",
                    "gate_phase2_final",
                    "gate_pass",
                    "fail_reasons",
                    "gate_all",
                    "gate_bridge_tradable",
                ]
            )
        frame["event_type"] = family
        source_files[family] = str(candidates_path)

        summary = _event_summary(frame)
        family_row = _family_defaults()
        family_row.update(summary)

        family_row["phase2_candidates"] = int(len(frame))
        family_row["phase2_gate_all_pass"] = int(_gate_pass_series(frame).sum())

        phase2_reasons = _split_fail_reasons(frame.get("fail_reasons", pd.Series(dtype=str)))
        family_fail_counter[family].update(phase2_reasons)
        global_fail_counter.update(phase2_reasons)

        bridge_path = bridge_root / family / "bridge_candidate_metrics.csv"
        bridge_df = _load_candidates(bridge_path) if bridge_path.exists() else pd.DataFrame()
        if not bridge_df.empty:
            eval_mask = pd.Series(True, index=bridge_df.index)
            if "bridge_eval_status" in bridge_df.columns:
                eval_mask = bridge_df["bridge_eval_status"].astype(str).str.strip().ne("")
            family_row["bridge_evaluable"] = int(eval_mask.sum())
            if "gate_bridge_tradable" in bridge_df.columns:
                family_row["bridge_pass_val"] = int(pd.to_numeric(bridge_df["gate_bridge_tradable"], errors="coerce").fillna(0.0).astype(float).gt(0).sum())
            # Count overlays killed due to missing base candidate.
            if "bridge_fail_reasons" in bridge_df.columns:
                missing_base_mask = bridge_df["bridge_fail_reasons"].astype(str).str.contains(
                    "gate_bridge_missing_overlay_base", regex=False, na=False
                )
                family_row["overlay_kill_by_missing_base_count"] = int(missing_base_mask.sum())
            elif "bridge_eval_status" in bridge_df.columns:
                missing_base_mask = bridge_df["bridge_eval_status"].astype(str).str.contains(
                    "missing_overlay_base", regex=False, na=False
                )
                family_row["overlay_kill_by_missing_base_count"] = int(missing_base_mask.sum())
            bridge_reasons = _split_fail_reasons(bridge_df.get("bridge_fail_reasons", pd.Series(dtype=str)))
            family_fail_counter[family].update(bridge_reasons)
            global_fail_counter.update(bridge_reasons)

        by_event_family[family] = family_row

    blueprints_path = DATA_ROOT / "reports" / "strategy_blueprints" / run_id / "blueprints.jsonl"
    blueprints_rows = _load_jsonl_rows(blueprints_path)
    strategy_to_family: Dict[str, str] = {}
    for row in blueprints_rows:
        family = str(row.get("event_type", "")).strip()
        if not family:
            continue
        by_event_family.setdefault(family, _family_defaults())
        by_event_family[family]["compiled_bases"] = int(by_event_family[family].get("compiled_bases", 0)) + 1
        overlays = row.get("overlays", [])
        overlay_count = len(overlays) if isinstance(overlays, list) else 0
        by_event_family[family]["compiled_overlays"] = int(by_event_family[family].get("compiled_overlays", 0)) + int(overlay_count)

        bp_id = str(row.get("id", "")).strip()
        if bp_id:
            strategy_to_family[f"dsl_interpreter_v1__{_sanitize(bp_id)}"] = family

    wf_summary = _load_json_object(DATA_ROOT / "reports" / "eval" / run_id / "walkforward_summary.json")
    wf_per_strategy = wf_summary.get("per_strategy_split_metrics", {})
    if isinstance(wf_per_strategy, dict):
        for strategy_id, split_payload in wf_per_strategy.items():
            if not isinstance(strategy_id, str) or not isinstance(split_payload, dict):
                continue
            family = strategy_to_family.get(strategy_id, "")
            if not family:
                continue
            has_validation = isinstance(split_payload.get("validation"), dict)
            has_test = isinstance(split_payload.get("test"), dict)
            if has_validation and has_test:
                by_event_family.setdefault(family, _family_defaults())
                by_event_family[family]["wf_tested"] = int(by_event_family[family].get("wf_tested", 0)) + 1

    promotion_report = _load_json_object(DATA_ROOT / "reports" / "promotions" / run_id / "promotion_report.json")
    tested_rows = promotion_report.get("tested", [])
    if isinstance(tested_rows, list):
        for row in tested_rows:
            if not isinstance(row, dict):
                continue
            family = str(row.get("family", row.get("event_type", ""))).strip()
            if not family:
                strategy_id = str(row.get("strategy_id", "")).strip()
                family = strategy_to_family.get(strategy_id, "")
            if not family:
                continue
            by_event_family.setdefault(family, _family_defaults())
            if bool(row.get("promoted", False)):
                by_event_family[family]["wf_survivors"] = int(by_event_family[family].get("wf_survivors", 0)) + 1
            reasons = row.get("fail_reasons", [])
            if isinstance(reasons, list):
                tokens = [str(x).strip() for x in reasons if str(x).strip()]
                family_fail_counter[family].update(tokens)
                global_fail_counter.update(tokens)

    for family, counter in family_fail_counter.items():
        by_event_family.setdefault(family, _family_defaults())
        by_event_family[family]["top_failure_reasons"] = [
            {"reason": reason, "count": int(count)}
            for reason, count in counter.most_common(max(0, int(top_fail_reasons)))
        ]

    event_families = sorted(by_event_family.keys())
    total_candidates = int(sum(int(by_event_family[f].get("total_candidates", 0)) for f in event_families))
    gate_pass_count = int(sum(int(by_event_family[f].get("gate_pass_count", 0)) for f in event_families))
    gate_pass_rate = float(gate_pass_count / total_candidates) if total_candidates else 0.0

    top_reasons = [
        {"reason": reason, "count": int(count)}
        for reason, count in global_fail_counter.most_common(max(0, int(top_fail_reasons)))
    ]

    return {
        "run_id": run_id,
        "generated_at": _utc_now_iso(),
        "phase2_root": str(phase2_root),
        "source_files": source_files,
        "event_families": event_families,
        "total_candidates": total_candidates,
        "gate_pass_count": gate_pass_count,
        "gate_pass_rate": gate_pass_rate,
        "top_fail_reasons": top_reasons,
        "by_event_family": by_event_family,
    }


def _build_funnel_payload(summary: Dict[str, object], *, top_fail_reasons: int) -> Dict[str, object]:
    by_event_family = summary.get("by_event_family", {})
    if not isinstance(by_event_family, dict):
        by_event_family = {}

    families: Dict[str, Dict[str, object]] = {}
    totals = {
        "phase2_candidates": 0,
        "phase2_gate_all_pass": 0,
        "bridge_evaluable": 0,
        "bridge_pass_val": 0,
        "overlay_kill_by_missing_base_count": 0,
        "compiled_bases": 0,
        "compiled_overlays": 0,
        "wf_tested": 0,
        "wf_survivors": 0,
    }
    global_fail_counter: Counter[str] = Counter()

    for family in sorted(by_event_family.keys()):
        row = by_event_family.get(family, {})
        if not isinstance(row, dict):
            continue
        family_counts = {
            "phase2_candidates": int(row.get("phase2_candidates", 0) or 0),
            "phase2_gate_all_pass": int(row.get("phase2_gate_all_pass", 0) or 0),
            "bridge_evaluable": int(row.get("bridge_evaluable", 0) or 0),
            "bridge_pass_val": int(row.get("bridge_pass_val", 0) or 0),
            "overlay_kill_by_missing_base_count": int(row.get("overlay_kill_by_missing_base_count", 0) or 0),
            "compiled_bases": int(row.get("compiled_bases", 0) or 0),
            "compiled_overlays": int(row.get("compiled_overlays", 0) or 0),
            "wf_tested": int(row.get("wf_tested", 0) or 0),
            "wf_survivors": int(row.get("wf_survivors", 0) or 0),
        }
        for key, value in family_counts.items():
            totals[key] += int(value)

        top_family = row.get("top_failure_reasons", [])
        if isinstance(top_family, list):
            for item in top_family:
                if not isinstance(item, dict):
                    continue
                reason = str(item.get("reason", "")).strip()
                count = int(item.get("count", 0) or 0)
                if reason and count > 0:
                    global_fail_counter[reason] += count

        families[family] = {
            **family_counts,
            "top_failure_reasons": top_family if isinstance(top_family, list) else [],
        }

    payload = {
        "run_id": str(summary.get("run_id", "")),
        "generated_at": str(summary.get("generated_at", _utc_now_iso())),
        "families": families,
        "totals": totals,
        "top_failure_reasons": [
            {"reason": reason, "count": int(count)}
            for reason, count in global_fail_counter.most_common(max(0, int(top_fail_reasons)))
        ],
        "diagnostic_flags": {
            "all_bridge_pass_zero": bool(totals["bridge_pass_val"] == 0),
        },
    }
    return payload


def main() -> int:
    args = _parse_args()
    phase2_root = Path(args.phase2_root) if args.phase2_root else DATA_ROOT / "reports" / "phase2" / args.run_id
    out_path = Path(args.out_path) if args.out_path else phase2_root / "discovery_quality_summary.json"
    funnel_out_path = (
        Path(args.funnel_out_path)
        if args.funnel_out_path
        else DATA_ROOT / "reports" / args.run_id / "funnel_summary.json"
    )

    payload = build_summary(
        run_id=args.run_id,
        phase2_root=phase2_root,
        top_fail_reasons=int(args.top_fail_reasons),
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    funnel_payload = _build_funnel_payload(payload, top_fail_reasons=int(args.top_fail_reasons))
    funnel_out_path.parent.mkdir(parents=True, exist_ok=True)
    funnel_out_path.write_text(json.dumps(funnel_payload, indent=2, sort_keys=True), encoding="utf-8")

    print(json.dumps({"run_id": args.run_id, "out_path": str(out_path), "funnel_out_path": str(funnel_out_path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
