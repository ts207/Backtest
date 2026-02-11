from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT / "data"))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a discovery-only run checklist from edge/expectancy artifacts.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument(
        "--reports_root",
        default=str(DATA_ROOT / "reports"),
        help="Root reports directory (default: data/reports)",
    )
    parser.add_argument(
        "--runs_root",
        default=str(DATA_ROOT / "runs"),
        help="Root runs directory (default: data/runs)",
    )
    parser.add_argument(
        "--out_dir",
        default="",
        help="Optional output directory (default: data/runs/<run_id>/research_checklist)",
    )
    parser.add_argument("--min_edge_candidates", type=int, default=1)
    parser.add_argument("--min_expectancy_evidence", type=int, default=1)
    parser.add_argument("--min_robust_survivors", type=int, default=1)
    parser.add_argument("--require_expectancy_exists", type=int, default=1)
    return parser.parse_args()


def _gate_result(name: str, passed: bool, observed: Any, threshold: Any, note: str = "") -> dict[str, Any]:
    return {
        "name": name,
        "passed": bool(passed),
        "observed": observed,
        "threshold": threshold,
        "note": note,
    }


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _edge_candidate_metrics(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"rows": 0, "promoted": 0}

    rows = 0
    promoted = 0
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                rows += 1
                if str(row.get("status", "")).strip().upper() == "PROMOTED":
                    promoted += 1
    except OSError:
        return {"rows": 0, "promoted": 0}
    return {"rows": rows, "promoted": promoted}


def _build_payload(
    run_id: str,
    args: argparse.Namespace,
    edge_metrics: dict[str, Any],
    expectancy_payload: dict[str, Any],
    robustness_payload: dict[str, Any],
    paths: dict[str, str],
) -> dict[str, Any]:
    reasons: list[str] = []
    gates: list[dict[str, Any]] = []

    candidate_rows = int(edge_metrics.get("rows", 0) or 0)
    candidate_ok = candidate_rows >= int(args.min_edge_candidates)
    gates.append(_gate_result("edge_candidates_generated", candidate_ok, candidate_rows, int(args.min_edge_candidates)))
    if not candidate_ok:
        reasons.append(f"edge candidates below threshold ({candidate_rows} < {args.min_edge_candidates})")

    expectancy_exists = bool(expectancy_payload.get("expectancy_exists", False))
    require_expectancy = bool(int(args.require_expectancy_exists))
    expectancy_gate_ok = expectancy_exists if require_expectancy else True
    gates.append(
        _gate_result(
            "expectancy_exists",
            expectancy_gate_ok,
            expectancy_exists,
            bool(require_expectancy),
            "from conditional_expectancy.json",
        )
    )
    if not expectancy_gate_ok:
        reasons.append("expectancy_exists is false")

    expectancy_evidence = expectancy_payload.get("expectancy_evidence", [])
    expectancy_evidence_count = len(expectancy_evidence) if isinstance(expectancy_evidence, list) else 0
    evidence_ok = expectancy_evidence_count >= int(args.min_expectancy_evidence)
    gates.append(
        _gate_result(
            "expectancy_evidence_count",
            evidence_ok,
            expectancy_evidence_count,
            int(args.min_expectancy_evidence),
        )
    )
    if not evidence_ok:
        reasons.append(
            f"expectancy evidence below threshold ({expectancy_evidence_count} < {args.min_expectancy_evidence})"
        )

    survivors = robustness_payload.get("survivors", [])
    survivor_count = len(survivors) if isinstance(survivors, list) else 0
    survivors_ok = survivor_count >= int(args.min_robust_survivors)
    gates.append(_gate_result("robust_survivor_count", survivors_ok, survivor_count, int(args.min_robust_survivors)))
    if not survivors_ok:
        reasons.append(f"robust survivors below threshold ({survivor_count} < {args.min_robust_survivors})")

    decision = "PROMOTE" if all(gate["passed"] for gate in gates) else "KEEP_RESEARCH"
    return {
        "run_id": run_id,
        "decision": decision,
        "gates": gates,
        "failure_reasons": reasons,
        "config": {
            "min_edge_candidates": int(args.min_edge_candidates),
            "min_expectancy_evidence": int(args.min_expectancy_evidence),
            "min_robust_survivors": int(args.min_robust_survivors),
            "require_expectancy_exists": bool(int(args.require_expectancy_exists)),
        },
        "metrics": {
            "edge_candidate_rows": candidate_rows,
            "edge_candidate_promoted": int(edge_metrics.get("promoted", 0) or 0),
            "expectancy_exists": expectancy_exists,
            "expectancy_evidence_count": expectancy_evidence_count,
            "robust_survivor_count": survivor_count,
        },
        "inputs": paths,
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Research Checklist",
        "",
        f"- Run ID: `{payload.get('run_id')}`",
        f"- Decision: **{payload.get('decision')}**",
        "",
        "## Gate Results",
        "",
    ]
    for gate in payload.get("gates", []):
        status = "PASS" if gate.get("passed") else "FAIL"
        lines.append(
            f"- **{gate.get('name')}**: {status} (observed={gate.get('observed')}, threshold={gate.get('threshold')})"
        )
        if gate.get("note"):
            lines.append(f"  note: {gate.get('note')}")

    lines.extend(["", "## Inputs", ""])
    for name, path in sorted((payload.get("inputs") or {}).items()):
        lines.append(f"- `{name}`: `{path}`")

    lines.extend(["", "## Metrics", ""])
    for name, value in sorted((payload.get("metrics") or {}).items()):
        lines.append(f"- `{name}`: `{value}`")

    reasons = payload.get("failure_reasons") or []
    lines.extend(["", "## Failure Reasons", ""])
    lines.extend([f"- {reason}" for reason in reasons] if reasons else ["- None"])

    return "\n".join(lines) + "\n"


def main() -> int:
    args = _parse_args()
    reports_root = Path(args.reports_root)
    edge_candidates_path = reports_root / "edge_candidates" / args.run_id / "edge_candidates_normalized.csv"
    expectancy_path = reports_root / "expectancy" / args.run_id / "conditional_expectancy.json"
    robustness_path = reports_root / "expectancy" / args.run_id / "conditional_expectancy_robustness.json"

    edge_metrics = _edge_candidate_metrics(edge_candidates_path)
    expectancy_payload = _read_json(expectancy_path)
    robustness_payload = _read_json(robustness_path)

    payload = _build_payload(
        run_id=args.run_id,
        args=args,
        edge_metrics=edge_metrics,
        expectancy_payload=expectancy_payload,
        robustness_payload=robustness_payload,
        paths={
            "edge_candidates": str(edge_candidates_path),
            "conditional_expectancy": str(expectancy_path),
            "conditional_expectancy_robustness": str(robustness_path),
        },
    )

    out_dir = Path(args.out_dir) if args.out_dir else Path(args.runs_root) / args.run_id / "research_checklist"
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / "checklist.json"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    md_path = out_dir / "checklist.md"
    md_path.write_text(_render_markdown(payload), encoding="utf-8")

    print(json.dumps({"decision": payload["decision"], "out_dir": str(out_dir)}, indent=2, sort_keys=True))
    return 0 if payload["decision"] == "PROMOTE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
