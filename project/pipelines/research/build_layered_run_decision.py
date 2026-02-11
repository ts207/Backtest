from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _data_root() -> Path:
    return Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))


def _read_json(path: Path) -> Dict[str, object] | List[object] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _collect_research_layer(run_id: str) -> Dict[str, object]:
    phase2_root = _data_root() / "reports" / "phase2" / run_id
    promoted_paths = sorted(phase2_root.glob("*/promoted_candidates.json")) if phase2_root.exists() else []

    if not promoted_paths:
        return {
            "decision": "RESEARCH_INCONCLUSIVE",
            "promoted_candidate_count": 0,
            "event_family_count": 0,
            "event_families": [],
            "notes": [
                "No phase2 promoted_candidates.json artifacts found under data/reports/phase2/<run_id>.",
                "Research-layer promotion status cannot be globally inferred from strategy-layer outputs only.",
            ],
        }

    promoted_total = 0
    families: List[Dict[str, object]] = []

    for p in promoted_paths:
        payload = _read_json(p)
        if isinstance(payload, dict):
            candidates = payload.get("candidates", [])
        else:
            candidates = payload or []
        count = len(candidates) if isinstance(candidates, list) else 0
        promoted_total += count
        families.append(
            {
                "event_type": p.parent.name,
                "promoted_candidates": count,
                "source_path": str(p),
            }
        )

    decision = "RESEARCH_PROMOTIONS_PRESENT" if promoted_total > 0 else "RESEARCH_EMPTY"

    return {
        "decision": decision,
        "promoted_candidate_count": int(promoted_total),
        "event_family_count": int(len(families)),
        "event_families": families,
        "notes": [
            "Research-layer decision is event-family based (phase2 promoted candidates).",
            "This is distinct from any one strategy/overlay backtest checklist.",
        ],
    }


def _collect_strategy_layer(run_id: str) -> Dict[str, object]:
    checklist_path = _data_root() / "runs" / run_id / "research_checklist" / "checklist.json"
    payload = _read_json(checklist_path)

    decisions: List[Dict[str, object]] = []
    if isinstance(payload, dict):
        raw_decision = str(payload.get("decision", "UNKNOWN"))
        decisions.append(
            {
                "strategy": "vol_compression_expansion_v1",
                "decision": f"VOL_COMPRESSION_{raw_decision}",
                "source_path": str(checklist_path),
                "failure_reasons": payload.get("failure_reasons", []),
            }
        )

    return {
        "strategy_count": len(decisions),
        "decisions": decisions,
        "notes": [
            "Strategy-layer decisions are per implementation (overlay/entry-exit mapping).",
        ],
    }


def _global_assessment(research: Dict[str, object], strategy: Dict[str, object]) -> Tuple[str, List[str]]:
    notes: List[str] = []
    strategy_count = int(strategy.get("strategy_count", 0) or 0)
    research_decision = str(research.get("decision", "RESEARCH_INCONCLUSIVE"))

    if strategy_count <= 1:
        notes.append("Only one strategy-layer checklist is available; global deployability is not assessed.")
        if research_decision == "RESEARCH_PROMOTIONS_PRESENT":
            notes.append("Research promotions exist, but they have not yet been mapped to multiple strategy-level audits.")
        return "GLOBAL_DEPLOYABILITY_NOT_ASSESSED_SINGLE_STRATEGY", notes

    # future-ready branch if/when multiple strategies are available
    strategy_decisions = [str(d.get("decision", "")) for d in strategy.get("decisions", []) if isinstance(d, dict)]
    if strategy_decisions and all(x.endswith("KEEP_RESEARCH") for x in strategy_decisions):
        return "GLOBAL_KEEP_RESEARCH", notes
    return "GLOBAL_MIXED", notes


def build_layered_decision(run_id: str, out_dir: Path | None = None) -> Dict[str, object]:
    out = out_dir or (_data_root() / "reports" / "run_decisions" / run_id)
    out.mkdir(parents=True, exist_ok=True)

    research = _collect_research_layer(run_id)
    strategy = _collect_strategy_layer(run_id)
    global_decision, global_notes = _global_assessment(research, strategy)

    payload: Dict[str, object] = {
        "run_id": run_id,
        "research_layer": research,
        "strategy_layer": strategy,
        "global_decision": global_decision,
        "global_notes": global_notes,
    }

    json_path = out / "layered_decision.json"
    md_path = out / "layered_decision.md"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# Layered Run Decision",
        "",
        f"Run ID: `{run_id}`",
        "",
        "## Global",
        "",
        f"- decision: **{global_decision}**",
    ]
    for note in global_notes:
        lines.append(f"- note: {note}")

    lines.extend(
        [
            "",
            "## Research layer (event-level)",
            "",
            f"- decision: **{research.get('decision')}**",
            f"- promoted candidates: {research.get('promoted_candidate_count')}",
            f"- event families: {research.get('event_family_count')}",
            "",
            "## Strategy layer (per strategy)",
            "",
            f"- strategy count: {strategy.get('strategy_count')}",
        ]
    )
    for dec in strategy.get("decisions", []):
        if isinstance(dec, dict):
            lines.append(f"- {dec.get('strategy')}: {dec.get('decision')}")
    lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")

    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Build layered run decision separating research and strategy outcomes")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--out_dir", default=None)
    args = parser.parse_args()

    payload = build_layered_decision(args.run_id, Path(args.out_dir) if args.out_dir else None)
    print(json.dumps({"status": "ok", "run_id": args.run_id, "global_decision": payload["global_decision"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
