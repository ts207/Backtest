"""
Generate research_backlog.csv from knowledge_atlas.json.

Applies the operationalization checklist (A–E) from complete-plan.md.txt:
  A) Event trigger or feature definition present
  B) Feature inputs & PIT windowing
  C) Label / horizon
  D) Test protocol (slices + multiplicity family)
  E) Cost / capacity model

Each claim gets:
  - candidate_type   : event | feature | label | evaluation | execution
  - operationalizable: Y / N / PARTIAL
  - missing          : comma-separated missing checklist items
  - next_artifact    : the concrete spec file to produce next
  - priority_score   : 1–15 (impact 1-5 × feasibility 1-5 × novelty 1-5, capped)
  - target_gate      : D0 | E-1 | V1 | Bridge | S | P
  - evidence_locator : from first evidence fragment
"""

import csv
import json
import re
import sys
from pathlib import Path

ATLAS_PATH = Path(__file__).parents[2] / "knowledge_atlas.json"
OUTPUT_PATH = Path(__file__).parents[2] / "research_backlog.csv"

# ---------------------------------------------------------------------------
# Concept → candidate_type mapping
# ---------------------------------------------------------------------------
CONCEPT_TYPE_MAP = {
    "C_EVENT_REGISTRY": "event",
    "C_EVENT_DEFINITIONS": "event",
    "C_VOLATILITY_STATE_TRANSITIONS": "event",
    "C_TREND_EXHAUSTION": "event",
    "C_LIQUIDITY_MEAN_REVERSION": "event",
    "C_SESSION_MICROSTRUCTURE": "feature",
    "C_MICROSTRUCTURE_METRICS": "feature",
    "C_CONTEXT_DELTAS": "feature",
    "C_CONTEXT_INTERACTIONS": "feature",
    "C_EXECUTION_COST_MODEL": "execution",
    "C_ML_TRADING_MODELS": "evaluation",
    "C_MARKET_EFFICIENCY": "evaluation",
    "C_MULTIPLICITY_CONTROL": "evaluation",
    "C_VALIDATION": "evaluation",
    "C_STRATEGY_BACKTEST": "evaluation",
    "C_PORTFOLIO": "evaluation",
    "C_LITERATURE_REVIEW": "evaluation",
    "C_DATA_CONTRACTS": "execution",
    "C_DATA_SCHEMA": "execution",
    "C_INVARIANTS": "evaluation",
    "C_PROVENANCE_REPLAY": "execution",
}

# candidate_type → target gate
GATE_MAP = {
    "event": "E-1",
    "feature": "V1",
    "label": "V1",
    "evaluation": "V1",
    "execution": "Bridge",
}

# candidate_type → next_artifact template
ARTIFACT_MAP = {
    "event": "spec/events/{event_type}.yaml",
    "feature": "spec/features/{feature_name}.yaml",
    "label": "spec/features/labels.yaml",
    "evaluation": "spec/tests/{test_id}.yaml",
    "execution": "spec/concepts/C_EXECUTION_COST_MODEL.yaml",
}

# Research-relevant concept IDs (prioritize these)
HIGH_PRIORITY_CONCEPTS = {
    "C_MICROSTRUCTURE_METRICS",
    "C_VOLATILITY_STATE_TRANSITIONS",
    "C_LIQUIDITY_MEAN_REVERSION",
    "C_TREND_EXHAUSTION",
    "C_SESSION_MICROSTRUCTURE",
    "C_EVENT_REGISTRY",
    "C_EVENT_DEFINITIONS",
}

# claim_type → novelty score
NOVELTY_MAP = {"mechanistic": 5, "heuristic": 3, "empirical": 2}

# Patterns that indicate a claim is a tool reference / URL / non-operational
TOOL_REF_PATTERNS = [
    r"https?://",
    r"github\.com/",
    r"\[[\w\s]+\]\(https?://",  # markdown links
    r"^\s*-\s*\[",               # markdown list item with link
]

TOOL_REF_RE = re.compile("|".join(TOOL_REF_PATTERNS))

# Words that suggest operationalizability
OPERATIONAL_KEYWORDS = {
    "threshold", "percentile", "z-score", "window", "lookback", "cooldown",
    "trigger", "signal", "condition", "regime", "spread", "depth", "imbalance",
    "funding", "basis", "vpin", "amihud", "roll", "oi", "liquidat", "volatil",
    "compression", "breakout", "reversion", "momentum", "carry", "basis",
    "forward return", "fwd_ret", "horizon", "5m", "15m", "1h", "4h", "24h",
    "event study", "slice", "fdr", "bh ", "multiplicity", "cost sweep",
    "slippage", "impact", "participation", "capacity",
}


def is_tool_reference(statement: str) -> bool:
    return bool(TOOL_REF_RE.search(statement))


def has_operational_content(statement: str) -> bool:
    s = statement.lower()
    return any(kw in s for kw in OPERATIONAL_KEYWORDS)


def infer_candidate_type(claim: dict) -> str:
    concept_id = claim.get("concept_id", "")
    if concept_id in CONCEPT_TYPE_MAP:
        return CONCEPT_TYPE_MAP[concept_id]
    statement = claim.get("statement", "").lower()
    if any(w in statement for w in ("event", "trigger", "cascade", "shock", "extreme", "spike")):
        return "event"
    if any(w in statement for w in ("feature", "spread", "depth", "vpin", "amihud", "roll")):
        return "feature"
    if any(w in statement for w in ("label", "horizon", "forward", "fwd_ret", "return")):
        return "label"
    if any(w in statement for w in ("cost", "slippage", "fill", "execution", "latency")):
        return "execution"
    return "evaluation"


def compute_operationalizable(claim: dict) -> tuple[str, list[str]]:
    """Returns (operationalizable, missing_items).

    Checklist A–E from complete-plan.md:
      A) Event trigger or feature definition at t0
      B) Feature inputs + PIT windowing
      C) Label / horizon
      D) Test protocol — satisfied by project default (spec/multiplicity/families.yaml)
      E) Cost/capacity — satisfied by project default (configs/fees.yaml)

    D and E have project-level defaults; they appear in missing[] for tracking
    but do NOT block Y.  Only A+B+C determine Y/PARTIAL/N.
    """
    op = claim.get("operationalization", {})
    features = op.get("features", [])
    label = op.get("label", "")
    statement = claim.get("statement", "")

    missing = []

    # A) Event trigger / feature definition
    has_a = bool(features) or has_operational_content(statement)
    if not has_a:
        missing.append("definition")

    # B) PIT windowing
    if not features:
        missing.append("PIT")

    # C) Label / horizon
    if not label:
        missing.append("label")

    # D) Test protocol — default exists; flag as "needs_split_spec" for info
    # E) Cost model — default exists; flag as "needs_cost_spec" for info
    # (these do not count against Y/PARTIAL/N)

    # Determine operationalizable flag.
    # Atlas claims are source fragments; labels come from experiment design, not
    # from the claim itself.  For event/feature/execution candidates label is
    # always in missing[] for tracking but does NOT block Y.
    # Only "definition" (A) and "PIT" (B) block Y for those types.
    ctype = infer_candidate_type(claim)
    if ctype in ("label", "evaluation"):
        # For label/eval claims the label field is the key check
        blocking = [m for m in missing if m in ("definition", "label")]
    else:
        blocking = [m for m in missing if m in ("definition", "PIT")]

    if not blocking:
        return "Y", missing
    if "definition" in blocking:
        return "N", missing
    return "PARTIAL", missing


def compute_priority(claim: dict, candidate_type: str) -> int:
    concept_id = claim.get("concept_id", "")
    claim_type = claim.get("claim_type", "empirical")
    op = claim.get("operationalization", {})
    features = op.get("features", [])

    # Impact: 1-5
    if concept_id in HIGH_PRIORITY_CONCEPTS:
        impact = 5
    elif candidate_type in ("event", "feature"):
        impact = 4
    elif candidate_type == "execution":
        impact = 3
    else:
        impact = 2

    # Feasibility: 1-5
    if features and has_operational_content(claim.get("statement", "")):
        feasibility = 5
    elif features or has_operational_content(claim.get("statement", "")):
        feasibility = 3
    else:
        feasibility = 1

    # Novelty: based on claim_type
    novelty = NOVELTY_MAP.get(claim_type, 2)

    return impact + feasibility + novelty  # 3–15 range


def next_artifact(candidate_type: str) -> str:
    return ARTIFACT_MAP.get(candidate_type, "spec/tests/test_spec.yaml")


def evidence_locator(claim: dict) -> str:
    evidence = claim.get("evidence", [])
    if evidence:
        e = evidence[0]
        return e.get("locator", "")
    return ""


def source_id(claim: dict) -> str:
    evidence = claim.get("evidence", [])
    if evidence:
        return evidence[0].get("source_id", "")
    return ""


FIELDNAMES = [
    "claim_id",
    "source_id",
    "concept_id",
    "candidate_type",
    "claim_type",
    "operationalizable",
    "missing",
    "priority_score",
    "target_gate",
    "next_artifact",
    "evidence_locator",
    "assets",
    "horizon",
    "stage",
    "features",
    "label",
    "status",
    "statement_summary",
]


def truncate(s: str, n: int = 200) -> str:
    return s[:n].replace("\n", " ").strip() if s else ""


def main():
    print(f"Loading atlas from {ATLAS_PATH} ...")
    with open(ATLAS_PATH, "r", encoding="utf-8") as f:
        atlas = json.load(f)

    claims = atlas.get("claims", [])
    print(f"  {len(claims)} claims found.")

    rows = []
    skipped_tool_refs = 0
    skipped_empty = 0

    for claim in claims:
        statement = claim.get("statement", "").strip()

        # Skip empty statements
        if not statement:
            skipped_empty += 1
            continue

        # Skip tool-reference / URL-only claims
        if is_tool_reference(statement) and not has_operational_content(statement):
            skipped_tool_refs += 1
            continue

        concept_id = claim.get("concept_id", "")
        ctype = infer_candidate_type(claim)
        op_flag, missing = compute_operationalizable(claim)
        priority = compute_priority(claim, ctype)
        gate = GATE_MAP.get(ctype, "V1")
        artifact = next_artifact(ctype)
        locator = evidence_locator(claim)
        src = source_id(claim)

        op = claim.get("operationalization", {})
        scope = claim.get("scope", {})

        rows.append({
            "claim_id": claim.get("claim_id", ""),
            "source_id": src,
            "concept_id": concept_id,
            "candidate_type": ctype,
            "claim_type": claim.get("claim_type", ""),
            "operationalizable": op_flag,
            "missing": "|".join(missing),
            "priority_score": priority,
            "target_gate": gate,
            "next_artifact": artifact,
            "evidence_locator": locator,
            "assets": "|".join(scope.get("assets", ["*"])),
            "horizon": scope.get("horizon", "*"),
            "stage": scope.get("stage", ""),
            "features": "|".join(op.get("features", [])),
            "label": op.get("label", ""),
            "status": claim.get("status", "unverified"),
            "statement_summary": truncate(statement, 200),
        })

    # Sort: highest priority first, then by operationalizable (Y > PARTIAL > N)
    op_order = {"Y": 0, "PARTIAL": 1, "N": 2}
    rows.sort(key=lambda r: (-r["priority_score"], op_order.get(r["operationalizable"], 3)))

    print(f"  Skipped {skipped_tool_refs} tool-reference claims.")
    print(f"  Skipped {skipped_empty} empty-statement claims.")
    print(f"  Writing {len(rows)} rows to {OUTPUT_PATH} ...")

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    # Summary stats
    y = sum(1 for r in rows if r["operationalizable"] == "Y")
    partial = sum(1 for r in rows if r["operationalizable"] == "PARTIAL")
    n = sum(1 for r in rows if r["operationalizable"] == "N")

    by_type = {}
    for r in rows:
        by_type[r["candidate_type"]] = by_type.get(r["candidate_type"], 0) + 1

    print("\n=== Research Backlog Summary ===")
    print(f"  Total rows   : {len(rows)}")
    print(f"  Operationalizable Y      : {y}")
    print(f"  Operationalizable PARTIAL: {partial}")
    print(f"  Operationalizable N      : {n}")
    print("\n  By candidate_type:")
    for t, cnt in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f"    {t:15s}: {cnt}")
    print(f"\n  Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    sys.exit(main())
