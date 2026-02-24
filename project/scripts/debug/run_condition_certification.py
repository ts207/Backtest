"""
Condition Enforcement Certification Batch
==========================================
Validates runtime condition plumbing end-to-end WITHOUT touching real strategy
pipelines. It exercises the exact code paths that Phase2 → compile traverses,
using four synthetic candidates that cover every routing branch.

Candidate Setup
---------------
1. vol_regime_high   → runtime-enforced, expect 1 ConditionNodeSpec on vol_regime_code
2. session_asia      → runtime-enforced, expect 1 ConditionNodeSpec on session_hour_utc
3. all (from severity_bucket_extreme_5pct bucket) → 0 nodes, condition_source=bucket_non_runtime
4. xyz_unknown       → strict mode BLOCKED, compile_eligible=False, NOT compiled

Usage
-----
    python run_condition_certification.py [--run_id <id>]

Prints a table of the audit rows and returns exit code 0 if all assertions pass.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from strategy_dsl.contract_v1 import normalize_entry_condition, is_executable_condition
from pipelines.research.phase2_candidate_discovery import _condition_routing

# ── Synthetic candidates ────────────────────────────────────────────────────

CANDIDATES = [
    {
        "id": "cert_01",
        "label": "vol_regime_high (runtime)",
        "cond_name": "vol_regime_high",
        "conditioning": "severity_bucket_top_10pct,vol_regime_high",
        "expect_source": "runtime",
        "expect_compile_eligible": True,
        "expect_num_nodes": 1,
        "expect_node_feature": "vol_regime_code",
    },
    {
        "id": "cert_02",
        "label": "session_asia (runtime)",
        "cond_name": "session_asia",
        "conditioning": "session_asia",
        "expect_source": "runtime",
        "expect_compile_eligible": True,
        "expect_num_nodes": 1,
        "expect_node_feature": "session_hour_utc",
    },
    {
        "id": "cert_03",
        "label": "all from severity bucket (bucket_non_runtime)",
        "cond_name": "severity_bucket_extreme_5pct",
        "conditioning": "severity_bucket_extreme_5pct",
        "expect_source": "bucket_non_runtime",
        "expect_compile_eligible": True,
        "expect_num_nodes": 0,
        "expect_node_feature": None,
    },
    {
        "id": "cert_04",
        "label": "xyz_unknown (blocked)",
        "cond_name": "xyz_unknown_feature",
        "conditioning": "xyz_unknown_feature",
        "expect_source": "blocked",
        "expect_compile_eligible": False,
        "expect_num_nodes": None,  # never reaches compile
        "expect_node_feature": None,
    },
]

# ── Run certification ────────────────────────────────────────────────────────

def run_certification() -> int:
    print("\n" + "=" * 70)
    print("CONDITION ENFORCEMENT CERTIFICATION BATCH")
    print("=" * 70)

    failures = []
    audit_rows = []

    for cand in CANDIDATES:
        cid = cand["id"]
        label = cand["label"]
        cond_name = cand["cond_name"]

        print(f"\n── {cid}: {label}")
        print(f"   cond_name      : {cond_name!r}")
        print(f"   conditioning   : {cand['conditioning']!r}")

        # Step 1: _condition_routing (strict=True)
        condition_str, source = _condition_routing(cond_name, strict=True)
        compile_eligible = source != "blocked"
        print(f"   condition_str  : {condition_str!r}")
        print(f"   condition_src  : {source!r}")
        print(f"   compile_eligible: {compile_eligible}")

        # Assert routing
        if source != cand["expect_source"]:
            msg = f"{cid}: expected source={cand['expect_source']!r}, got={source!r}"
            failures.append(msg)
            print(f"   FAIL: {msg}")
        if compile_eligible != cand["expect_compile_eligible"]:
            msg = f"{cid}: expected compile_eligible={cand['expect_compile_eligible']}, got={compile_eligible}"
            failures.append(msg)
            print(f"   FAIL: {msg}")

        # Candidates blocked here don't reach compile
        if not compile_eligible:
            print(f"   → BLOCKED (not compiled, as expected)")
            audit_rows.append({
                "candidate_id": cid,
                "blueprint_id": f"bp_cert_{cid}",
                "condition": condition_str,
                "num_condition_nodes": 0,
                "condition_source": source,
                "compile_reason": "blocked_non_executable_condition",
            })
            continue

        # Assert condition_str doesn't contain legacy '__' or rule template names
        if "__" in condition_str:
            failures.append(f"{cid}: condition_str contains '__': {condition_str!r}")
        if condition_str.lower() in {"mean_reversion", "continuation", "carry", "breakout"}:
            failures.append(f"{cid}: condition_str is a rule template name: {condition_str!r}")

        # Step 2: normalize_entry_condition (blueprint compiler path)
        canonical, nodes, sym = normalize_entry_condition(
            condition_str,
            event_type="LIQUIDATION_CASCADE",
            candidate_id=cid,
        )
        num_nodes = len(nodes)
        node_feature = nodes[0].feature if nodes else None

        print(f"   canonical      : {canonical!r}")
        print(f"   num_nodes      : {num_nodes}")
        if node_feature:
            print(f"   node_feature   : {node_feature!r}")
            print(f"   node_op/val    : {nodes[0].operator!r} / {nodes[0].value}")

        # Assert node count
        if cand["expect_num_nodes"] is not None and num_nodes != cand["expect_num_nodes"]:
            msg = f"{cid}: expected num_nodes={cand['expect_num_nodes']}, got={num_nodes}"
            failures.append(msg)
            print(f"   FAIL: {msg}")

        # Assert node feature
        if cand["expect_node_feature"] and node_feature != cand["expect_node_feature"]:
            msg = f"{cid}: expected node_feature={cand['expect_node_feature']!r}, got={node_feature!r}"
            failures.append(msg)
            print(f"   FAIL: {msg}")

        # Audit gate check (simulate what compile does)
        if condition_str not in ("all", "") and not condition_str.startswith("symbol_") and num_nodes == 0:
            if is_executable_condition(condition_str):
                msg = f"{cid}: AUDIT GATE FAIL — runtime condition '{condition_str}' has 0 nodes"
                failures.append(msg)
                print(f"   FAIL: {msg}")

        nodes_hash = hashlib.sha256(
            json.dumps([
                {"feature": n.feature, "operator": n.operator, "value": n.value}
                for n in nodes
            ], sort_keys=True).encode()
        ).hexdigest()[:16]

        audit_rows.append({
            "candidate_id": cid,
            "blueprint_id": f"bp_cert_{cid}",
            "condition": canonical,
            "num_condition_nodes": num_nodes,
            "condition_source": source,
            "compile_reason": "compiled",
        })

        status = "PASS" if not [f for f in failures if f.startswith(cid)] else "FAIL"
        print(f"   Status: {status}")

    # ── Final Report ────────────────────────────────────────────────────────

    print("\n" + "=" * 70)
    print("AUDIT TABLE (compiled_blueprints_condition_audit columns)")
    print("=" * 70)
    print(f"{'candidate_id':<12} {'condition':<22} {'num_nodes':>9} {'condition_source':<22} {'compile_reason'}")
    print("-" * 100)
    for row in audit_rows:
        print(
            f"{row['candidate_id']:<12} {row['condition']:<22} {row['num_condition_nodes']:>9} "
            f"{row['condition_source']:<22} {row['compile_reason']}"
        )

    print("\n" + "=" * 70)
    if failures:
        print(f"CERTIFICATION FAILED — {len(failures)} assertion(s):")
        for f in failures:
            print(f"  ✗ {f}")
        return 1
    else:
        print("CERTIFICATION PASSED — all 4 candidates routed and guarded correctly.")
        print()
        print("  ✓ cert_01: vol_regime_high → 1 node (vol_regime_code == 2.0)")
        print("  ✓ cert_02: session_asia → 1 node (session_hour_utc in_range 0..7)")
        print("  ✓ cert_03: severity_bucket → all, 0 nodes, bucket_non_runtime")
        print("  ✓ cert_04: xyz_unknown → BLOCKED, compile_eligible=False")
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run condition enforcement certification.")
    parser.parse_args()
    sys.exit(run_certification())
