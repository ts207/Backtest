from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir, read_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.spec_utils import get_spec_hashes
from pipelines.research.evaluate_naive_entry import _condition_mask, _load_phase1_events

def _calculate_jaccard(set_a: set, set_b: set) -> float:
    if not set_a and not set_b:
        return 1.0
    intersection = len(set_a.intersection(set_b))
    union = len(set_a.union(set_b))
    return float(intersection / union) if union > 0 else 0.0

def main() -> int:
    parser = argparse.ArgumentParser(description="Deduplicate survivors and initialize promotion ledger")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--verbose", action="store_true", help="Emit per-candidate debug output")
    args = parser.parse_args()

    run_dir = DATA_ROOT / "runs" / args.run_id
    bridge_queue_path = run_dir / "bridge_queue.parquet"
    if not bridge_queue_path.exists():
        print(f"Bridge queue not found: {bridge_queue_path}", file=sys.stderr)
        return 1

    params = {"run_id": args.run_id}
    manifest = start_manifest("deduplicate_survivors", args.run_id, params, [], [])

    try:
        df = pd.read_parquet(bridge_queue_path)
        
        # Add duplicate_of column
        df["duplicate_of"] = None
        deduped_rows = []
        cluster_records = [] # List of {cluster_id, members, representative}
        
        # Group by (event_type, rule_template, horizon, sign)
        # To identify cross-symbol redundancy or parameter-neighborhood variations
        group_cols = ["event_type", "rule_template", "horizon", "sign"]
        grouped = df.groupby(group_cols)
        
        cluster_id_counter = 0
        
        # Cache events to avoid reloading
        events_cache = {}

        for name, group in grouped:
            # Sort by rank metric
            candidates = group.to_dict("records")
            candidates.sort(key=lambda x: (
                x.get("gate_bridge_tradable", False),
                x.get("after_cost_expectancy_per_trade", 0.0),
                x.get("robustness_score", 0.0)
            ), reverse=True)
            
            event_type = name[0]
            if event_type not in events_cache:
                try:
                    events_cache[event_type] = _load_phase1_events(args.run_id, event_type)
                except Exception as e:
                    print(f"Warning: Could not load events for {event_type}: {e}")
                    deduped_rows.extend(candidates)
                    continue
            
            base_events = events_cache[event_type]
            
            # Compute event sets for each candidate
            candidate_sets = []
            for cand in candidates:
                symbol = cand.get("symbol")
                sym_events = base_events[base_events["symbol"] == symbol]
                mask = _condition_mask(sym_events, cand.get("condition", "all"))
                if "event_id" in sym_events.columns:
                    ids = set(sym_events[mask]["event_id"])
                else:
                    ids = set(sym_events[mask].index)
                if args.verbose:
                    print(f"Candidate {cand['candidate_id']} has {len(ids)} events.")
                candidate_sets.append(ids)
            
            # Greedy deduplication
            kept = []
            kept_sets = []
            
            for i, cand in enumerate(candidates):
                is_dup = False
                my_set = candidate_sets[i]
                
                for j, kept_cand in enumerate(kept):
                    kept_set = kept_sets[j]
                    overlap = _calculate_jaccard(my_set, kept_set)
                    if overlap > 0.8:
                        cand["duplicate_of"] = kept_cand["candidate_id"]
                        is_dup = True
                        
                        # Record cluster member
                        cluster_records.append({
                            "cluster_id": f"cluster_{cluster_id_counter}",
                            "candidate_id": cand["candidate_id"],
                            "is_representative": False,
                            "duplicate_of": kept_cand["candidate_id"],
                            "overlap_score": overlap
                        })
                        break
                
                if not is_dup:
                    kept.append(cand)
                    kept_sets.append(my_set)
                    deduped_rows.append(cand)
                    
                    # New cluster
                    cluster_id_counter += 1
                    cluster_records.append({
                        "cluster_id": f"cluster_{cluster_id_counter}",
                        "candidate_id": cand["candidate_id"],
                        "is_representative": True,
                        "duplicate_of": None,
                        "overlap_score": 1.0
                    })
            
        deduped = pd.DataFrame(deduped_rows)

        # Output survivors
        out_path = run_dir / "survivors_deduped.parquet"
        deduped.to_parquet(out_path)
        
        # Output cluster diagnostic
        if cluster_records:
            clusters_df = pd.DataFrame(cluster_records)
            clusters_df.to_parquet(run_dir / "dedup_clusters.parquet")

        # Initialize Promotion Ledger — derive gate flags from actual columns.
        ledger = deduped.copy()
        # gate_phase2_final is the authoritative Phase 2 pass column written by
        # phase2_candidate_discovery.  Fall back to gate_economic_conservative if
        # the column was produced under an older schema.
        if "gate_phase2_final" in ledger.columns:
            ledger["phase2_pass"] = ledger["gate_phase2_final"].astype(bool)
        elif "gate_economic_conservative" in ledger.columns:
            ledger["phase2_pass"] = ledger["gate_economic_conservative"].astype(bool)
        else:
            ledger["phase2_pass"] = False  # unknown → conservative default

        if "gate_bridge_tradable" in ledger.columns:
            ledger["bridge_pass"] = ledger["gate_bridge_tradable"].astype(bool)
        else:
            ledger["bridge_pass"] = False  # bridge has not run yet

        ledger["blueprint_compiled"] = False
        ledger["stress_pass"] = False
        ledger["walkforward_pass"] = False

        # Hash the spec directory that is the source of truth for this run.
        spec_hashes = get_spec_hashes(PROJECT_ROOT)
        ledger["spec_version"] = str(spec_hashes)
        
        ledger_path = run_dir / "promotion_ledger.parquet"
        ledger.to_parquet(ledger_path)

        finalize_manifest(manifest, "success", stats={
            "input_candidates": len(df),
            "deduped_candidates": len(deduped)
        })
        print(f"Deduped survivors: {len(deduped)} (from {len(df)})")
        return 0
        
    except Exception as exc:
        print(f"Deduplication failed with error: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1

if __name__ == "__main__":
    sys.exit(main())
