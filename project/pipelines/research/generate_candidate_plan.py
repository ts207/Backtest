from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir, read_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from events.registry import EVENT_REGISTRY_SPECS

# Planner Budgets
MAX_TOTAL_CANDIDATES = 200
MAX_CANDIDATES_PER_CLAIM = 20
MAX_SYMBOLS_PER_TEMPLATE = 10
MAX_CONDITIONING_VARIANTS = 6
MAX_HORIZONS_PER_TEMPLATE = 3

def _check_spec_exists(path_str: str) -> bool:
    return (PROJECT_ROOT.parent / path_str).exists()

def _check_dataset_exists(symbol: str, run_id: str) -> bool:
    # Basic check for OHLCV 5m data
    path = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "bars_5m"
    run_scoped = DATA_ROOT / "lake" / "runs" / run_id / "cleaned" / "perp" / symbol / "bars_5m"
    return path.exists() or run_scoped.exists()

def _check_market_context_exists(symbol: str, run_id: str) -> bool:
    path = DATA_ROOT / "lake" / "context" / "market_state" / symbol / "5m.parquet"
    run_scoped = DATA_ROOT / "lake" / "runs" / run_id / "context" / "market_state" / symbol / "5m.parquet"
    return path.exists() or run_scoped.exists()

def _match_assets(filter_str: str, universe: List[str]) -> List[str]:
    if not filter_str or filter_str == "*":
        return universe
    filters = [f.strip().upper() for f in filter_str.split("|") if f.strip()]
    return [s for s in universe if any(f in s for f in filters)]

def main() -> int:
    parser = argparse.ArgumentParser(description="Knowledge Atlas: Run-time Candidate Plan Enumerator")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True, help="Symbols to consider for this run")
    parser.add_argument("--atlas_dir", default="atlas")
    parser.add_argument("--out_dir", default=None)
    args = parser.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "hypothesis_generator" / args.run_id
    ensure_dir(out_dir)

    run_symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    
    params = vars(args)
    params["budgets"] = {
        "max_total": MAX_TOTAL_CANDIDATES,
        "max_per_claim": MAX_CANDIDATES_PER_CLAIM,
        "max_symbols": MAX_SYMBOLS_PER_TEMPLATE
    }
    
    manifest = start_manifest("generate_candidate_plan", args.run_id, params, [], [])

    try:
        atlas_dir = PROJECT_ROOT.parent / args.atlas_dir
        templates_path = atlas_dir / "candidate_templates.parquet"
        if not templates_path.exists():
            raise FileNotFoundError(f"Candidate templates not found: {templates_path}")
        
        templates_df = pd.read_parquet(templates_path)
        
        plan_rows = []
        seen_plan_ids = set()
        plan_duplicates = 0
        feasibility_report = []
        total_count = 0
        # Load Atlas Exclusions
        exclusions_path = DATA_ROOT / "reports" / "atlas_verification" / args.run_id / "planner_excluded_claims.json"
        excluded_claims = set()
        
        exclusions_hash = None
        exclusions_count = 0
        
        if exclusions_path.exists():
            try:
                excl_bytes = exclusions_path.read_bytes()
                ex_data = json.loads(excl_bytes.decode("utf-8"))
                for e in ex_data:
                    if "claim_id" in e:
                        excluded_claims.add(e["claim_id"])
                
                exclusions_hash = "sha256:" + hashlib.sha256(excl_bytes).hexdigest()
                exclusions_count = len(excluded_claims)
            except Exception as e:
                logging.getLogger(__name__).warning(f"Failed to read exclusions file: {e}")
                # User requested: Fail closed if exclusions exist but cannot be parsed.
                raise ValueError(f"Failed to parse planner_excluded_claims.json: {e}")


        # Templates are already sorted by priority from Stage 1
        for _, row in templates_df.iterrows():
            claim_id = row['source_claim_id']
            template_id = row['template_id']
            
            # Exclusion Check
            if claim_id in excluded_claims:
                feasibility_report.append({
                    "template_id": template_id,
                    "claim_id": claim_id,
                    "status": "blocked_non_executable_condition",
                    "reason": "Atlas claim tagged as non-executable (rolling blocked limit)"
                })
                continue

            # Feasibility Checks
            status = "ready"
            reason = ""
            
            # 1. Spec Check
            if not _check_spec_exists(row['target_spec_path']):
                status = "blocked_missing_spec"
                reason = f"spec missing at {row['target_spec_path']}"
            
            # 2. Registry Check
            event_type = row.get('event_type')
            if status == "ready" and event_type and event_type not in EVENT_REGISTRY_SPECS:
                status = "blocked_missing_registry"
                reason = f"event_type {event_type} not in registry"
            
            # 3. Asset Filtering
            eligible_symbols = _match_assets(row['assets_filter'], run_symbols)
            if status == "ready" and not eligible_symbols:
                status = "blocked_asset_mismatch"
                reason = f"no symbols match asset filter {row['assets_filter']}"
            
            if status != "ready":
                feasibility_report.append({
                    "template_id": template_id,
                    "claim_id": claim_id,
                    "status": status,
                    "reason": reason
                })
                continue

            eligible_symbols = eligible_symbols[:MAX_SYMBOLS_PER_TEMPLATE]
            claim_candidates = 0
            
            for symbol in eligible_symbols:
                if total_count >= MAX_TOTAL_CANDIDATES or claim_candidates >= MAX_CANDIDATES_PER_CLAIM:
                    break
                
                # 4. Dataset check
                if not _check_dataset_exists(symbol, args.run_id):
                    feasibility_report.append({
                        "template_id": template_id,
                        "symbol": symbol,
                        "status": "blocked_missing_dataset",
                        "reason": f"OHLCV 5m missing for {symbol}"
                    })
                    continue
                
                for rule in row['rule_templates']:
                    for horizon in row['horizons'][:MAX_HORIZONS_PER_TEMPLATE]:
                        # conditioning variants
                        cond_config = row['conditioning']
                        
                        # Add "all" (base) variant
                        plan_row_id = f"{claim_id}:{event_type or row.get('feature_name')}:{rule}:{horizon}:all:{symbol}"
                        
                        if plan_row_id in seen_plan_ids:
                            plan_duplicates += 1
                        else:
                            seen_plan_ids.add(plan_row_id)
                            plan_row = {
                                "plan_row_id": plan_row_id,
                                "source_claim_ids": [claim_id],
                                "source_concept_ids": [row['concept_id']],
                                "event_type": event_type or "microstructure_proxy",
                                "rule_template": rule,
                                "horizon": horizon,
                                "symbol": symbol,
                                "conditioning": {},
                                "min_events": row['min_events']
                            }
                            plan_rows.append(plan_row)
                            total_count += 1
                            claim_candidates += 1
                        
                        # Add buckets
                        for c_key, c_vals in cond_config.items():
                            if c_key == "vol_regime" and not _check_market_context_exists(symbol, args.run_id):
                                feasibility_report.append({
                                    "template_id": template_id,
                                    "symbol": symbol,
                                    "status": "blocked_missing_dataset",
                                    "reason": f"market_context missing for vol_regime on {symbol}"
                                })
                                continue
                                
                            for c_val in c_vals[:MAX_CONDITIONING_VARIANTS]:
                                if total_count >= MAX_TOTAL_CANDIDATES or claim_candidates >= MAX_CANDIDATES_PER_CLAIM:
                                    break
                                
                                plan_row_id = f"{claim_id}:{event_type or row.get('feature_name')}:{rule}:{horizon}:{c_key}_{c_val}:{symbol}"
                                if plan_row_id in seen_plan_ids:
                                    plan_duplicates += 1
                                else:
                                    seen_plan_ids.add(plan_row_id)
                                    plan_row = {
                                        "plan_row_id": plan_row_id,
                                        "source_claim_ids": [claim_id],
                                        "source_concept_ids": [row['concept_id']],
                                        "event_type": event_type or "microstructure_proxy",
                                        "rule_template": rule,
                                        "horizon": horizon,
                                        "symbol": symbol,
                                        "conditioning": {c_key: c_val},
                                        "min_events": row['min_events']
                                    }
                                    plan_rows.append(plan_row)
                                    total_count += 1
                                    claim_candidates += 1
            
            feasibility_report.append({
                "template_id": template_id,
                "claim_id": claim_id,
                "status": "ready",
                "candidates_enumerated": claim_candidates
            })

        # Assert and abort if duplicates generated according to new rules
        if plan_duplicates > 0:
            raise ValueError(f"Plan duplicate count > 0: detected {plan_duplicates} duplicated candidate configurations.")

        # Write Plan
        plan_path = out_dir / "candidate_plan.jsonl"
        with plan_path.open("w", encoding="utf-8") as f:
            for row in plan_rows:
                f.write(json.dumps(row) + "\n")
        
        # Write Feasibility Report
        pd.DataFrame(feasibility_report).to_parquet(out_dir / "plan_feasibility_report.parquet", index=False)
        
        # Generate Hash
        plan_bytes = plan_path.read_bytes()
        plan_hash = "sha256:" + hashlib.sha256(plan_bytes).hexdigest()
        
        # Populate inputs_hashes dictionary securely (Standardize "hash field names")
        
        inputs_hashes = {
            "candidate_plan": {
                "path": str(plan_path.relative_to(PROJECT_ROOT.parent)),
                "hash": plan_hash,
                "rows": len(plan_rows)
            },
            "planner_exclusions": {
                "path": str(exclusions_path.relative_to(PROJECT_ROOT.parent)) if exclusions_path.exists() else None,
                "hash": exclusions_hash,
                "excluded_claims_count": exclusions_count,
                "mode": "file" if exclusions_path.exists() else "none"
            }
        }
        
        finalize_manifest(manifest, "success", stats={
            "total_candidates": len(plan_rows),
            "inputs_hashes": inputs_hashes,
            # Duplicate as convenience root keys as requested
            "candidate_plan_hash": plan_hash,
            "planner_exclusions_hash": exclusions_hash
        })

        
        # Explicitly append to global run manifest
        global_manifest_path = DATA_ROOT / "runs" / args.run_id / "run_manifest.json"
        if global_manifest_path.exists():
            try:
                g_manifest = json.loads(global_manifest_path.read_text(encoding="utf-8"))
                g_manifest["inputs_hashes"] = inputs_hashes
                g_manifest["candidate_plan_hash"] = plan_hash
                g_manifest["planner_exclusions_hash"] = exclusions_hash
                global_manifest_path.write_text(json.dumps(g_manifest, indent=2, sort_keys=True), encoding="utf-8")
            except Exception as e:
                logging.getLogger(__name__).warning(f"Failed to update global run manifest: {e}")

        # Output hash to stdout for orchestrator
        print(f"CANDIDATE_PLAN_HASH={plan_hash}")
        return 0

    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1

if __name__ == "__main__":
    sys.exit(main())
