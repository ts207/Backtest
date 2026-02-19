from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.run_manifest import finalize_manifest, start_manifest

# Bounded search caps
MAX_CANDIDATES_PER_CLAIM = 30
MAX_TOTAL_CANDIDATES = 50
MAX_CONDITIONING_VARIANTS = 6

DEFAULT_HORIZONS = ["5m", "15m", "60m"]
DEFAULT_RULE_TEMPLATES = ["mean_reversion", "continuation"]
DEFAULT_CONDITIONING = {
    "vol_regime": ["high"],
    "severity_bucket": ["top_10pct", "extreme_5pct"]
}

def _parse_symbols(assets: str) -> List[str]:
    if not assets or assets == "*":
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT"] # Default universe
    return [s.strip().upper() + "USDT" if not s.endswith("USDT") else s.strip().upper() 
            for s in assets.split("|") if s.strip()]

def _extract_event_type(statement: str) -> Optional[str]:
    # Heuristic: find uppercase tokens that look like event names
    match = re.search(r'""event_type"": ""([A-Z0-9_]+)""', statement)
    if match:
        return match.group(1)
    # Fallback to any uppercase word with underscore
    matches = re.findall(r'\b[A-Z][A-Z0-9_]{5,}\b', statement)
    if matches:
        return matches[0]
    return None

def main() -> int:
    parser = argparse.ArgumentParser(description="Atlas-driven hypothesis and task generator")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--backlog", default="research_backlog.csv")
    parser.add_argument("--out_dir", default=None)
    args = parser.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "hypothesis_generator" / args.run_id
    ensure_dir(out_dir)

    manifest = start_manifest("atlas_hypothesis_generator", args.run_id, vars(args))

    try:
        backlog_path = PROJECT_ROOT.parent / args.backlog
        if not backlog_path.exists():
            raise FileNotFoundError(f"Backlog not found: {backlog_path}")
        
        df = pd.read_csv(backlog_path)
        
        # Filter for operationalizable and unverified
        active_claims = df[(df['operationalizable'] == 'Y') & (df['status'] != 'verified')].copy()
        
        spec_tasks = []
        plan_rows = []
        
        for _, row in active_claims.iterrows():
            claim_id = str(row['claim_id'])
            c_type = str(row['candidate_type']).lower()
            statement = str(row['statement_summary'])
            features_list = str(row['features']).split('|')
            
            # 1. Spec Task Check
            # Resolve target path from next_artifact pattern
            target_pattern = str(row['next_artifact'])
            
            if c_type == 'event':
                event_type = _extract_event_type(statement)
                if not event_type:
                    continue
                target_path = target_pattern.replace("{event_type}", event_type)
                
                spec_exists = (PROJECT_ROOT.parent / target_path).exists()
                if not spec_exists:
                    spec_tasks.append({
                        "claim_id": claim_id,
                        "concept_id": row['concept_id'],
                        "object_type": "event",
                        "target_path": target_path,
                        "priority_rank": row['priority_score'],
                        "statement": statement,
                        "assets_filter": row['assets']
                    })
                else:
                    # 2. Generate Candidate Plan Rows
                    symbols = _parse_symbols(str(row['assets']))
                    for symbol in symbols:
                        for rule in DEFAULT_RULE_TEMPLATES:
                            for horizon in DEFAULT_HORIZONS:
                                for c_key, c_vals in DEFAULT_CONDITIONING.items():
                                    for c_val in c_vals:
                                        plan_rows.append({
                                            "plan_row_id": f"{claim_id}:{event_type}:{rule}:{horizon}:{c_key}_{c_val}:{symbol}",
                                            "source_claim_ids": [claim_id],
                                            "source_concept_ids": [str(row['concept_id'])],
                                            "event_type": event_type,
                                            "rule_template": rule,
                                            "horizon": horizon,
                                            "symbol": symbol,
                                            "conditioning": {c_key: c_val},
                                            "min_events": 20 if "extreme" in c_val else 50
                                        })

            elif c_type == 'feature':
                # For features like ROLL/VPIN
                for feat in features_list:
                    target_path = target_pattern.replace("{feature_name}", feat)
                    spec_exists = (PROJECT_ROOT.parent / target_path).exists()
                    if not spec_exists:
                        spec_tasks.append({
                            "claim_id": claim_id,
                            "concept_id": row['concept_id'],
                            "object_type": "feature",
                            "target_path": target_path,
                            "priority_rank": row['priority_score'],
                            "statement": statement,
                            "assets_filter": row['assets']
                        })
                    else:
                        # Once feature exists, we might want to test its predictive power
                        # This would map to a specialized predictive family
                        # For now, let's just log the task until implemented
                        pass

        # Apply Global Caps
        if len(plan_rows) > MAX_TOTAL_CANDIDATES:
            plan_rows = plan_rows[:MAX_TOTAL_CANDIDATES]

        # Save Artifacts
        tasks_df = pd.DataFrame(spec_tasks)
        tasks_df.to_parquet(out_dir / "spec_tasks.parquet", index=False)
        
        with (out_dir / "candidate_plan.jsonl").open("w") as f:
            for row in plan_rows:
                f.write(json.dumps(row) + "
")

        finalize_manifest(manifest, "success", stats={
            "claims_processed": len(active_claims),
            "spec_tasks_generated": len(spec_tasks),
            "plan_rows_generated": len(plan_rows)
        })
        return 0

    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1

if __name__ == "__main__":
    sys.exit(main())
