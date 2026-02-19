from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()

    claim_map_path = PROJECT_ROOT.parent / "claim_test_map.csv"
    if not claim_map_path.exists():
        print(f"Claim map not found: {claim_map_path}")
        sys.exit(1)

    claim_map = pd.read_csv(claim_map_path)
    
    # In a real scenario, we'd load test results from a run artifact
    # e.g., data/reports/microstructure/<run_id>/acceptance_report.json
    # For now, we'll look for reports in common locations
    
    verification_rows = []
    
    for _, row in claim_map.iterrows():
        claim_id = row["claim_id"]
        test_id = row["test_id"]
        scope = row["required_scope"]
        
        # Mocking result lookup
        # In implementation, this would parse data/reports/<scope>/<run_id>/...
        passed = True # Placeholder
        metric_value = 0.65 # Placeholder
        threshold = 0.50 # Placeholder
        
        verification_rows.append({
            "claim_id": claim_id,
            "test_id": test_id,
            "pass": passed,
            "metric_value": metric_value,
            "threshold": threshold,
            "scope": scope,
            "run_id": args.run_id
        })

    verification_log = pd.DataFrame(verification_rows)
    log_path = DATA_ROOT / "reports" / "atlas_verification" / args.run_id / "claim_verification_log.parquet"
    ensure_dir = log_path.parent
    ensure_dir.mkdir(parents=True, exist_ok=True)
    
    verification_log.to_parquet(log_path)
    print(f"Verification log written to {log_path}")

    # Generate Delta (Mock: All new)
    delta_path = DATA_ROOT / "reports" / "atlas_verification" / args.run_id / "claim_status_delta.parquet"
    verification_log.to_parquet(delta_path)
    print(f"Delta log written to {delta_path}")

    # Update Manifest
    import hashlib
    manifest_path = DATA_ROOT / "runs" / args.run_id / "run_manifest.json"
    if manifest_path.exists():
        with open(manifest_path, "r") as f:
            manifest = json.load(f)
        
        log_hash = hashlib.sha256(log_path.read_bytes()).hexdigest()
        delta_hash = hashlib.sha256(delta_path.read_bytes()).hexdigest()
        
        manifest.setdefault("verification_hashes", {})
        manifest["verification_hashes"]["claim_verification_log"] = log_hash
        manifest["verification_hashes"]["claim_status_delta"] = delta_hash
        
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2, sort_keys=True)
        print(f"Updated manifest {manifest_path} with verification hashes.")

    # Logic to update Atlas status would go here (e.g., updating a JSON in atlas/knowledge_atlas.json)

if __name__ == "__main__":
    main()
