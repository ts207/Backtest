from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

def load_backlog(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Backlog not found: {path}")
    return pd.read_csv(path)

def save_backlog(df: pd.DataFrame, path: Path) -> None:
    # Backup existing
    backup_path = path.with_suffix(".csv.bak")
    if path.exists():
        import shutil
        shutil.copy2(path, backup_path)
    df.to_csv(path, index=False)
    logging.info("Saved updated backlog to %s (backup at %s)", path, backup_path)

def main() -> int:
    parser = argparse.ArgumentParser(description="Verify research backlog claims against run results.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--backlog_path", default=str(PROJECT_ROOT.parent / "research_backlog.csv"))
    parser.add_argument("--source_artifact_path", default=None, help="Direct path to promotion_audit.csv or similar")
    parser.add_argument("--promoted_only", type=int, default=0)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    
    run_id = args.run_id
    backlog_path = Path(args.backlog_path)
    
    try:
        backlog = load_backlog(backlog_path)
        logging.info("Loaded backlog with %d claims.", len(backlog))
        
        if args.source_artifact_path:
            audit_path = Path(args.source_artifact_path)
        else:
            promo_dir = DATA_ROOT / "reports" / "promotions" / run_id
            audit_path = promo_dir / "promotion_audit.csv"
        
        if not audit_path.exists():
            logging.error("Promotion audit not found: %s", audit_path)
            return 1
            
        audit = pd.read_csv(audit_path)
        logging.info("Loaded promotion audit with %d candidates.", len(audit))
        
        # Flatten source_claim_ids
        # Format is usually "CL_0001|CL_0002" or similar
        claim_map = {}
        for _, row in audit.iterrows():
            cid = str(row["candidate_id"])
            decision = str(row["promotion_decision"])
            sample_size = int(row.get("sample_size", 0))
            claims_raw = str(row.get("source_claim_ids", ""))
            
            if not claims_raw or claims_raw.lower() == "nan":
                continue
                
            claim_ids = [c.strip() for c in claims_raw.split("|") if c.strip()]
            for claim_id in claim_ids:
                if claim_id not in claim_map:
                    claim_map[claim_id] = []
                
                status = decision.upper()
                if decision == "promoted":
                    status = "VERIFIED"
                elif decision == "rejected":
                    # Check if it was rejected primarily for sample size
                    if sample_size < 30:
                        status = "INSUFFICIENT_DATA"
                    else:
                        status = "REJECTED"
                
                claim_map[claim_id].append({
                    "candidate_id": cid,
                    "status": status,
                    "run_id": run_id,
                    "sample_size": sample_size
                })
        
        # Update backlog
        updates = 0
        for idx, row in backlog.iterrows():
            claim_id = str(row["claim_id"])
            if claim_id in claim_map:
                matches = claim_map[claim_id]
                # Priority: VERIFIED > REJECTED > INSUFFICIENT_DATA
                verified = [m for m in matches if m["status"] == "VERIFIED"]
                rejected = [m for m in matches if m["status"] == "REJECTED"]
                
                if verified:
                    best = verified[0]
                elif rejected:
                    best = rejected[0]
                else:
                    best = matches[0]
                
                backlog.at[idx, "status"] = best["status"]
                backlog.at[idx, "evidence_locator"] = f"run_id={best['run_id']}|candidate_id={best['candidate_id']}"
                updates += 1
        
        if updates > 0:
            save_backlog(backlog, backlog_path)
            logging.info("Updated %d claims in backlog.", updates)
        else:
            logging.info("No matching claims found to update.")
            
        return 0
    except Exception as exc:
        logging.exception("Verification failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
