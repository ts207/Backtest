import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import os
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir

def calculate_lift(group_df: pd.DataFrame) -> pd.DataFrame:
    # Identify unconditional baseline (condition == "all" or empty or "unconditional")
    # Our candidate plan generator produces "conditioning": {} which results in a candidate_id without conditioning suffix?
    # Actually, in Phase 2, we might not have explicit "condition" column parsed out yet if it's just in the candidate_id string.
    # Let's assume we can parse it from candidate_id or it's passed through.
    
    # In generate_candidate_plan: plan_row_id = f"{claim_id}:{event_type}:{rule}:{horizon}:{c_key}_{c_val}:{symbol}"
    # Phase 2 results come from executing this plan.
    
    # We need to parse candidate_id to extract conditioning.
    # Format: claim:event:rule:horizon:condition:symbol
    
    rows = []
    for _, row in group_df.iterrows():
        cid = str(row["candidate_id"])
        parts = cid.split(":")
        if len(parts) >= 6:
            condition = parts[4]
        else:
            condition = "unknown"
        
        rows.append({
            "candidate_id": cid,
            "condition": condition,
            "expectancy": row.get("expectancy", 0.0),
            "n_events": row.get("n_events", 0)
        })
        
    df = pd.DataFrame(rows)
    
    # Baseline: condition == "all"
    baseline = df[df["condition"] == "all"]
    if baseline.empty:
        return pd.DataFrame()
    
    base_exp = baseline["expectancy"].mean()
    
    out_rows = []
    for _, row in df.iterrows():
        if row["condition"] == "all":
            continue
            
        lift = (row["expectancy"] - base_exp)
        lift_pct = (lift / abs(base_exp)) if base_exp != 0 else 0.0
        
        out_rows.append({
            "candidate_id": row["candidate_id"],
            "condition": row["condition"],
            "baseline_expectancy": base_exp,
            "conditioned_expectancy": row["expectancy"],
            "lift_bps": lift * 10000.0,
            "lift_pct": lift_pct,
            "n_events": row["n_events"]
        })
        
    return pd.DataFrame(out_rows)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()
    
    # Load Phase 2 Results
    # We need to aggregate across all event types
    phase2_root = DATA_ROOT / "reports" / "phase2" / args.run_id
    all_results = []
    
    if not phase2_root.exists():
        print(f"No Phase 2 results found at {phase2_root}")
        return
        
    for event_dir in phase2_root.iterdir():
        if event_dir.is_dir():
            res_file = event_dir / "phase2_candidates.csv"
            if res_file.exists():
                df = pd.read_csv(res_file)
                all_results.append(df)
                
    if not all_results:
        print("No results loaded.")
        return
        
    full_df = pd.concat(all_results)
    
    # Parse family keys from candidate_id (claim:event:rule:horizon:condition:symbol)
    # Group by (event, rule, horizon, symbol) to compare conditions within that scope
    
    results = []
    
    def parse_key(cid):
        parts = str(cid).split(":")
        if len(parts) >= 6:
            # claim, event, rule, horizon, condition, symbol
            return tuple([parts[1], parts[2], parts[3], parts[5]])
        return None

    full_df["group_key"] = full_df["candidate_id"].apply(parse_key)
    
    for key, group in full_df.groupby("group_key"):
        if key is None: continue
        
        lift_df = calculate_lift(group)
        if not lift_df.empty:
            lift_df["event_type"] = key[0]
            lift_df["rule"] = key[1]
            lift_df["horizon"] = key[2]
            lift_df["symbol"] = key[3]
            results.append(lift_df)
            
    if results:
        final_df = pd.concat(results)
        out_dir = DATA_ROOT / "reports" / "ablation" / args.run_id
        ensure_dir(out_dir)
        
        final_df.to_parquet(out_dir / "ablation_report.parquet", index=False)
        final_df.to_csv(out_dir / "lift_summary.csv", index=False)
        print(f"Ablation report written to {out_dir}")
        print(final_df.groupby("condition")["lift_bps"].mean().to_markdown())
    else:
        print("No lift calculations possible (missing baselines?)")

if __name__ == "__main__":
    main()
