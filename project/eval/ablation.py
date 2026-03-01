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
from eval.multiplicity import benjamini_hochberg

def calculate_lift(group_df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-condition lift vs baseline ('all') with BH multiplicity correction.

    Requires columns: candidate_id, condition_key, expectancy, n_events.
    Optional column: p_value (used for BH; defaults to 1.0 if absent).
    Returns DataFrame with lift_q_value and is_lift_discovery added (F-2 fix).
    """
    rows = []
    for _, row in group_df.iterrows():
        condition = str(row.get("condition_key", "unknown")).strip()
        rows.append({
            "candidate_id": row["candidate_id"],
            "condition": condition,
            "expectancy": row.get("expectancy", 0.0),
            "n_events": row.get("n_events", 0),
            "p_value": float(row.get("p_value", 1.0)),
        })

    df = pd.DataFrame(rows)

    baseline = df[df["condition"] == "all"]
    if baseline.empty:
        return pd.DataFrame()

    base_exp = baseline["expectancy"].mean()

    out_rows = []
    for _, row in df.iterrows():
        if row["condition"] == "all":
            continue
        lift = row["expectancy"] - base_exp
        lift_pct = (lift / abs(base_exp)) if base_exp != 0 else 0.0
        out_rows.append({
            "candidate_id": row["candidate_id"],
            "condition": row["condition"],
            "baseline_expectancy": base_exp,
            "conditioned_expectancy": row["expectancy"],
            "lift_bps": lift * 10000.0,
            "lift_pct": lift_pct,
            "n_events": row["n_events"],
            "p_value": row["p_value"],
        })

    if not out_rows:
        return pd.DataFrame()

    result = pd.DataFrame(out_rows)

    # BH multiplicity correction across all conditions in this group (F-2 fix).
    _BH_ALPHA = 0.10
    _, q_values = benjamini_hochberg(result["p_value"].tolist(), alpha=_BH_ALPHA)
    result["lift_q_value"] = q_values
    result["is_lift_discovery"] = result["lift_q_value"] <= _BH_ALPHA

    return result.drop(columns=["p_value"])

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
            res_file_parquet = event_dir / "phase2_candidates.parquet"
            res_file_csv = event_dir / "phase2_candidates.csv"
            df = pd.DataFrame()
            if res_file_parquet.exists():
                try:
                    df = pd.read_parquet(res_file_parquet)
                except Exception:
                    df = pd.DataFrame()
            if df.empty and res_file_csv.exists():
                try:
                    df = pd.read_csv(res_file_csv)
                except Exception:
                    df = pd.DataFrame()
            if not df.empty:
                all_results.append(df)
                
    if not all_results:
        print("No results loaded.")
        return
        
    full_df = pd.concat(all_results)
    
    # Group by (event_type, rule_template, horizon, symbol) to compare conditions within that scope
    
    results = []
    
    # Ensure columns exist
    required_cols = ["event_type", "rule_template", "horizon", "symbol", "conditioning", "expectancy", "n_events"]
    if not all(col in full_df.columns for col in required_cols):
        print(f"Missing required columns in Phase 2 results. Available: {full_df.columns.tolist()}")
        return

    # Create grouping key
    full_df["group_key"] = list(zip(full_df["event_type"], full_df["rule_template"], full_df["horizon"], full_df["symbol"]))
    
    for key, group in full_df.groupby("group_key"):
        # group is a DataFrame for one (event, rule, horizon, symbol)
        # It contains multiple conditions (all, vol_regime_high, etc)
        print(f"DEBUG: Processing group {key}. Conditions: {group['conditioning'].unique()}")
        
        # Prepare for calculate_lift
        # calculate_lift expects "candidate_id", "condition_key", "expectancy", "n_events"
        group_renamed = group.rename(columns={"conditioning": "condition_key"})
        
        lift_df = calculate_lift(group_renamed)
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
