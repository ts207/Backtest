import pandas as pd
import json
from pathlib import Path

run_id = "atlas_verification"
reports_dir = Path(f"data/reports")
phase1_file = reports_dir / f"liquidation_cascade/{run_id}/liquidation_cascade_events.csv"
plan_file = reports_dir / f"hypothesis_generator/{run_id}/candidate_plan.jsonl"
phase2_file = reports_dir / f"phase2/{run_id}/liquidation_cascade/phase2_candidates.csv"

print("="*60)
print("A) Bucket Sanity Block")
print("="*60)
if phase1_file.exists():
    df1 = pd.read_csv(phase1_file)
    for sym in df1['symbol'].unique():
        sym_df = df1[df1['symbol'] == sym]
        s = sym_df['severity']
        q = s.quantile([0.8, 0.9, 0.95])
        q80, q90, q95 = q[0.8], q[0.9], q[0.95]
        
        c_base = len(s)
        c80 = (s >= q80).sum()
        c90 = (s >= q90).sum()
        c95 = (s >= q95).sum()
        
        frac90 = c90 / c_base if c_base > 0 else 0
        frac95 = c95 / c_base if c_base > 0 else 0
        
        print(f"Symbol: {sym}")
        print(f"  Counts: cbase={c_base}, c80={c80}, c90={c90}, c95={c95}")
        print(f"  Fractions: c90/cbase={frac90:.4f}, c95/cbase={frac95:.4f}")
        print(f"  Quantiles: q80={q80:.2f}, q90={q90:.2f}, q95={q95:.2f}")
        
        # Hard acceptance criteria
        assert c95 <= c90 <= c80 <= c_base, "Monotonicity failed"
        assert frac90 <= 0.35, "Fraction sanity > 0.35 failed"
        if frac90 > 0 and frac90 != frac95:
             pass # just noting
        print("  Status: Sanity Checks Passed")
        print("-" * 40)
else:
    print(f"Phase 1 file not found: {phase1_file}")

print("\n" + "="*60)
print("B) Candidate Plan Uniqueness")
print("="*60)
if plan_file.exists():
    plan_rows = []
    with open(plan_file, "r") as f:
        for line in f:
            if line.strip():
                plan_rows.append(json.loads(line))
                
    plan_total_rows = len(plan_rows)
    plan_ids = [r.get("plan_row_id") for r in plan_rows]
    plan_unique_rows = len(set(plan_ids))
    duplicates = plan_total_rows - plan_unique_rows
    
    print(f"plan_total_rows: {plan_total_rows}")
    print(f"plan_unique_rows: {plan_unique_rows}")
    print(f"duplicates: {duplicates}")
    print("Top 10 plan_row_id (sample):")
    for pid in list(set(plan_ids))[:10]:
        print(f"  {pid}")
else:
    print(f"Plan file not found: {plan_file}")

print("\n" + "="*60)
print("C) Phase 2 Candidate Summary")
print("="*60)
if phase2_file.exists():
    df2 = pd.read_csv(phase2_file)
    print("Aggregate Stats:")
    min_pval = df2['p_value'].min()
    min_qval = df2['q_value'].min()
    under_10 = (df2['p_value'] < 0.1).sum()
    under_05 = (df2['p_value'] < 0.05).sum()
    
    print(f"  min_p_value: {min_pval:.4f}")
    print(f"  min_q_value: {min_qval:.4f}")
    print(f"  counts under 0.1: {under_10}")
    print(f"  counts under 0.05: {under_05}")
    print("-" * 40)
    
    print("Top 10 Phase 2 Rows (sorted by p_value):")
    top10 = df2.sort_values("p_value").head(10)
    for _, r in top10.iterrows():
        print(f"{r['candidate_id']}: n_events={r['n_events']}, "
              f"effect_bps={r['expectancy']*10000:.2f}, "
              f"p={r['p_value']:.4f}, q={r['q_value']:.4f}, "
              f"stability={r['gate_stability']}")
else:
    print(f"Phase 2 file not found: {phase2_file}")
