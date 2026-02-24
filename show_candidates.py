import pandas as pd
import glob
import os

base_dir = "data/reports/phase2/discovery_2025_q1_v5"
files = glob.glob(os.path.join(base_dir, "*/phase2_candidates.csv"))

dfs = []
for f in files:
    try:
        df = pd.read_csv(f)
        if not df.empty and "is_discovery" in df.columns:
            # Filter for discoveries
            disc = df[df["is_discovery"] == True].copy()
            if not disc.empty:
                dfs.append(disc)
    except Exception as e:
        print(f"Error reading {f}: {e}")

if dfs:
    all_cands = pd.concat(dfs, ignore_index=True)
    # Select key columns
    cols = ["candidate_id", "event_type", "rule_template", "horizon", "expectancy", "p_value", "n_events", "gate_phase2_final", "phase2_quality_score"]
    # Sort by quality score desc
    print(all_cands[cols].sort_values("phase2_quality_score", ascending=False).head(20).to_markdown(index=False))
    print(f"\nTotal Discoveries: {len(all_cands)}")
else:
    print("No discoveries found in CSV files.")
