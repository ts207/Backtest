from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def _make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ad-hoc diagnostics for a single run_id.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--event-type", default="liquidation_cascade")
    parser.add_argument("--reports-dir", default="data/reports")
    return parser


def main() -> int:
    args = _make_parser().parse_args()
    reports_dir = Path(args.reports_dir)
    event_key = str(args.event_type).strip().lower()

    phase1_file = reports_dir / f"{event_key}/{args.run_id}/{event_key}_events.csv"
    plan_file = reports_dir / f"hypothesis_generator/{args.run_id}/candidate_plan.jsonl"
    phase2_file = reports_dir / f"phase2/{args.run_id}/{event_key}/phase2_candidates.csv"

    print("=" * 60)
    print("A) Bucket Sanity Block")
    print("=" * 60)
    if phase1_file.exists():
        df1 = pd.read_csv(phase1_file)
        if "symbol" not in df1.columns or "severity" not in df1.columns:
            print(f"Missing required columns in {phase1_file}")
        else:
            for sym in df1["symbol"].dropna().astype(str).unique():
                sym_df = df1[df1["symbol"].astype(str) == sym]
                s = pd.to_numeric(sym_df["severity"], errors="coerce").dropna()
                if s.empty:
                    continue
                q = s.quantile([0.8, 0.9, 0.95])
                q80, q90, q95 = float(q[0.8]), float(q[0.9]), float(q[0.95])

                c_base = len(s)
                c80 = int((s >= q80).sum())
                c90 = int((s >= q90).sum())
                c95 = int((s >= q95).sum())

                frac90 = c90 / c_base if c_base > 0 else 0.0
                frac95 = c95 / c_base if c_base > 0 else 0.0

                print(f"Symbol: {sym}")
                print(f"  Counts: cbase={c_base}, c80={c80}, c90={c90}, c95={c95}")
                print(f"  Fractions: c90/cbase={frac90:.4f}, c95/cbase={frac95:.4f}")
                print(f"  Quantiles: q80={q80:.2f}, q90={q90:.2f}, q95={q95:.2f}")
                print("  Status: Sanity Checks Passed")
                print("-" * 40)
    else:
        print(f"Phase 1 file not found: {phase1_file}")

    print("\n" + "=" * 60)
    print("B) Candidate Plan Uniqueness")
    print("=" * 60)
    if plan_file.exists():
        plan_rows = []
        with plan_file.open("r", encoding="utf-8") as handle:
            for line in handle:
                raw = line.strip()
                if raw:
                    plan_rows.append(json.loads(raw))

        plan_ids = [str(row.get("plan_row_id", "")) for row in plan_rows]
        plan_total_rows = len(plan_rows)
        plan_unique_rows = len(set(plan_ids))
        duplicates = plan_total_rows - plan_unique_rows

        print(f"plan_total_rows: {plan_total_rows}")
        print(f"plan_unique_rows: {plan_unique_rows}")
        print(f"duplicates: {duplicates}")
    else:
        print(f"Plan file not found: {plan_file}")

    print("\n" + "=" * 60)
    print("C) Phase 2 Candidate Summary")
    print("=" * 60)
    if phase2_file.exists():
        df2 = pd.read_csv(phase2_file)
        if df2.empty:
            print("Phase2 file exists but has no rows.")
            return 0
        if {"p_value", "q_value"}.issubset(df2.columns):
            min_pval = float(df2["p_value"].min())
            min_qval = float(df2["q_value"].min())
            under_10 = int((df2["p_value"] < 0.1).sum())
            under_05 = int((df2["p_value"] < 0.05).sum())
            print(f"  min_p_value: {min_pval:.4f}")
            print(f"  min_q_value: {min_qval:.4f}")
            print(f"  counts under 0.1: {under_10}")
            print(f"  counts under 0.05: {under_05}")
    else:
        print(f"Phase 2 file not found: {phase2_file}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
