from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def _make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Show top discovery candidates across phase2 outputs.")
    parser.add_argument(
        "--base-dir",
        default="data/reports/phase2",
        help="Directory to search recursively for phase2_candidates.csv files.",
    )
    parser.add_argument("--top-k", type=int, default=20)
    return parser


def main() -> int:
    args = _make_parser().parse_args()
    base_dir = Path(args.base_dir)
    files = sorted(base_dir.rglob("phase2_candidates.csv")) if base_dir.exists() else []

    dfs = []
    for file_path in files:
        try:
            df = pd.read_csv(file_path)
        except Exception as exc:
            print(f"Error reading {file_path}: {exc}")
            continue

        if df.empty or "is_discovery" not in df.columns:
            continue
        disc = df[df["is_discovery"].astype(bool)].copy()
        if not disc.empty:
            dfs.append(disc)

    if not dfs:
        print("No discovery rows found.")
        return 0

    all_cands = pd.concat(dfs, ignore_index=True)
    preferred = [
        "candidate_id",
        "event_type",
        "rule_template",
        "horizon",
        "expectancy",
        "p_value",
        "n_events",
        "gate_phase2_final",
        "phase2_quality_score",
    ]
    cols = [c for c in preferred if c in all_cands.columns]
    sort_col = "phase2_quality_score" if "phase2_quality_score" in all_cands.columns else cols[-1]
    print(all_cands[cols].sort_values(sort_col, ascending=False).head(max(1, int(args.top_k))).to_markdown(index=False))
    print(f"\nTotal Discoveries: {len(all_cands)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
