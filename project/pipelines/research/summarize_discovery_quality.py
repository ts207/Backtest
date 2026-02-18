from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize phase2 discovery quality across event families.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument(
        "--phase2_root",
        default="",
        help="Optional phase2 root directory (default: data/reports/phase2/<run_id>).",
    )
    parser.add_argument(
        "--out_path",
        default="",
        help="Optional output path (default: <phase2_root>/discovery_quality_summary.json).",
    )
    parser.add_argument("--top_fail_reasons", type=int, default=10)
    return parser.parse_args()


def _load_candidates(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _split_fail_reasons(series: pd.Series) -> List[str]:
    out: List[str] = []
    for raw in series.fillna("").astype(str):
        for token in raw.split(","):
            reason = token.strip()
            if reason:
                out.append(reason)
    return out


def _gate_pass_series(df: pd.DataFrame) -> pd.Series:
    if "gate_pass" in df.columns:
        return pd.to_numeric(df["gate_pass"], errors="coerce").fillna(0.0).astype(float) > 0.0
    if "gate_all" in df.columns:
        return pd.to_numeric(df["gate_all"], errors="coerce").fillna(0.0).astype(float) > 0.0
    return pd.Series(False, index=df.index)


def _event_summary(df: pd.DataFrame) -> Dict[str, float | int]:
    total = int(len(df))
    gate_pass = _gate_pass_series(df)
    pass_count = int(gate_pass.sum()) if total else 0
    pass_rate = float(pass_count / total) if total else 0.0
    return {
        "total_candidates": total,
        "gate_pass_count": pass_count,
        "gate_pass_rate": pass_rate,
    }


def build_summary(*, run_id: str, phase2_root: Path, top_fail_reasons: int) -> dict:
    event_dirs = sorted([p for p in phase2_root.iterdir() if p.is_dir()]) if phase2_root.exists() else []
    if not event_dirs:
        raise FileNotFoundError(f"No phase2 event directories found for run_id={run_id}: {phase2_root}")

    by_event_family: Dict[str, Dict[str, float | int]] = {}
    all_frames: List[pd.DataFrame] = []
    source_files: Dict[str, str] = {}
    for event_dir in event_dirs:
        candidates_path = event_dir / "phase2_candidates.csv"
        if not candidates_path.exists():
            continue
        frame = _load_candidates(candidates_path)
        if frame.empty:
            frame = pd.DataFrame(columns=["candidate_id", "gate_pass", "fail_reasons"])
        frame["event_type"] = str(event_dir.name)
        by_event_family[event_dir.name] = _event_summary(frame)
        source_files[event_dir.name] = str(candidates_path)
        all_frames.append(frame)

    combined = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
    total_candidates = int(len(combined))
    gate_pass = _gate_pass_series(combined) if not combined.empty else pd.Series(dtype=bool)
    gate_pass_count = int(gate_pass.sum()) if total_candidates else 0
    gate_pass_rate = float(gate_pass_count / total_candidates) if total_candidates else 0.0

    fail_reason_counter = Counter(_split_fail_reasons(combined.get("fail_reasons", pd.Series(dtype=str))))
    top_reasons = [
        {"reason": reason, "count": int(count)}
        for reason, count in fail_reason_counter.most_common(max(0, int(top_fail_reasons)))
    ]

    return {
        "run_id": run_id,
        "generated_at": _utc_now_iso(),
        "phase2_root": str(phase2_root),
        "source_files": source_files,
        "event_families": sorted(by_event_family.keys()),
        "total_candidates": total_candidates,
        "gate_pass_count": gate_pass_count,
        "gate_pass_rate": gate_pass_rate,
        "top_fail_reasons": top_reasons,
        "by_event_family": by_event_family,
    }


def main() -> int:
    args = _parse_args()
    phase2_root = Path(args.phase2_root) if args.phase2_root else DATA_ROOT / "reports" / "phase2" / args.run_id
    out_path = Path(args.out_path) if args.out_path else phase2_root / "discovery_quality_summary.json"

    payload = build_summary(
        run_id=args.run_id,
        phase2_root=phase2_root,
        top_fail_reasons=int(args.top_fail_reasons),
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"run_id": args.run_id, "out_path": str(out_path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

