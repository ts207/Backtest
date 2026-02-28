"""
validate_oos_window.py
======================
Validates that an evaluation window does not overlap the discovery window
recorded in a run_manifest.json. Fails with exit code 1 if overlap detected.

Usage:
    python project/scripts/validate_oos_window.py \\
        --run_id discovery_2020_2025 \\
        --eval_start 2025-01-01 \\
        --eval_end   2025-07-01 \\
        [--allow_oos_overlap 0]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))


def _load_manifest(run_id: str) -> dict:
    path = DATA_ROOT / "runs" / run_id / "run_manifest.json"
    if not path.exists():
        print(f"[oos_guard] Warning: run_manifest.json not found for run_id={run_id}. Skipping OOS check.", file=sys.stderr)
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"[oos_guard] Warning: could not parse manifest: {exc}", file=sys.stderr)
        return {}


def validate_oos_window(
    run_id: str,
    eval_start: str,
    eval_end: str,
    allow_overlap: bool = False,
) -> bool:
    """
    Returns True if the eval window is clean (no overlap with discovery window).
    Returns False and prints an informative error if overlap detected.
    Raises SystemExit(1) unless allow_overlap=True.
    """
    manifest = _load_manifest(run_id)
    if not manifest:
        return True  # Cannot determine; allow by default

    discovery_start = str(manifest.get("start") or manifest.get("start_date") or "").strip()
    discovery_end = str(manifest.get("end") or manifest.get("end_date") or "").strip()

    if not discovery_end:
        print("[oos_guard] No discovery end date found in manifest. Skipping OOS overlap check.", file=sys.stderr)
        return True

    # Compare date strings — ISO format comparison is safe for YYYY-MM-DD
    if eval_start <= discovery_end:
        msg = (
            f"[oos_guard] OOS OVERLAP DETECTED:\n"
            f"  Discovery window: {discovery_start} → {discovery_end}\n"
            f"  Evaluation window: {eval_start} → {eval_end}\n"
            f"  eval_start ({eval_start}) <= discovery_end ({discovery_end}).\n"
            f"  Set --allow_oos_overlap 1 to bypass (forbidden in production mode)."
        )
        print(msg, file=sys.stderr)
        if not allow_overlap:
            sys.exit(1)
        return False
    print(
        f"[oos_guard] OOS window OK: discovery_end={discovery_end} < eval_start={eval_start}",
        file=sys.stderr,
    )
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate OOS evaluation window against discovery window.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--eval_start", required=True, help="Start of evaluation window (YYYY-MM-DD)")
    parser.add_argument("--eval_end", required=True, help="End of evaluation window (YYYY-MM-DD)")
    parser.add_argument("--allow_oos_overlap", type=int, default=0, help="If 1, log warning instead of failing on overlap.")
    args = parser.parse_args()

    ok = validate_oos_window(
        run_id=args.run_id,
        eval_start=args.eval_start,
        eval_end=args.eval_end,
        allow_overlap=bool(int(args.allow_oos_overlap)),
    )
    return 0 if ok else 2


if __name__ == "__main__":
    sys.exit(main())
