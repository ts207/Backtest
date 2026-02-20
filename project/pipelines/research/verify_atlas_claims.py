"""
Verify atlas claims against actual run artifacts.

For each row in claim_test_map.csv the script:
  1. Locates the test artifact for this run (JSON report).
  2. Reads the relevant metric field.
  3. Compares it against the threshold.
  4. Writes claim_verification_log.parquet.
  5. Computes a true delta against the prior run's log and writes
     claim_status_delta.parquet (only changed rows).
  6. Scans Phase 2 output for blocked conditions and writes
     claim_blocked_conditions.parquet.
  7. Applies rolling blocked count and marks claims
     blocked_non_executable_condition when count >= ROLLING_BLOCKED_THRESHOLD.
  8. Writes planner_excluded_claims.json for fast exclusion at plan-time.
  9. Updates run_manifest.json with hashes and row/byte counts.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

# Number of recent runs to consider for rolling blocked count.
ROLLING_BLOCKED_K = 5
# Consecutive blocks to flip a claim to blocked_non_executable_condition.
ROLLING_BLOCKED_THRESHOLD = 3


# ---------------------------------------------------------------------------
# Artifact resolution
# ---------------------------------------------------------------------------

def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _lookup_microstructure(run_id: str, scope: str) -> Tuple[Optional[float], float, bool]:
    """T_MICROSTRUCTURE_ACCEPTANCE — checks gate_e1_pass in event_quality_report.json."""
    reports_root = DATA_ROOT / "reports"
    for event_dir in reports_root.iterdir() if reports_root.exists() else []:
        if not event_dir.is_dir():
            continue
        path = event_dir / run_id / "event_quality_report.json"
        payload = _read_json(path)
        if payload is None:
            continue
        reports = payload if isinstance(payload, list) else [payload]
        for r in reports:
            if not isinstance(r, dict):
                continue
            join_rate = r.get("metrics", {}).get("join_rate")
            if join_rate is None:
                continue
            threshold = r.get("thresholds", {}).get("min_join_rate", 0.99)
            return float(join_rate), float(threshold), bool(r.get("gate_e1_pass", False))
    return None, 0.99, False


def _lookup_multiplicity_fdr(run_id: str, scope: str) -> Tuple[Optional[float], float, bool]:
    """T_MULTIPLICITY_FDR — checks max q_value across phase2_fdr.parquet."""
    phase2_root = DATA_ROOT / "reports" / "phase2" / run_id
    if not phase2_root.exists():
        return None, 0.05, False
    q_values = []
    for event_dir in phase2_root.iterdir():
        fdr_path = event_dir / "phase2_fdr.parquet"
        if not fdr_path.exists():
            continue
        try:
            df = pd.read_parquet(fdr_path)
        except Exception:
            continue
        if "q_value" in df.columns and "is_discovery" in df.columns:
            discoveries = df[df["is_discovery"]]
            if not discoveries.empty:
                q_values.extend(discoveries["q_value"].tolist())
    if not q_values:
        return None, 0.05, False
    max_q = float(max(q_values))
    threshold = 0.05
    return max_q, threshold, max_q <= threshold


def _lookup_conditional_expectancy(run_id: str, scope: str) -> Tuple[Optional[float], float, bool]:
    """T_CONDITIONAL_EXPECTANCY — checks expectancy_exists in conditional_expectancy.json."""
    path = DATA_ROOT / "reports" / "expectancy" / run_id / "conditional_expectancy.json"
    payload = _read_json(path)
    if payload is None:
        return None, 1.0, False
    exists = bool(payload.get("expectancy_exists", False))
    evidence = payload.get("expectancy_evidence", [])
    metric_value = float(len(evidence))
    threshold = 1.0
    return metric_value, threshold, exists


def _lookup_event_quality(run_id: str, scope: str) -> Tuple[Optional[float], float, bool]:
    """T_EVENT_QUALITY_E1 — checks gate_e1_pass rate across symbols."""
    reports_root = DATA_ROOT / "reports"
    pass_count = 0
    total_count = 0
    for event_dir in (reports_root.iterdir() if reports_root.exists() else []):
        if not event_dir.is_dir():
            continue
        path = event_dir / run_id / "event_quality_report.json"
        payload = _read_json(path)
        if payload is None:
            continue
        reports = payload if isinstance(payload, list) else [payload]
        for r in reports:
            if not isinstance(r, dict):
                continue
            total_count += 1
            if r.get("gate_e1_pass", False):
                pass_count += 1
    if total_count == 0:
        return None, 1.0, False
    rate = float(pass_count / total_count)
    return rate, 1.0, rate >= 1.0


# Test ID → lookup function
_LOOKUP_MAP = {
    "T_MICROSTRUCTURE_ACCEPTANCE": _lookup_microstructure,
    "T_MULTIPLICITY_FDR": _lookup_multiplicity_fdr,
    "T_CONDITIONAL_EXPECTANCY": _lookup_conditional_expectancy,
    "T_EVENT_QUALITY_E1": _lookup_event_quality,
}

_DEFAULT_LOOKUP = lambda run_id, scope: (None, 0.5, False)


def _find_prior_log(run_id: str) -> Optional[pd.DataFrame]:
    """Find the most recent claim_verification_log.parquet from any previous run."""
    base = DATA_ROOT / "reports" / "atlas_verification"
    if not base.exists():
        return None
    prior_logs = []
    for run_dir in base.iterdir():
        if run_dir.name == run_id or not run_dir.is_dir():
            continue
        log_path = run_dir / "claim_verification_log.parquet"
        if log_path.exists():
            prior_logs.append((log_path.stat().st_mtime, log_path))
    if not prior_logs:
        return None
    _, latest = sorted(prior_logs)[-1]
    try:
        return pd.read_parquet(latest)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Blocked-condition feedback helpers
# ---------------------------------------------------------------------------

def _sha256_file(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _load_phase2_blocked_rows(run_id: str) -> pd.DataFrame:
    """Collect Phase 2 output rows where compile_eligible=False (blocked conditions).

    Searches all event-type subdirectories under reports/phase2/<run_id>/.
    Returns a unified DataFrame with the columns needed for claim feedback.
    """
    phase2_root = DATA_ROOT / "reports" / "phase2" / run_id
    if not phase2_root.exists():
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []
    for event_dir in phase2_root.iterdir():
        if not event_dir.is_dir():
            continue
        for fname in ("phase2_candidates.csv", "phase2_candidates.parquet"):
            fpath = event_dir / fname
            if not fpath.exists():
                continue
            try:
                df = pd.read_csv(fpath) if fpath.suffix == ".csv" else pd.read_parquet(fpath)
            except Exception:
                continue
            if df.empty:
                continue
            # Only keep rows that have the new condition_source column and are blocked
            if "condition_source" not in df.columns:
                continue
            blocked = df[df["condition_source"] == "blocked"].copy()
            if blocked.empty:
                continue
            frames.append(blocked)
            break  # prefer csv over parquet if both exist

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _aggregate_blocked_conditions(
    run_id: str,
    blocked_df: pd.DataFrame,
    candidate_plan_hash: str,
) -> pd.DataFrame:
    """Build claim_blocked_conditions.parquet rows from blocked Phase 2 candidates.

    Groups by (claim_id, condition_raw) over all blocked rows in this run.
    Returns one row per (claim_id, condition_name) with occurrence statistics.
    """
    if blocked_df.empty:
        return pd.DataFrame()

    # Explode source_claim_ids — each candidate may belong to multiple claims
    records: List[Dict] = []
    for _, row in blocked_df.iterrows():
        raw_claims = str(row.get("source_claim_ids", "") or "")
        if not raw_claims.strip():
            # Use a sentinel if no claim lineage: makes the record visible for debugging
            claim_ids = ["_no_claim_lineage"]
        else:
            claim_ids = [c.strip() for c in raw_claims.split(",") if c.strip()]

        condition_raw = str(row.get("condition_raw", row.get("conditioning", "unknown")))
        routed_condition = str(row.get("condition", "__BLOCKED__"))
        cand_id = str(row.get("candidate_id", ""))
        p_hash = str(row.get("candidate_plan_hash", candidate_plan_hash))

        for claim_id in claim_ids:
            records.append({
                "claim_id": claim_id,
                "condition_name": condition_raw,
                "routed_condition": routed_condition,
                "run_id": run_id,
                "candidate_plan_hash": p_hash,
                "candidate_id": cand_id,
            })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    # Group to get count + example candidate IDs (up to 3)
    grouped = (
        df.groupby(["claim_id", "condition_name", "routed_condition", "run_id", "candidate_plan_hash"])
        .agg(
            count_candidates_blocked=("candidate_id", "count"),
            example_candidate_ids=("candidate_id", lambda ids: ",".join(list(ids)[:3])),
        )
        .reset_index()
    )
    return grouped


def _collect_prior_blocked_counts(
    run_id: str,
    k: int = ROLLING_BLOCKED_K,
) -> Dict[str, int]:
    """Sum blocked_count across the K most recent prior runs (excluding current).

    Returns {claim_id: rolling_blocked_count}.
    """
    base = DATA_ROOT / "reports" / "atlas_verification"
    if not base.exists():
        return {}

    prior_artifacts: List[Tuple[float, Path]] = []
    for run_dir in base.iterdir():
        if run_dir.name == run_id or not run_dir.is_dir():
            continue
        p = run_dir / "claim_blocked_conditions.parquet"
        if p.exists():
            prior_artifacts.append((p.stat().st_mtime, p))

    # Most recent K runs
    selected = [path for _, path in sorted(prior_artifacts, reverse=True)[:k]]

    rolling: Dict[str, int] = {}
    for path in selected:
        try:
            df = pd.read_parquet(path)
        except Exception:
            continue
        if "claim_id" not in df.columns or "count_candidates_blocked" not in df.columns:
            continue
        for _, row in df.iterrows():
            cid = str(row["claim_id"])
            cnt = int(row.get("count_candidates_blocked", 0))
            rolling[cid] = rolling.get(cid, 0) + cnt

    return rolling


def _build_planner_exclusion_list(
    current_blocked: pd.DataFrame,
    rolling_counts: Dict[str, int],
    threshold: int = ROLLING_BLOCKED_THRESHOLD,
) -> List[Dict]:
    """Build exclusion entries for claims that exceed the rolling block threshold.

    Returns a list of dicts suitable for planner_excluded_claims.json.
    """
    if current_blocked.empty:
        return []

    # Merge current run's blocked counts into rolling totals
    current_totals: Dict[str, int] = {}
    for _, row in current_blocked.iterrows():
        cid = str(row["claim_id"])
        cnt = int(row.get("count_candidates_blocked", 0))
        current_totals[cid] = current_totals.get(cid, 0) + cnt

    exclusions = []
    for claim_id, current_cnt in current_totals.items():
        total_rolling = rolling_counts.get(claim_id, 0) + current_cnt
        if total_rolling >= threshold:
            # Collect a representative condition_name
            rows = current_blocked[current_blocked["claim_id"] == claim_id]
            cond_names = rows["condition_name"].unique().tolist() if not rows.empty else []
            exclusions.append({
                "claim_id": claim_id,
                "status": "blocked_non_executable_condition",
                "recommended_action": "add_condition_mapping_or_remove_condition",
                "blocked_condition_names": cond_names,
                "rolling_blocked_count": int(total_rolling),
                "current_run_blocked_count": int(current_cnt),
            })

    return sorted(exclusions, key=lambda x: -x["rolling_blocked_count"])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    parser.add_argument(
        "--candidate_plan_hash",
        default="",
        help="Hash of the candidate plan used in this run (for traceability).",
    )
    args = parser.parse_args()

    claim_map_path = PROJECT_ROOT.parent / "claim_test_map.csv"
    if not claim_map_path.exists():
        print(f"Claim map not found: {claim_map_path}", file=sys.stderr)
        return 1

    claim_map = pd.read_csv(claim_map_path)
    required_cols = {"claim_id", "test_id", "required_scope"}
    missing = required_cols - set(claim_map.columns)
    if missing:
        print(f"claim_test_map.csv missing columns: {sorted(missing)}", file=sys.stderr)
        return 1

    if claim_map.empty:
        print("claim_test_map.csv is empty — no claims to verify.")
        return 0

    # ── Step 1: Standard claim verification ──────────────────────────────────
    verification_rows = []
    for _, row in claim_map.iterrows():
        claim_id = str(row["claim_id"])
        test_id = str(row["test_id"])
        scope = str(row.get("required_scope", ""))

        lookup_fn = _LOOKUP_MAP.get(test_id, _DEFAULT_LOOKUP)
        metric_value, threshold, passed = lookup_fn(args.run_id, scope)

        status = "supported_in_env" if passed else "unsupported_in_env"
        if metric_value is None:
            status = "artifact_missing"

        verification_rows.append({
            "claim_id": claim_id,
            "test_id": test_id,
            "scope": scope,
            "run_id": args.run_id,
            "metric_value": metric_value,
            "threshold": threshold,
            "pass": passed,
            "status": status,
            "blocked_condition_count_rolling": 0,  # filled in below
        })

    verification_log = pd.DataFrame(verification_rows)

    out_dir = DATA_ROOT / "reports" / "atlas_verification" / args.run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Step 2: Blocked-condition feedback ────────────────────────────────────
    blocked_df = _load_phase2_blocked_rows(args.run_id)
    blocked_rows = _aggregate_blocked_conditions(
        args.run_id, blocked_df, args.candidate_plan_hash
    )

    blocked_path = out_dir / "claim_blocked_conditions.parquet"
    blocked_hash = ""
    if not blocked_rows.empty:
        blocked_rows.to_parquet(blocked_path, index=False)
        blocked_hash = _sha256_file(blocked_path)
        blocked_count = len(blocked_rows)
        print(f"Blocked conditions: {blocked_count} claim-condition pairs → {blocked_path}")
    else:
        # Write empty to establish the artifact even when nothing is blocked
        pd.DataFrame(columns=[
            "claim_id", "condition_name", "routed_condition", "run_id",
            "candidate_plan_hash", "count_candidates_blocked", "example_candidate_ids",
        ]).to_parquet(blocked_path, index=False)
        blocked_hash = _sha256_file(blocked_path)
        print("No blocked conditions found in Phase 2 output.")

    # ── Step 3: Rolling blocked count + status transition ────────────────────
    rolling_counts = _collect_prior_blocked_counts(args.run_id)

    # Merge rolling count into verification log
    if not blocked_rows.empty and "claim_id" in blocked_rows.columns:
        current_by_claim: Dict[str, int] = (
            blocked_rows.groupby("claim_id")["count_candidates_blocked"].sum().to_dict()
        )
        for vrow in verification_rows:
            cid = vrow["claim_id"]
            current_cnt = current_by_claim.get(cid, 0)
            rolling_cnt = rolling_counts.get(cid, 0) + current_cnt
            vrow["blocked_condition_count_rolling"] = int(rolling_cnt)
            if rolling_cnt >= ROLLING_BLOCKED_THRESHOLD and vrow["status"] not in (
                "supported_in_env",
            ):
                vrow["status"] = "blocked_non_executable_condition"

    verification_log = pd.DataFrame(verification_rows)

    # ── Step 4: Planner exclusion list ───────────────────────────────────────
    exclusions = _build_planner_exclusion_list(blocked_rows, rolling_counts)
    exclusions_path = out_dir / "planner_excluded_claims.json"
    exclusions_path.write_text(json.dumps(exclusions, indent=2), encoding="utf-8")
    if exclusions:
        print(f"Planner exclusions: {len(exclusions)} claims tagged → {exclusions_path}")
        for ex in exclusions:
            print(
                f"  EXCLUDED: {ex['claim_id']} — blocked_condition_names={ex['blocked_condition_names']}, "
                f"rolling_count={ex['rolling_blocked_count']}"
            )
    else:
        print(f"No planner exclusions.")

    # ── Step 5: Write primary artifacts ──────────────────────────────────────
    log_path = out_dir / "claim_verification_log.parquet"
    verification_log.to_parquet(log_path, index=False)
    log_bytes = log_path.stat().st_size

    prior_log = _find_prior_log(args.run_id)
    if prior_log is not None and "claim_id" in prior_log.columns and "status" in prior_log.columns:
        prior_status = prior_log.set_index("claim_id")["status"].to_dict()
        changed_rows = [
            r for r in verification_rows
            if prior_status.get(r["claim_id"]) != r["status"]
        ]
    else:
        changed_rows = verification_rows

    delta_df = pd.DataFrame(changed_rows)
    delta_path = out_dir / "claim_status_delta.parquet"
    delta_df.to_parquet(delta_path, index=False)
    delta_bytes = delta_path.stat().st_size

    print(f"Verification log: {len(verification_log)} rows, {log_bytes} bytes → {log_path}")
    print(f"Delta (changed):  {len(delta_df)} rows, {delta_bytes} bytes → {delta_path}")

    # ── Step 6: Update run manifest with all hashes ───────────────────────────
    manifest_path = DATA_ROOT / "runs" / args.run_id / "run_manifest.json"
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            manifest = {}

        log_hash = _sha256_file(log_path)
        delta_hash = _sha256_file(delta_path)

        manifest.setdefault("verification_hashes", {})
        vh = manifest["verification_hashes"]
        vh["claim_verification_log"] = log_hash
        vh["claim_verification_log_rows"] = int(len(verification_log))
        vh["claim_verification_log_bytes"] = int(log_bytes)
        vh["claim_status_delta"] = delta_hash
        vh["claim_status_delta_rows"] = int(len(delta_df))
        vh["claim_status_delta_bytes"] = int(delta_bytes)
        vh["blocked_conditions_hash"] = blocked_hash
        vh["blocked_conditions_path"] = str(blocked_path)
        vh["planner_excluded_claims_count"] = int(len(exclusions))

        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
        print(f"Updated manifest: {manifest_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
