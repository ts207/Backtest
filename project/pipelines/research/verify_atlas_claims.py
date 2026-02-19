"""
Verify atlas claims against actual run artifacts.

For each row in claim_test_map.csv the script:
  1. Locates the test artifact for this run (JSON report).
  2. Reads the relevant metric field.
  3. Compares it against the threshold.
  4. Writes claim_verification_log.parquet.
  5. Computes a true delta against the prior run's log and writes
     claim_status_delta.parquet (only changed rows).
  6. Updates run_manifest.json with hashes and row/byte counts.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Artifact resolution
# Each test_id maps to a function that finds the relevant report and extracts
# (metric_value, threshold, passed).
# ---------------------------------------------------------------------------

def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _lookup_microstructure(run_id: str, scope: str) -> tuple[Optional[float], float, bool]:
    """T_MICROSTRUCTURE_ACCEPTANCE — checks gate_e1_pass in event_quality_report.json."""
    # scope is typically an event_type; try each event type report
    reports_root = DATA_ROOT / "reports"
    # search for event_quality_report.json in any event type sub-directory
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


def _lookup_multiplicity_fdr(run_id: str, scope: str) -> tuple[Optional[float], float, bool]:
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


def _lookup_conditional_expectancy(run_id: str, scope: str) -> tuple[Optional[float], float, bool]:
    """T_CONDITIONAL_EXPECTANCY — checks expectancy_exists in conditional_expectancy.json."""
    path = DATA_ROOT / "reports" / "expectancy" / run_id / "conditional_expectancy.json"
    payload = _read_json(path)
    if payload is None:
        return None, 1.0, False
    exists = bool(payload.get("expectancy_exists", False))
    # Report a count of significant findings as the metric value
    evidence = payload.get("expectancy_evidence", [])
    metric_value = float(len(evidence))
    threshold = 1.0  # at least one significant finding required
    return metric_value, threshold, exists


def _lookup_event_quality(run_id: str, scope: str) -> tuple[Optional[float], float, bool]:
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
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
        })

    verification_log = pd.DataFrame(verification_rows)

    out_dir = DATA_ROOT / "reports" / "atlas_verification" / args.run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "claim_verification_log.parquet"
    verification_log.to_parquet(log_path, index=False)
    log_bytes = log_path.stat().st_size

    # Compute true delta against the most recent prior run
    prior_log = _find_prior_log(args.run_id)
    if prior_log is not None and "claim_id" in prior_log.columns and "status" in prior_log.columns:
        prior_status = prior_log.set_index("claim_id")["status"].to_dict()
        changed_rows = [
            r for r in verification_rows
            if prior_status.get(r["claim_id"]) != r["status"]
        ]
    else:
        # No prior log → every row is "new"
        changed_rows = verification_rows

    delta_df = pd.DataFrame(changed_rows)
    delta_path = out_dir / "claim_status_delta.parquet"
    delta_df.to_parquet(delta_path, index=False)
    delta_bytes = delta_path.stat().st_size

    print(f"Verification log: {len(verification_log)} rows, {log_bytes} bytes → {log_path}")
    print(f"Delta (changed):  {len(delta_df)} rows, {delta_bytes} bytes → {delta_path}")

    # Update run_manifest.json
    manifest_path = DATA_ROOT / "runs" / args.run_id / "run_manifest.json"
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            manifest = {}

        log_hash = "sha256:" + hashlib.sha256(log_path.read_bytes()).hexdigest()
        delta_hash = "sha256:" + hashlib.sha256(delta_path.read_bytes()).hexdigest()

        manifest.setdefault("verification_hashes", {})
        manifest["verification_hashes"]["claim_verification_log"] = log_hash
        manifest["verification_hashes"]["claim_verification_log_rows"] = int(len(verification_log))
        manifest["verification_hashes"]["claim_verification_log_bytes"] = int(log_bytes)
        manifest["verification_hashes"]["claim_status_delta"] = delta_hash
        manifest["verification_hashes"]["claim_status_delta_rows"] = int(len(delta_df))
        manifest["verification_hashes"]["claim_status_delta_bytes"] = int(delta_bytes)

        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
        print(f"Updated manifest: {manifest_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
