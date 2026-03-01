"""
evaluation_guard.py
-------------------
Enforcement module for C_EVALUATION_MODE.

Checks all required invariants from the active hypothesis spec before any
claim-level artifact is produced. If any invariant fails, raises
EvaluationModeViolation — caller must not write the claim artifact.

Exploratory artifacts (lift_summary.csv, ablation_report.parquet) may always
be produced and are never gated by this module.

Usage (in eval/ablation.py, before writing lift_claim_report.parquet):

    from pipelines._lib.evaluation_guard import check_evaluation_mode

    result = check_evaluation_mode(
        run_id=args.run_id,
        project_root=PROJECT_ROOT,
        blueprints_path=blueprints_path,
        ablation_report_path=ablation_out / "ablation_report.parquet",
        phase2_report_root=phase2_root,
        embargo_days=args.embargo_days,
        cost_config_digest=cost_digest,
    )
    # result.evaluation_mode is True iff all invariants passed.
    # Raises EvaluationModeViolation if any invariant fails.

Usage (in eval/run_walkforward.py, before writing oos_claim_report.json):

    result = check_evaluation_mode(
        run_id=args.run_id,
        project_root=PROJECT_ROOT,
        blueprints_path=blueprints_path,
        embargo_days=args.embargo_days,
        cost_config_digest=cost_digest,
        skip_invariants=["INV_BH_APPLIED_TO_LIFT"],  # not applicable in walkforward
    )
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Sequence

import yaml
import pandas as pd


# ---------------------------------------------------------------------------
# Public exception
# ---------------------------------------------------------------------------

class EvaluationModeViolation(RuntimeError):
    """
    Raised when one or more required invariants fail.

    Attributes
    ----------
    failed_invariants : list of InvariantResult
        Every invariant that did not pass.
    all_results : list of InvariantResult
        Full results including passing invariants (for manifest recording).
    """

    def __init__(
        self,
        failed_invariants: list[InvariantResult],
        all_results: list[InvariantResult],
    ) -> None:
        self.failed_invariants = failed_invariants
        self.all_results = all_results
        lines = [
            "EvaluationModeViolation: claim artifact production is blocked.",
            f"  {len(failed_invariants)} invariant(s) failed:\n",
        ]
        for inv in failed_invariants:
            lines.append(f"  [{inv.id}] FAILED")
            lines.append(f"    reason      : {inv.failure_reason}")
            lines.append(f"    remediation : {inv.remediation}\n")
        super().__init__("\n".join(lines))


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------

@dataclass
class InvariantResult:
    id: str
    passed: bool
    failure_reason: Optional[str] = None
    remediation: Optional[str] = None
    checked_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "passed": self.passed,
            "failure_reason": self.failure_reason,
            "remediation": self.remediation,
            "checked_at": self.checked_at,
        }


@dataclass
class EvaluationModeResult:
    evaluation_mode: bool
    invariant_results: list[InvariantResult]
    checked_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    hypothesis_spec_path: str = ""

    def to_manifest_dict(self) -> dict:
        return {
            "evaluation_mode": self.evaluation_mode,
            "evaluation_mode_checked_at": self.checked_at,
            "evaluation_mode_hypothesis_spec": self.hypothesis_spec_path,
            "evaluation_mode_invariant_results": [
                r.to_dict() for r in self.invariant_results
            ],
        }


# ---------------------------------------------------------------------------
# Core public function
# ---------------------------------------------------------------------------

def check_evaluation_mode(
    *,
    run_id: str,
    project_root: Path,
    blueprints_path: Optional[Path] = None,
    ablation_report_path: Optional[Path] = None,
    phase2_report_root: Optional[Path] = None,
    embargo_days: int = 0,
    cost_config_digest: Optional[str] = None,
    skip_invariants: Sequence[str] = (),
    raise_on_failure: bool = True,
) -> EvaluationModeResult:
    """
    Check all required invariants from the active hypothesis spec.

    Parameters
    ----------
    run_id : str
        The current pipeline run identifier.
    project_root : Path
        Root of the repository (contains spec/).
    blueprints_path : Path, optional
        Path to blueprints.jsonl. Required for INV_NO_FALLBACK_IN_MEASUREMENT
        and INV_COST_DIGEST_UNIFORM.
    ablation_report_path : Path, optional
        Path to ablation_report.parquet. Required for INV_BH_APPLIED_TO_LIFT.
    phase2_report_root : Path, optional
        Root directory of phase2 reports for this run. Required for
        INV_SYMBOL_STRATIFIED_FAMILY.
    embargo_days : int
        The embargo_days used in walk-forward. Required for INV_EMBARGO_NONZERO.
    cost_config_digest : str, optional
        The expected cost_config_digest for this run. Required for
        INV_COST_DIGEST_UNIFORM.
    skip_invariants : sequence of str
        Invariant IDs to skip (e.g., walk-forward skips INV_BH_APPLIED_TO_LIFT
        because the ablation report is not yet produced at that point).
    raise_on_failure : bool
        If True (default), raise EvaluationModeViolation when any invariant fails.
        Set to False only in test contexts where you want to inspect the result.

    Returns
    -------
    EvaluationModeResult
        Structured result with evaluation_mode bool and per-invariant details.
        Call .to_manifest_dict() to get run_manifest-compatible output.

    Raises
    ------
    EvaluationModeViolation
        If raise_on_failure is True and any required invariant fails.
    """
    hypothesis_spec_path = (
        project_root / "spec" / "hypotheses" / "lift_state_conditioned_v1.yaml"
    )

    results: list[InvariantResult] = []
    skip_set = set(skip_invariants)

    # --- INV_HYPOTHESIS_REGISTERED -------------------------------------------
    if "INV_HYPOTHESIS_REGISTERED" not in skip_set:
        results.append(
            _check_hypothesis_registered(hypothesis_spec_path)
        )

    # --- INV_NO_FALLBACK_IN_MEASUREMENT --------------------------------------
    if "INV_NO_FALLBACK_IN_MEASUREMENT" not in skip_set:
        results.append(
            _check_no_fallback_in_measurement(blueprints_path)
        )

    # --- INV_BH_APPLIED_TO_LIFT ----------------------------------------------
    if "INV_BH_APPLIED_TO_LIFT" not in skip_set:
        results.append(
            _check_bh_applied_to_lift(ablation_report_path)
        )

    # --- INV_SYMBOL_STRATIFIED_FAMILY ----------------------------------------
    if "INV_SYMBOL_STRATIFIED_FAMILY" not in skip_set:
        results.append(
            _check_symbol_stratified_family(phase2_report_root, run_id)
        )

    # --- INV_EMBARGO_NONZERO -------------------------------------------------
    if "INV_EMBARGO_NONZERO" not in skip_set:
        results.append(
            _check_embargo_nonzero(embargo_days)
        )

    # --- INV_COST_DIGEST_UNIFORM ---------------------------------------------
    if "INV_COST_DIGEST_UNIFORM" not in skip_set:
        results.append(
            _check_cost_digest_uniform(blueprints_path, cost_config_digest)
        )

    # --- Build result --------------------------------------------------------
    failed = [r for r in results if not r.passed]
    evaluation_mode = len(failed) == 0

    result = EvaluationModeResult(
        evaluation_mode=evaluation_mode,
        invariant_results=results,
        hypothesis_spec_path=str(hypothesis_spec_path),
    )

    if not evaluation_mode and raise_on_failure:
        raise EvaluationModeViolation(
            failed_invariants=failed,
            all_results=results,
        )

    return result


# ---------------------------------------------------------------------------
# Individual invariant checks
# ---------------------------------------------------------------------------

def _check_hypothesis_registered(spec_path: Path) -> InvariantResult:
    inv_id = "INV_HYPOTHESIS_REGISTERED"
    remediation = (
        "spec/hypotheses/lift_state_conditioned_v1.yaml must exist "
        "with status: active before any claim artifact is produced."
    )
    if not spec_path.exists():
        return InvariantResult(
            id=inv_id,
            passed=False,
            failure_reason=f"Hypothesis spec not found at {spec_path}",
            remediation=remediation,
        )
    try:
        for candidate_path in sorted(
            list(phase2_report_root.rglob("phase2_candidates.parquet"))
            + list(phase2_report_root.rglob("phase2_candidates.csv"))
        ):
            if candidate_path.suffix.lower() == ".parquet":
                try:
                    df = pd.read_parquet(candidate_path, columns=["symbol", "family_id"])
                except Exception:
                    df = pd.read_parquet(candidate_path)
                if not df.empty:
                    for _, row in df.head(500 - checked).iterrows():
                        sym = str(row.get("symbol", "")).strip().upper()
                        if sym:
                            observed_symbols.add(sym)
                        fid = row.get("family_id", "")
                        if fid:
                            unchecked_rows.append((fid, sym))
                        checked += 1
                        if checked >= 500:
                            break
            else:
                import csv
                with open(candidate_path, newline="") as fh:
                    reader = csv.DictReader(fh)
                    for row in reader:
                        sym = str(row.get("symbol", "")).strip().upper()
                        if sym:
                            observed_symbols.add(sym)
                        fid = row.get("family_id", "")
                        if fid:
                            unchecked_rows.append((fid, sym))
                        checked += 1
                        if checked >= 500:
                            break
            if checked >= 500:
                break
    except Exception as exc:
        return InvariantResult(
            id=inv_id,
            passed=False,
            failure_reason=f"Failed to read phase2 reports: {exc}",
            remediation=remediation,
        )

    if checked == 0:
        return InvariantResult(
            id=inv_id,
            passed=False,
            failure_reason="No phase2_candidates.(parquet|csv) files found — cannot verify family stratification.",
            remediation=remediation,
        )

    if observed_symbols:
        # Build a precise regex from the actual symbols seen in this run.
        symbol_prefix_re = re.compile(
            r"^(" + "|".join(re.escape(s) for s in sorted(observed_symbols)) + r")_"
        )
        for fid, _ in unchecked_rows:
            if fid and not symbol_prefix_re.match(fid):
                bad_family_ids.append(fid)
    else:
        # No `symbol` column — fall back: family_id must start with an ALLCAPS word
        # (≥3 chars) followed by '_'. This catches the obvious un-stratified case.
        fallback_re = re.compile(r"^[A-Z]{3,}_")
        for fid, _ in unchecked_rows:
            if fid and not fallback_re.match(fid):
                bad_family_ids.append(fid)

    if bad_family_ids:
        unique_bad = list(dict.fromkeys(bad_family_ids))[:5]
        return InvariantResult(
            id=inv_id,
            passed=False,
            failure_reason=(
                f"{len(bad_family_ids)} family_id(s) (of {checked} sampled) lack symbol prefix. "
                f"Examples: {unique_bad}"
            ),
            remediation=remediation,
        )
    return InvariantResult(id=inv_id, passed=True)


def _check_embargo_nonzero(embargo_days: int) -> InvariantResult:
    inv_id = "INV_EMBARGO_NONZERO"
    remediation = (
        "Set --embargo_days default to 1 in eval/run_walkforward.py "
        "(argparse default=0 → default=1). "
        "Ensure the 60-day run passes --embargo_days >= 1."
    )
    if embargo_days < 1:
        return InvariantResult(
            id=inv_id,
            passed=False,
            failure_reason=(
                f"embargo_days={embargo_days}. Must be >= 1 to prevent autocorrelation "
                "bleed across train/validation boundary."
            ),
            remediation=remediation,
        )
    return InvariantResult(id=inv_id, passed=True)


def _check_cost_digest_uniform(
    blueprints_path: Optional[Path], expected_digest: Optional[str]
) -> InvariantResult:
    inv_id = "INV_COST_DIGEST_UNIFORM"
    remediation = (
        "Re-run all discovery stages with the same --cost_bps and --fees_bps flags. "
        "The cost_config_digest in all blueprints lineage must match the run's digest."
    )
    if blueprints_path is None or not blueprints_path.exists():
        return InvariantResult(
            id=inv_id,
            passed=False,
            failure_reason=(
                f"blueprints_path not provided or does not exist: {blueprints_path}. "
                "Cannot verify cost_config_digest uniformity."
            ),
            remediation=remediation,
        )
    digests_seen: set[str] = set()
    try:
        with open(blueprints_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                bp = json.loads(line)
                lineage = bp.get("lineage", {})
                digest = lineage.get("cost_config_digest", "")
                if digest:
                    digests_seen.add(digest)
    except Exception as exc:
        return InvariantResult(
            id=inv_id,
            passed=False,
            failure_reason=f"Failed to read blueprints for cost digest check: {exc}",
            remediation=remediation,
        )

    if len(digests_seen) > 1:
        return InvariantResult(
            id=inv_id,
            passed=False,
            failure_reason=(
                f"Found {len(digests_seen)} distinct cost_config_digest values in blueprints.jsonl. "
                "All blueprints in the measurement set must share one digest."
            ),
            remediation=remediation,
        )

    if expected_digest and digests_seen and expected_digest not in digests_seen:
        found = next(iter(digests_seen))
        return InvariantResult(
            id=inv_id,
            passed=False,
            failure_reason=(
                f"Blueprint cost_config_digest '{found}' does not match "
                f"run's expected digest '{expected_digest}'."
            ),
            remediation=remediation,
        )

    return InvariantResult(id=inv_id, passed=True)
