# Implementation Plan: Enable statistical promotion for manual DSL strategies

## Phase 1: CLI and Adapter Setup
- [x] Task: Update `project/pipelines/research/promote_candidates.py` to add `--manual_spec_path`.
- [x] Task: Implement a loader for manual YAML specs that generates a virtual `Candidate` object.
- [x] Task: Conductor - User Manual Verification 'Phase 1 Setup' (Protocol in workflow.md)

## Phase 2: Statistical Evaluation Integration
- [x] Task: Modify the promotion loop to include manual candidates if present.
- [x] Task: Ensure manual candidates are passed through `evaluate_oos_consistency` and `evaluate_regime_stability`.
- [x] Task: Handle PASS/FAIL logic specifically for single manual candidates (ignoring cohort-based gates).
- [x] Task: Conductor - User Manual Verification 'Phase 2 Integration' (Protocol in workflow.md)

## Phase 3: Verification and Finalization
- [x] Task: Run a manual promotion test using `spec/strategies/liquidity_dislocation_mr.yaml`.
- [x] Task: Verify the generated report correctly reflects the PASS/FAIL status.
- [x] Task: Final hygiene: `make test-fast` and `make check-hygiene`.
- [x] Task: Conductor - User Manual Verification 'Phase 3 Finalization' (Protocol in workflow.md)

## Phase: Review Fixes
- [x] Task: Apply review suggestions 8990f6c
