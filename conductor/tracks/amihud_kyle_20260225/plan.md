# Implementation Plan: Implement Amihud and Kyle's Lambda microstructure metrics

## Phase 1: Setup and Contract Definition
- [ ] Task: Update `project/schemas/feature_schema_v1.json` to include new metrics.
- [ ] Task: Conductor - User Manual Verification 'Schema Setup' (Protocol in workflow.md)

## Phase 2: Implementation and TDD
- [ ] Task: Create `tests/features/test_microstructure_metrics.py` (or update existing).
- [ ] Task: Implement `calculate_amihud` and `calculate_kyle_lambda` in `project/features/microstructure.py`.
- [ ] Task: Integrate calculations into `project/pipelines/features/build_features_v1.py`.
- [ ] Task: Conductor - User Manual Verification 'Metric Implementation' (Protocol in workflow.md)

## Phase 3: Verification and Finalization
- [ ] Task: Run feature build for a sample run ID and verify outputs.
- [ ] Task: Final hygiene: `make test-fast` and `make check-hygiene`.
- [ ] Task: Conductor - User Manual Verification 'Final Verification' (Protocol in workflow.md)
