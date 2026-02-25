# Implementation Plan: VPIN and Roll Microstructure Metrics

## Phase 1: Setup and Contract Definition
- [ ] Task: Update Feature Schema
    - [ ] Add `ms_vpin_24` and `ms_roll_24` to `project/schemas/feature_schema_v1.json`.
- [ ] Task: V1 Validation
    - [ ] Ensure the schema change passes basic validation.

## Phase 2: Implementation and TDD
- [ ] Task: Test-Driven Development Setup
    - [ ] Create `tests/features/test_microstructure_metrics.py` with failing tests for VPIN and Roll calculations.
- [ ] Task: Core Logic Implementation
    - [ ] Create `project/pipelines/features/microstructure.py`.
    - [ ] Implement `calculate_vpin`.
    - [ ] Implement `calculate_roll`.
- [ ] Task: Integration
    - [ ] Update `project/pipelines/features/build_features_v1.py` to include the new metrics.

## Phase 3: Verification and Checkpointing
- [ ] Task: Verification Run
    - [ ] Run the feature build for a sample run and verify outputs.
- [ ] Task: Final Hygiene and Audit
    - [ ] Run `make test-fast` to ensure no regressions.
    - [ ] Run `make check-hygiene` to verify repository consistency.
