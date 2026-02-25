# Implementation Plan: VPIN and Roll Microstructure Metrics

## Phase 1: Setup and Contract Definition
- [x] Task: Update Feature Schema
    - [x] Add `ms_vpin_24` and `ms_roll_24` to `project/schemas/feature_schema_v1.json`.
- [x] Task: V1 Validation
    - [x] Ensure the schema change passes basic validation.

## Phase 2: Implementation and TDD
- [x] Task: Test-Driven Development Setup
    - [x] Create `tests/features/test_microstructure_metrics.py` with failing tests for VPIN and Roll calculations.
- [x] Task: Core Logic Implementation
    - [x] Create `project/pipelines/features/microstructure.py`.
    - [x] Implement `calculate_vpin`.
    - [x] Implement `calculate_roll`.
- [x] Task: Integration
    - [x] Update `project/pipelines/features/build_features_v1.py` to include the new metrics.

## Phase 3: Verification and Checkpointing
- [x] Task: Verification Run
    - [x] Run the feature build for a sample run and verify outputs.
- [x] Task: Final Hygiene and Audit
    - [x] Run `make test-fast` to ensure no regressions.
    - [x] Run `make check-hygiene` to verify repository consistency.
