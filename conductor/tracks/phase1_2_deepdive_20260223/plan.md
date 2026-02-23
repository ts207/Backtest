# Implementation Plan: Phase 1 & 2 Deep Dive and Refinement

## Phase 1: Data Layer Audit and Fixes
- [x] Task: Audit `project/pipelines/ingest/` and `project/pipelines/clean/` for timestamp alignment and PIT violations. [checkpoint: 16e617b]
- [x] Task: TDD - Write unit tests for identified edge cases in cleaning logic. [checkpoint: 16e617b]
- [x] Task: Implement fixes for identified data layer risks (e.g., funding scaling, bar alignment). [checkpoint: 16e617b]
- [ ] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md)

## Phase 2: Logic and Discovery Refinement
- [ ] Task: Audit `project/events/` and `project/pipelines/research/` for logic consistency.
- [ ] Task: TDD - Create regression tests for event detection and statistical validation (BH-FDR).
- [ ] Task: Implement logic refinements for Phase 1 event triggers and Phase 2 discovery gates.
- [ ] Task: Conductor - User Manual Verification 'Phase 2' (Protocol in workflow.md)

## Phase 3: Final Validation and Certification
- [ ] Task: Run full discovery pipeline on a 7-day slice (certification run).
- [ ] Task: Verify artifact schemas and provenance against `project/schemas/`.
- [ ] Task: Fix any remaining inconsistencies found during the certification run.
- [ ] Task: Conductor - User Manual Verification 'Phase 3' (Protocol in workflow.md)
