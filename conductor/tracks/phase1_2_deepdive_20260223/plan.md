# Implementation Plan: Phase 1 & 2 Deep Dive and Refinement

## Phase 1: Data Layer Audit and Fixes [checkpoint: b8da19b]
- [x] Task: Audit `project/pipelines/ingest/` and `project/pipelines/clean/` for timestamp alignment and PIT violations. [checkpoint: 6c3fc2f]
- [x] Task: TDD - Write unit tests for identified edge cases in cleaning logic. [checkpoint: 6c3fc2f]
- [x] Task: Implement fixes for identified data layer risks (e.g., funding scaling, bar alignment). [checkpoint: 6c3fc2f]
- [x] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md)

## Phase 2: Logic and Discovery Refinement
- [x] Task: Audit `project/events/` and `project/pipelines/research/` for logic consistency. [checkpoint: b8da19b]
- [x] Task: TDD - Create regression tests for event detection and statistical validation (BH-FDR). [checkpoint: b8da19b]
- [x] Task: Implement logic refinements for Phase 1 event triggers and Phase 2 discovery gates. [checkpoint: b8da19b]
- [ ] Task: Conductor - User Manual Verification 'Phase 2' (Protocol in workflow.md)

## Phase 3: Final Validation and Certification
- [ ] Task: Run full discovery pipeline on a 7-day slice (certification run).
- [ ] Task: Verify artifact schemas and provenance against `project/schemas/`.
- [ ] Task: Fix any remaining inconsistencies found during the certification run.
- [ ] Task: Conductor - User Manual Verification 'Phase 3' (Protocol in workflow.md)
