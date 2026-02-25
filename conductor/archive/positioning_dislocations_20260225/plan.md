# Implementation Plan: Funding and Basis Dislocation Analyzers

## Phase 1: Event Registration and Contracts
- [x] Task: Create `spec/events/FND_DISLOC.yaml` and `spec/events/BASIS_DISLOC.yaml`.
- [x] Task: Register new events in `project/pipelines/run_all.py` (PHASE2_EVENT_CHAIN).
- [x] Task: Conductor - User Manual Verification 'Contracts Setup' (Protocol in workflow.md)

## Phase 2: Funding Dislocation Implementation
- [x] Task: Create `project/pipelines/research/analyze_funding_dislocation.py`.
- [x] Task: Implement detection logic and threshold profiling.
- [x] Task: Create `tests/pipelines/research/test_funding_dislocation.py`.
- [x] Task: Conductor - User Manual Verification 'Funding Analyzer' (Protocol in workflow.md)

## Phase 3: Basis Dislocation Implementation
- [x] Task: Create `project/pipelines/research/analyze_basis_dislocation.py`.
- [x] Task: Implement perp-spot alignment and z-score detection.
- [x] Task: Create `tests/pipelines/research/test_basis_dislocation.py`.
- [x] Task: Conductor - User Manual Verification 'Basis Analyzer' (Protocol in workflow.md)

## Phase 4: Final Integration and Audit
- [x] Task: Run end-to-end event detection for a sample symbol (BTCUSDT).
- [x] Task: Final hygiene: `make test-fast` and `make check-hygiene`.
- [x] Task: Conductor - User Manual Verification 'Final Audit' (Protocol in workflow.md)
