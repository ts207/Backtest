# Implementation Plan: Funding and Basis Dislocation Analyzers

## Phase 1: Event Registration and Contracts
- [ ] Task: Create `spec/events/FND_DISLOC.yaml` and `spec/events/BASIS_DISLOC.yaml`.
- [ ] Task: Register new events in `project/pipelines/run_all.py` (PHASE2_EVENT_CHAIN).
- [ ] Task: Conductor - User Manual Verification 'Contracts Setup' (Protocol in workflow.md)

## Phase 2: Funding Dislocation Implementation
- [ ] Task: Create `project/pipelines/research/analyze_funding_dislocation.py`.
- [ ] Task: Implement detection logic and threshold profiling.
- [ ] Task: Create `tests/pipelines/research/test_funding_dislocation.py`.
- [ ] Task: Conductor - User Manual Verification 'Funding Analyzer' (Protocol in workflow.md)

## Phase 3: Basis Dislocation Implementation
- [ ] Task: Create `project/pipelines/research/analyze_basis_dislocation.py`.
- [ ] Task: Implement perp-spot alignment and z-score detection.
- [ ] Task: Create `tests/pipelines/research/test_basis_dislocation.py`.
- [ ] Task: Conductor - User Manual Verification 'Basis Analyzer' (Protocol in workflow.md)

## Phase 4: Final Integration and Audit
- [ ] Task: Run end-to-end event detection for a sample symbol (BTCUSDT).
- [ ] Task: Final hygiene: `make test-fast` and `make check-hygiene`.
- [ ] Task: Conductor - User Manual Verification 'Final Audit' (Protocol in workflow.md)
