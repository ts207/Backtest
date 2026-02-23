# Implementation Plan: Performance Attribution and Economic Evaluation

## Phase 1: Foundation & Data Preparation
- [x] Task: Implement PIT Joiner for Attribution (4404aff)
    - [x] Write failing tests for joining candidates with regime features
    - [x] Implement `project/eval/attribution_joiner.py` to join candidates with PIT features
    - [x] Verify tests pass and coverage >80%
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Foundation & Data Preparation' (Protocol in workflow.md)

## Phase 2: Performance Attribution Logic
- [ ] Task: Calculate Regime-Specific Metrics
    - [ ] Write failing tests for per-regime P&L and Sharpe calculation
    - [ ] Implement metric aggregation in `project/eval/performance_attribution.py`
    - [ ] Verify tests pass and coverage >80%
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Performance Attribution Logic' (Protocol in workflow.md)

## Phase 3: Economic Evaluation (Bridge)
- [ ] Task: Integrate Cost Model
    - [ ] Write failing tests for fee and slippage application
    - [ ] Implement cost application logic using `project/configs/fees.yaml`
    - [ ] Verify tests pass and coverage >80%
- [ ] Task: Conductor - User Manual Verification 'Phase 3: Economic Evaluation (Bridge)' (Protocol in workflow.md)

## Phase 4: Reporting & Integration
- [ ] Task: Generate Attribution Reports
    - [ ] Write failing tests for Parquet report generation
    - [ ] Implement report export in `project/pipelines/report/performance_attribution_report.py`
    - [ ] Verify tests pass and coverage >80%
- [ ] Task: Conductor - User Manual Verification 'Phase 4: Reporting & Integration' (Protocol in workflow.md)
