# Implementation Plan: Performance Attribution and Economic Evaluation

## Phase 1: Foundation & Data Preparation [checkpoint: a8cecb0]
- [x] Task: Implement PIT Joiner for Attribution (4404aff)
    - [x] Write failing tests for joining candidates with regime features
    - [x] Implement `project/eval/attribution_joiner.py` to join candidates with PIT features
    - [x] Verify tests pass and coverage >80%
- [x] Task: Conductor - User Manual Verification 'Phase 1: Foundation & Data Preparation' (Protocol in workflow.md)

## Phase 2: Performance Attribution Logic [checkpoint: 82fb369]
- [x] Task: Calculate Regime-Specific Metrics (ad731c7)
    - [x] Write failing tests for per-regime P&L and Sharpe calculation
    - [x] Implement metric aggregation in `project/eval/performance_attribution.py`
    - [x] Verify tests pass and coverage >80%
- [x] Task: Conductor - User Manual Verification 'Phase 2: Performance Attribution Logic' (Protocol in workflow.md)

## Phase 3: Economic Evaluation (Bridge) [checkpoint: 47f86b0]
- [x] Task: Integrate Cost Model (0436c1e)
    - [x] Write failing tests for fee and slippage application
    - [x] Implement cost application logic using `project/configs/fees.yaml`
    - [x] Verify tests pass and coverage >80%
- [x] Task: Conductor - User Manual Verification 'Phase 3: Economic Evaluation (Bridge)' (Protocol in workflow.md)

## Phase 4: Reporting & Integration [checkpoint: a259f94]
- [x] Task: Generate Attribution Reports (58d271c)
    - [x] Write failing tests for Parquet report generation
    - [x] Implement report export in `project/pipelines/report/performance_attribution_report.py`
    - [x] Verify tests pass and coverage >80%
- [x] Task: Conductor - User Manual Verification 'Phase 4: Reporting & Integration' (Protocol in workflow.md)
