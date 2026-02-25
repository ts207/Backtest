# Implementation Plan: DSL Strategy - Liquidity Dislocation Mean Reversion

## Phase 1: Setup and Contract Definition
- [ ] **Task: Strategy Spec Definition**
    - [ ] Create `spec/strategies/liquidity_dislocation_mr.yaml` defining the filters and actions.
    - [ ] Verify the YAML adheres to the `strategy_dsl` v1 schema.

## Phase 2: Implementation and TDD
- [ ] **Task: Test-Driven Development Setup**
    - [ ] Create `tests/strategies/test_liquidity_dislocation_mr.py`.
    - [ ] Implement test cases for:
        - Triggering on `LIQUIDITY_DISLOCATION` events.
        - Correctly filtering for `severity > 2.0`.
        - Blocking trades when `vol_regime == "SHOCK"`.
- [ ] **Task: Minimal Strategy Implementation**
    - [ ] Implement the strategy logic within the DSL execution path.
    - [ ] Ensure the strategy correctly retrieves `vol_regime` and `severity` from the event/context.

## Phase 3: Integration and Verification
- [ ] **Task: Backtest Execution**
    - [ ] Run the full pipeline for a sample period: `python project/pipelines/run_all.py --run_id discovery_dislocation --symbols BTCUSDT --strategies liquidity_dislocation_mr`.
    - [ ] Verify that trades are generated and cost estimates from `execution_model.py` are applied.
- [ ] **Task: Final Hygiene and Audit**
    - [ ] Run `make test-fast` to ensure no regressions.
    - [ ] Run `make check-hygiene` to verify repository consistency.
    - [ ] Document the results in a new report artifact.
控制项控制项