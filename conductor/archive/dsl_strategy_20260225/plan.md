# Implementation Plan: DSL Strategy - Liquidity Dislocation Mean Reversion

## Phase 1: Setup and Contract Definition
- [x] **Task: Strategy Spec Definition**
    - [x] Create `spec/strategies/liquidity_dislocation_mr.yaml` defining the filters and actions.
    - [x] Verify the YAML adheres to the `strategy_dsl` v1 schema.

## Phase 2: Implementation and TDD
- [x] **Task: Test-Driven Development Setup**
    - [x] Create `tests/strategies/test_liquidity_dislocation_mr.py`.
    - [x] Implement test cases for:
        - Triggering on `LIQUIDITY_DISLOCATION` events.
        - Correctly filtering for `severity > 2.0`.
        - Blocking trades when `vol_regime == "SHOCK"`.
- [x] **Task: Minimal Strategy Implementation**
    - [x] Implement the strategy logic within the DSL execution path.
    - [x] Ensure the strategy correctly retrieves `vol_regime` and `severity` from the event/context.

## Phase 3: Integration and Verification
- [x] **Task: Backtest Execution**
    - [x] Run the full pipeline for a sample period: `python project/pipelines/run_all.py --run_id discovery_dislocation --symbols BTCUSDT --strategies liquidity_dislocation_mr`.
    - [x] Verify that trades are generated and cost estimates from `execution_model.py` are applied.
- [x] **Task: Final Hygiene and Audit**
    - [x] Run `make test-fast` to ensure no regressions.
    - [x] Run `make check-hygiene` to verify repository consistency.
    - [x] Document the results in a new report artifact.

## Phase: Review Fixes
- [x] Task: Apply review suggestions 4ff4800
控制项控制项