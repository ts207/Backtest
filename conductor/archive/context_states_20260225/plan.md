# Implementation Plan: Implement multi-dimensional context state vectors

## Phase 1: Logic and Builders
- [x] Task: Update `project/pipelines/features/build_market_context.py` to calculate new state dimensions.
- [x] Task: Implement the permutation logic for `ms_context_state_code`.
- [x] Task: Register new columns in the market context schema.

## Phase 2: TDD and Verification
- [x] Task: Create `tests/features/test_context_states.py` to verify categorical mappings and code generation.
- [x] Task: Run a full feature build for a sample run ID and verify column materialization.

## Phase 3: DSL Integration Smoke Test
- [x] Task: Create a sample DSL strategy that uses `ms_context_state_code` in its filters.
- [x] Task: Verify the strategy correctly filters bars in a backtest run.
- [x] Task: Final hygiene: `make test-fast` and `make check-hygiene`.
