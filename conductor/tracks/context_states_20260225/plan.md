# Implementation Plan: Implement multi-dimensional context state vectors

## Phase 1: Logic and Builders
- [ ] Task: Update `project/pipelines/features/build_market_context.py` to calculate new state dimensions.
- [ ] Task: Implement the permutation logic for `ms_context_state_code`.
- [ ] Task: Register new columns in the market context schema.

## Phase 2: TDD and Verification
- [ ] Task: Create `tests/features/test_context_states.py` to verify categorical mappings and code generation.
- [ ] Task: Run a full feature build for a sample run ID and verify column materialization.

## Phase 3: DSL Integration Smoke Test
- [ ] Task: Create a sample DSL strategy that uses `ms_context_state_code` in its filters.
- [ ] Task: Verify the strategy correctly filters bars in a backtest run.
- [ ] Task: Final hygiene: `make test-fast` and `make check-hygiene`.
