# Implementation Plan: Liquidation Cascade Event Analyzer

## Phase 1: Setup and Contract Definition
- [ ] Task: Register Event Specification
    - [ ] Create `spec/events/LIQUIDATION_CASCADE.yaml`.
    - [ ] Verify registry compatibility.

## Phase 2: Implementation and TDD
- [ ] Task: Core Analyzer Implementation
    - [ ] Create `project/pipelines/research/analyze_liquidation_cascade.py`.
    - [ ] Implement detection logic using `liquidation_notional` and `oi_delta_1h`.
    - [ ] Add event aggregation logic for multi-bar cascades.
- [ ] Task: Unit Testing
    - [ ] Create `tests/pipelines/research/test_liquidation_cascade.py` with synthetic flush patterns.

## Phase 3: Verification and Checkpointing
- [ ] Task: Baseline Validation
    - [ ] Run the analyzer on a sample period (e.g. BTCUSDT 2021-05-19).
    - [ ] Export results to the report directory and verify row counts.
- [ ] Task: Registry Integration
    - [ ] Run `build_event_registry.py` for the new event type and verify `events.parquet` inclusion.
- [ ] Task: Final Hygiene
    - [ ] Run `make check-hygiene`.
