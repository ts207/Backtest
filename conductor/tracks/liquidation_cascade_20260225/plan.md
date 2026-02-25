# Implementation Plan: Liquidation Cascade Event Analyzer

## Phase 1: Setup and Contract Definition
- [x] Task: Register Event Specification
    - [x] Create `spec/events/LIQUIDATION_CASCADE.yaml`.
    - [x] Verify registry compatibility.

## Phase 2: Implementation and TDD
- [x] Task: Core Analyzer Implementation
    - [x] Create `project/pipelines/research/analyze_liquidation_cascade.py`.
    - [x] Implement detection logic using `liquidation_notional` and `oi_delta_1h`.
    - [x] Add event aggregation logic for multi-bar cascades.
- [x] Task: Unit Testing
    - [x] Create `tests/pipelines/research/test_liquidation_cascade.py` with synthetic flush patterns.

## Phase 3: Verification and Checkpointing
- [x] Task: Baseline Validation
    - [x] Run the analyzer on a sample period (e.g. BTCUSDT 2021-05-19).
    - [x] Export results to the report directory and verify row counts.
- [x] Task: Registry Integration
    - [x] Run `build_event_registry.py` for the new event type and verify `events.parquet` inclusion.
- [x] Task: Final Hygiene
    - [x] Run `make check-hygiene`.
