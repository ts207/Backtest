# Implementation Plan: Copula Pairs Trading event analyzer

## Phase 1: Statistical Foundation
- [ ] Task: Create `project/pipelines/_lib/stats_utils.py` with cointegration (Engle-Granger) and Kendall's Tau helpers.
- [ ] Task: Implement `project/pipelines/_lib/copula_utils.py` for Gaussian copula parameter estimation and conditional probability calculation.

## Phase 2: Analyzer Implementation
- [ ] Task: Create `project/pipelines/research/analyze_copula_pairs.py`.
- [ ] Task: Implement multi-asset data loader to align pairs from the lake.
- [ ] Task: Implement detection loop and event aggregation.

## Phase 3: Registration and Verification
- [ ] Task: Create `spec/events/COPULA_PAIRS_TRADING.yaml`.
- [ ] Task: Run verification on BTCUSDT/ETHUSDT pair and verify event counts.
- [ ] Task: Final hygiene: `make test-fast` and `make check-hygiene`.
