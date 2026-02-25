# Implementation Plan: Copula Pairs Trading event analyzer

## Phase 1: Statistical Foundation
- [x] Task: Create `project/pipelines/_lib/stats_utils.py` with cointegration (Engle-Granger) and Kendall's Tau helpers.
- [x] Task: Implement `project/pipelines/_lib/copula_utils.py` for Gaussian copula parameter estimation and conditional probability calculation.

## Phase 2: Analyzer Implementation
- [x] Task: Create `project/pipelines/research/analyze_copula_pairs.py`.
- [x] Task: Implement multi-asset data loader to align pairs from the lake.
- [x] Task: Implement detection loop and event aggregation.

## Phase 3: Registration and Verification
- [x] Task: Create `spec/events/COPULA_PAIRS_TRADING.yaml`.
- [x] Task: Run verification on BTCUSDT/ETHUSDT pair and verify event counts.
- [x] Task: Final hygiene: `make test-fast` and `make check-hygiene`.
