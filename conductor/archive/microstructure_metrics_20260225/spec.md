# Specification: VPIN and Roll Microstructure Metrics

## Overview
Implement VPIN (Volume-synchronized Probability of Informed Trading) and Roll's effective spread estimator. These features will be derived from OHLCV bar proxies to measure order flow toxicity and implicit transaction costs.

## Functional Requirements
- **VPIN Calculation:** Approximate volume buckets using bar data. Calculate VPIN over a 24-bar sliding window.
- **Roll Measure:** Estimate effective spread using the serial covariance of price changes over a 24-bar window.
- **PIT Safety:** All rolling calculations must be causal (no lookahead).

## Acceptance Criteria
- New features `ms_vpin_24` and `ms_roll_24` materialized in the feature lake.
- VPIN remains bounded between 0 and 1.
- Roll measure reflects positive spread estimates in trending/volatile markets.
