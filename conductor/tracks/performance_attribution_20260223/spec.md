# Specification: Performance Attribution and Economic Evaluation

## Overview
This track implements the logic to attribute the performance of Phase 2 trading candidates to specific market regimes and simulate realistic economic costs (slippage, fees, and market impact).

## Goals
- **Attribution**: Map candidate P&L to market volatility regimes and liquidity states.
- **Economic Modeling**: Implement a robust cost model including Binance-specific fees and slippage estimates.
- **Reporting**: Generate a "Performance Attribution Report" for each discovered candidate.

## Requirements
- Join Phase 2 candidates with PIT features (`vol_regime`, `liquidity_vacuum`).
- Calculate Sharpe Ratio and Drawdown per regime.
- Apply execution costs based on `project/configs/fees.yaml`.
- Export results to `attribution_reports.parquet`.

## Technical Constraints
- Must use existing PIT data structures.
- Must adhere to Spec-First design (add to `spec/concepts/performance_attribution.yaml` if needed).
