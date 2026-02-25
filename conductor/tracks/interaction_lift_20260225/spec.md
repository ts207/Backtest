# Specification: Interaction Lift Analysis

## Overview
Implement a diagnostic tool to measure the marginal performance lift provided by state-conditioning. This will quantify whether adding a "Context State" filter actually improves the baseline event performance or merely reduces sample size.

## Functional Requirements
- **Logic:** For every (Event Type, State) pair in discovery artifacts:
    - Identify the "Unconditioned" performance (Event Type, State="all").
    - Calculate the delta in Expectancy (bps) and Sharpe Ratio.
    - Compute a t-statistic for the difference in means between conditioned and unconditioned samples.
- **Reporting:** 
    - Materialize `lift_analysis.parquet` in the run's report directory.
    - Generate `top_lifts.md` highlighting the most significant positive interactions.

## Acceptance Criteria
- `analyze_interaction_lift.py` successfully executes on existing run IDs.
- Results clearly show which states (e.g., HIGH_VOL) provide the most predictive lift for specific event families.
- Lift calculation handles small-sample bias appropriately (e.g., min 30 trades).
