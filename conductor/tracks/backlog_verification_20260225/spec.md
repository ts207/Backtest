# Specification: Automated Backlog Verification

## Overview
Bridge the gap between research results and the source of truth for research hypotheses (`research_backlog.csv`). This track implements a mechanism to automatically update claim statuses based on the statistical outcomes of backtest runs.

## Functional Requirements
- **Status Mapping:**
    - `VERIFIED`: Associated candidate was promoted successfully.
    - `REJECTED`: Associated candidate failed statistical or economic gates.
    - `INSUFFICIENT_DATA`: Associated event had fewer than 30 samples.
- **Data Source:** Process `promoted_candidates.parquet` and `promotion_audit.csv` from the research report directory.
- **Traceability:**
    - Record the `run_id` and `candidate_id` in the `evidence_locator` column.
    - Update the `status` column in `research_backlog.csv`.
- **Safe Persistence:** Maintain a backup of the original `research_backlog.csv` before performing any updates.

## Acceptance Criteria
- Running `verify_backlog_claims.py --run_id <id>` updates the CSV correctly.
- Claims associated with promoted strategies are marked as `VERIFIED`.
- Claims with zero or low event counts remain `unverified` or are marked as `INSUFFICIENT_DATA`.
