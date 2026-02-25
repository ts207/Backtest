# Specification: Enable statistical promotion for manual DSL strategies

## Overview
Extend the research promotion pipeline to support manually defined Strategy DSL YAML files. This ensures that human-hypothesized strategies are subjected to the same rigorous Out-of-Sample (OOS) and Regime Stability testing as discovery-generated candidates.

## Functional Requirements
- **CLI Extension:** Update `project/pipelines/research/promote_candidates.py` to accept a `--manual_spec_path` argument.
- **Candidate Emulation:** Implement a mapper to transform manual YAML specs into internal candidate representations compatible with the evaluation engine.
- **Statistical Gating:**
    - Perform **Out-of-Sample (OOS)** validation by comparing performance across temporal splits.
    - Perform **Regime Stability** validation to ensure the strategy remains robust across different volatility regimes (e.g., LOW, MID, HIGH).
- **Decision Logic:** Generate a "Promotion Decision" artifact indicating PASS/FAIL status based on pre-defined statistical thresholds in `spec/gates.yaml`.

## Non-Functional Requirements
- **Consistency:** Use the same evaluation functions as the automated discovery pipeline to prevent logic drift.
- **Traceability:** Record the manual spec hash in the promotion manifest for auditability.

## Acceptance Criteria
- `promote_candidates.py --manual_spec_path <path>` runs without error.
- Promotion report correctly evaluates OOS and Regime stability for the manual strategy.
- Decision follows the thresholds defined in `spec/gates.yaml`.

## Out of Scope
- Integration with the full BH-FDR correction (reserved for large-scale discovery cohorts).
- Automatic staging of manual strategies to the `production/` directory.
