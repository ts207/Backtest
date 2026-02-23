# Specification: Phase 1 & 2 Deep Dive and Refinement

## Overview
This track involves a comprehensive audit and refinement of the discovery pipeline, covering everything from raw data ingestion (Phase 1) to statistical validation and economic evaluation (Phase 2). The primary goal is to ensure technical integrity, resolve latent bugs, and improve logic across the entire research lifecycle.

## Functional Requirements
1.  **Phase 1 Data Audit**: Rigorously audit ingestion, cleaning, and point-in-time (PIT) feature construction logic to eliminate any risk of lookahead bias or timestamp misalignment.
2.  **Phase 1 Logic Refinement**: Verify and improve event detection algorithms (e.g., liquidity shocks, vol regimes) to ensure they match microstructure theoretical expectations.
3.  **Phase 2 Statistical Validation**: Review and verify the BH-FDR multiplicity control implementation and candidate discovery gates for statistical correctness.
4.  **Phase 2 Economic Evaluation**: Audit the Bridge stage, including performance attribution and cost modeling, to ensure realistic expectancy estimations.
5.  **Refactoring & Bug Fixes**: Resolve all identified technical debt, silent data integrity risks, and logical inconsistencies across these phases.

## Acceptance Criteria
- [ ] Successful end-to-end execution of the full discovery pipeline (`make run` or equivalent).
- [ ] All code paths modified during the audit pass through the `spec_qa_linter.py` and unit tests.
- [ ] Verified consistency against the "Certification Batch" (Run ID: `certification_batch`) or equivalent golden baseline.
- [ ] No regressions in core invariants (PIT guards, Multiplicity Control).

## Out of Scope
-   Introducing new strategy archetypes or DSL extensions.
-   Modifying the core backtesting engine's fill/risk simulation logic.
