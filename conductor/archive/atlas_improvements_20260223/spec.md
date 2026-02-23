# Specification: Atlas Improvements

## Overview
This track focuses on refining the **Atlas** hypothesis generation system. The goal is to enhance the core generation logic, expand the variety of strategy templates, and improve the system's ability to condition hypotheses on complex market states. Critically, this update integrates the recently implemented **Performance Attribution** metrics to allow Atlas to prioritize candidates based on historical regime-specific performance.

## Functional Requirements
1.  **Template Expansion**: Expand `generate_candidate_templates.py` to include new strategy archetypes:
    -   `trend_continuation`: Capturing momentum in high-volatility regimes.
    -   `liquidity_reversion_v2`: Improved mean reversion using rolling microstructure metrics.
2.  **Granular Conditioning**: Enhance `generate_candidate_plan.py` to support conditioning on:
    -   `funding_bps` (Carry state).
    -   `vpin` (Toxic flow proxy).
    -   `regime_vol_liquidity` (Composite state).
3.  **Attribution-Aware Scoring**: Integrate metrics from `performance_attribution.py` into the Atlas candidate ranking logic. Atlas should favor hypothesis types that have shown stability in the current or targeted market regimes.
4.  **Automated Claim Verification**: Upgrade `verify_atlas_claims.py` to cross-reference Atlas claims against the live `Event Registry`, ensuring all generated candidates are executable.
5.  **Enhanced Traceability**: Ensure all generated artifacts (Templates, Plans) include robust lineage metadata, including `source_concept_id` and logic versioning.

## Acceptance Criteria
- [ ] New templates are verified and correctly assigned a `rule_template` in Phase 2 outputs.
- [ ] Candidate plans correctly emit variants for funding and liquidity regimes.
- [ ] The Atlas report contains a `regime_attribution_score` for each candidate.
- [ ] `verify_atlas_claims.py` successfully validates 100% of candidates against the Registry flags.
- [ ] The end-to-end `run_all.py` pipeline completes with the new Atlas logic enabled.

## Out of Scope
-   Modifying the engine's cost model or fill simulation.
-   Adding new data ingestion sources.
