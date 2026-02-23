# Implementation Plan: Atlas Improvements

## Phase 1: Template & State Expansion [checkpoint: f2b5f8f]
- [x] Task: TDD - Create tests for new Atlas template generation
- [x] Task: Implement `trend_continuation` archetype in `generate_candidate_templates.py`
- [x] Task: Implement `liquidity_reversion_v2` archetype in `generate_candidate_templates.py`
- [x] Task: TDD - Create tests for advanced state conditioning in candidate plans
- [x] Task: Update `generate_candidate_plan.py` to support `funding_bps`, `vpin`, and composite regimes
- [x] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md)

## Phase 2: Attribution-Aware Scoring [checkpoint: a636fa4]
- [x] Task: TDD - Create tests for attribution-aware ranking logic
- [x] Task: Implement logic to load regime attribution metrics in Atlas generator
- [x] Task: Implement `regime_attribution_score` calculation in ranking loop
- [x] Task: Update Atlas reporting to include attribution-aware metrics
- [x] Task: Conductor - User Manual Verification 'Phase 2' (Protocol in workflow.md)

## Phase 3: Verification & Lineage [checkpoint: a636fa4]
- [x] Task: TDD - Create tests for automated claim verification against Registry
- [x] Task: Update `verify_atlas_claims.py` to cross-reference with `Event Registry` flags
- [x] Task: Implement lineage metadata injection (concept IDs, versioning) in all Atlas outputs
- [x] Task: End-to-end pipeline validation with new Atlas logic enabled
- [x] Task: Conductor - User Manual Verification 'Phase 3' (Protocol in workflow.md)
