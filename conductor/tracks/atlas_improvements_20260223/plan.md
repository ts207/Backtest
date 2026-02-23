# Implementation Plan: Atlas Improvements

## Phase 1: Template & State Expansion
- [ ] Task: TDD - Create tests for new Atlas template generation
- [ ] Task: Implement `trend_continuation` archetype in `generate_candidate_templates.py`
- [ ] Task: Implement `liquidity_reversion_v2` archetype in `generate_candidate_templates.py`
- [ ] Task: TDD - Create tests for advanced state conditioning in candidate plans
- [ ] Task: Update `generate_candidate_plan.py` to support `funding_bps`, `vpin`, and composite regimes
- [ ] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md)

## Phase 2: Attribution-Aware Scoring
- [ ] Task: TDD - Create tests for attribution-aware ranking logic
- [ ] Task: Implement logic to load regime attribution metrics in Atlas generator
- [ ] Task: Implement `regime_attribution_score` calculation in ranking loop
- [ ] Task: Update Atlas reporting to include attribution-aware metrics
- [ ] Task: Conductor - User Manual Verification 'Phase 2' (Protocol in workflow.md)

## Phase 3: Verification & Lineage
- [ ] Task: TDD - Create tests for automated claim verification against Registry
- [ ] Task: Update `verify_atlas_claims.py` to cross-reference with `Event Registry` flags
- [ ] Task: Implement lineage metadata injection (concept IDs, versioning) in all Atlas outputs
- [ ] Task: End-to-end pipeline validation with new Atlas logic enabled
- [ ] Task: Conductor - User Manual Verification 'Phase 3' (Protocol in workflow.md)
