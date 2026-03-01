---
task: Patch pipeline integrity issues from IterativeDepth analysis
slug: 20260228-000000_patch-pipeline-integrity
effort: Extended
phase: build
progress: 0/20
mode: ALGORITHM
started: 2026-02-28T00:00:00Z
updated: 2026-02-28T00:00:00Z
---

## Context

IterativeDepth (Literal + Failure lenses) on the backtest pipeline architecture surfaced
several silent-wrong-ness failure modes. Key findings:

- `allow_ontology_hash_mismatch=1` bypass is completely silent — no manifest record, no warning
- Candidate IDs auto-generated as `{event_type}_{idx}` without run_id, risking collision
- `promote_candidates.py` has no guard against promoting discovery-mode candidates
- BH-FDR applied per-run within `analyze_conditional_expectancy.py` — scoped by directory,
  but no explicit run_id validation in the correction function
- Funding convention "longs pay positive" is hardcoded without explicit exchange config

The user requested "patch" — fix the actionable integrity bugs surfaced by the analysis.

### Risks

- Ontology bypass audit: run_manifest already tracks `non_production_overrides`; adding
  `allow_ontology_hash_mismatch` to that list is a safe extension.
- Promotion guard: must NOT break the standard promotion flow; must only warn+block
  when run_mode==discovery, with an explicit override escape hatch.
- Candidate ID prefix: must preserve backward compat with downstream consumers that
  read candidate_id as a string; prefix change is non-breaking since it's additive.

## Criteria

- [ ] ISC-1: Warning printed to stderr when ontology hash bypass activated
- [ ] ISC-2: Bypass activation recorded in run_manifest non_production_overrides list
- [ ] ISC-3: Warning includes existing hash, current hash, and run_id
- [ ] ISC-4: Warning is printed before any stage runs, not after
- [ ] ISC-5: Bypass warning is not printed when hashes match (no false positive)
- [ ] ISC-6: Bypass warning is not printed when no existing manifest (new run, no mismatch)
- [ ] ISC-7: Auto-generated candidate IDs include run_id prefix when run_id arg present
- [ ] ISC-8: Auto-generated candidate IDs preserve event_name and index components
- [ ] ISC-9: Existing candidate_id column is NOT overwritten by auto-generation
- [ ] ISC-10: promote_candidates.py reads run_mode from run_manifest for the run_id
- [ ] ISC-11: Promotion raises error when run_mode==discovery and no override flag
- [ ] ISC-12: Error message names discovery mode as the rejection reason explicitly
- [ ] ISC-13: --allow_discovery_promotion flag added to promote_candidates argparser
- [ ] ISC-14: --allow_discovery_promotion=1 bypasses the discovery guard with warning
- [ ] ISC-15: run_mode guard only fires when run_manifest exists and run_mode is readable
- [ ] ISC-16: run_mode guard does not fire on missing manifest (graceful degradation)
- [ ] ISC-17: Funding convention documented as explicit constant with source comment
- [ ] ISC-18: pnl.py compute_pnl_components docstring updated to name convention assumption
- [ ] ISC-19: Tests in test_promote_candidates or existing test suite not broken
- [ ] ISC-20: Existing test_run_all or integration tests not broken by ontology patch

## Decisions

- Ontology bypass goes into `_STARTUP_NON_PROD_FLAGS` list + explicit stderr print
- Candidate ID prefix uses `{run_id}__{event_name}_{idx}` format (double underscore)
- Discovery promotion guard reads run manifest via existing `load_run_manifest_hashes`
- Funding: add a module-level comment, no behavioral change (convention is correct for Binance)

## Verification
