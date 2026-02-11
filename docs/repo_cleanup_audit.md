# Repo Cleanup Audit

## Scope

This audit covers repository hygiene, workflow consistency, and discovery pipeline ergonomics.

## Findings

1. Tracked data artifacts caused repository bloat and noisy diffs.
2. No first-class clean command existed for runtime-only cleanup versus full data reset.
3. `run_all.py` accepted only a single `--phase2_event_type`, forcing repetitive manual runs for multi-mechanism discovery.
4. Operator docs and Makefile flow were missing an explicit discovery-first "all mechanisms" command.
5. Ignore file had duplicated entries and minimal artifact coverage.
6. Discovery-first `--phase2_event_type all` had CLI contract inconsistencies (`--log_path`) across analyzers.
7. Phase 2 could fail hard on empty or missing Phase 1 event files, making all-mechanism discovery brittle.
8. Phase 2 control-merge path could fail when controls already carried a `symbol` column.
9. Funding ingest had a pandas deprecation path (`Series.view`) producing warning noise.

## Fixes Applied

1. Added multi-mechanism support to `run_all.py`:
   - `--phase2_event_type all`
   - comma-separated event types
   - parser-level validation for invalid types
2. Added cleanup utility script:
   - `project/scripts/clean_data.sh runtime`
   - `project/scripts/clean_data.sh all`
3. Expanded Makefile workflows:
   - `run`, `discover-edges`, `clean-runtime`, `clean-all-data`, `test`
4. Added root `Makefile` delegating to `project/Makefile`.
5. Updated docs (`README.md`, `docs/operator_runbook.md`) with discovery-first and cleanup workflows.
6. Simplified `.gitignore` and added `.pytest_cache/`.
7. Added `--log_path` compatibility to:
   - `analyze_cross_venue_desync.py`
   - `analyze_directional_exhaustion_after_forced_flow.py`
8. Hardened `phase2_conditional_hypotheses.py` to freeze gracefully (emit artifacts) when:
   - events CSV is empty
   - events CSV is missing
   - no events remain after symbol filtering
9. Hardened phase2 forward-opportunity control merge for controls that already include `symbol`.
10. Replaced deprecated funding timestamp conversion path with `astype("int64", copy=False)`.
11. Added regression tests for analyzer CLI contracts and phase2 empty/missing-event edge cases.
12. Implemented a template-bound dataset->mechanism hypothesis generator with anti-leak enforcement and fusion operators.
13. Added Binance sensor ingestors for:
   - liquidation snapshots
   - open interest history
14. Wired hypothesis generation and optional sensor ingestors into `run_all.py` and Makefile/README/runbook workflows.

## Residual Risks / Open Items

1. Historical data files remain tracked in git history; deleting current files reduces working tree size but does not shrink history.
2. If history-size reduction is required, repository history rewrite and remote coordination are needed.
3. Some placeholder edge specs are intentionally retained for contract tests and lifecycle gating; they are not runtime bloat.
