# Architecture

Backtest is a subprocess-orchestrated pipeline centered on `project/pipelines/run_all.py`.

## High-Level Flow

1. Ingest market data (`pipelines/ingest/*`)
2. Build cleaned bars (`pipelines/clean/*`)
3. Build features/context (`pipelines/features/*`)
4. Run Phase1 analyzers (`pipelines/research/analyze_*.py`)
5. Build event registry (`build_event_registry.py`)
6. Run Phase2 discovery (`phase2_candidate_discovery.py`)
7. Bridge/economic checks (`bridge_evaluate_phase2.py`)
8. Candidate promotion and registry update
9. Optional compile/builder/backtest/eval/report

## Run Orchestration Model

`run_all.py` constructs an ordered stage list and executes stages as isolated subprocesses.

- Stage command-line wiring is explicit.
- Stage-level success/failure is tracked in `data/runs/<run_id>/`.
- Phase2 event families can expand into multiple stage instances.

## Stage Instance Tracing

Event-specific stages are tracked with instance IDs to avoid overwrite collisions.

- Example: `build_event_registry_LIQUIDITY_VACUUM`
- Instance timing is captured separately from logical stage timing.

This preserves per-event provenance and makes post-run audits reliable.

## Manifest and Audit Contracts

Run manifest (`run_manifest.json`) captures:

- spec hashes, ontology component hashes, git commit, data hash, config digest
- planned logical/instance stages
- logical/instance timing maps
- checklist/execution guard fields
- terminal artifact audit fields (`artifact_cutoff_utc`, late artifact metadata)

Stage manifests include stage/session identity propagated from orchestrator.

## Artifact Roots

All outputs are rooted at `BACKTEST_DATA_ROOT`.

- `data/runs/<run_id>/...`
- `data/events/<run_id>/...`
- `data/reports/<stage>/<run_id>/...`
- `data/lake/...`
