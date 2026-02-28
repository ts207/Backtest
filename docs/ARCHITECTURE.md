# Architecture

Backtest is a subprocess-orchestrated pipeline centered on `project/pipelines/run_all.py`.

## High-Level Flow

The orchestrator dynamically builds an execution graph from the following stage groups:

1. **Ingest Market Data** (`pipelines/ingest/*`): OHLCV (perp & spot), funding, open interest, and liquidations.
2. **Build Cleaned Data** (`pipelines/clean/*`): 5-minute bars (enforcing strict gap handling without arbitrary forward-fills), orderbook snapshots, and accurate discrete funding alignment.
3. **Build Features & Context** (`pipelines/features/*`): Generates orthogonal feature schemas, market contexts, and universe snapshots safely maintaining strict point-in-time constraints.
4. **Phase 1 Analyzers** (`pipelines/research/analyze_*.py`): Specialized domain detectors that run hypothesis scans to output raw event triggers (e.g. liquidity dislocations, volatility shocks).
5. **Event Registry Assembly** (`build_event_registry.py`): Collates analyzer outputs into `events.parquet` and unified bar-level `event_flags.parquet`.
6. **Phase 2 Conditional Discovery** (`phase2_candidate_discovery.py`): Performs large-scale hypothesis space exploration with rigorous BH-FDR multiplicity control, stratified inherently across the universe of symbols.
7. **Expectancy & Bridge Checks** (`aggregate_evaluation.py`, `bridge_evaluate_phase2.py`): Tests hypotheses across various dimensions (cost survivability, interaction lift, raw delay grid stability, and deep combinatorial robustness).
8. **Candidate Promotion & Compiling**: Filters viable candidates (`run_candidate_promotion.py`, `run_edge_registry_update.py`), then fuses them together using `compile_strategy_blueprints.py` and `run_strategy_builder.py`.
9. **Execution & Reporting**: Runs discrete backtesting (`backtest_strategies.py`), automated out-of-sample stress testing (`run_walkforward_eval.py`), and performance attribution reporting (`make_report.py`).

## Run Orchestration Model

`run_all.py` constructs an ordered stage list and executes stages as isolated subprocesses.

- Stage command-line wiring is explicit, highly configurable via toggles in the CLI.
- Stage-level success/failure is tracked deeply in `data/runs/<run_id>/`.
- Phase2 event families can gracefully expand into multiple discrete stage instances.
- Pre-flight checks (like the `ontology_consistency_audit.py`) ensure safe environments before kicking off expensive data processes.

## Stage Instance Tracing

Event-specific stages are tracked with instance IDs tightly scoped to the event to avoid overwrite collisions during massive multiplexing.

- Example: `build_event_registry_LIQUIDITY_VACUUM`
- Instance timing is fundamentally captured separately from logical stage timing inside execution manifests.

This preserves meticulous per-event provenance and guarantees post-run audits are consistently reliable.

## Manifest and Audit Contracts

Run manifests (`run_manifest.json`) orchestrate deep cryptographic provenance:

- Records: spec hashes, ontology component hashes, git commit, data hash (across lake snapshots), config digest.
- Delineates `planned_stages` vs instantiated `planned_stage_instances`.
- Stores `logical` vs `instance` highly-granular timing maps.
- Checklist/execution guard fields (enforcing `KEEP_RESEARCH` defaults to isolate hypothesis mining from overfit execution traces).
- Tracks terminal artifact audit fields (`artifact_cutoff_utc`, late artifact metadata, `ended_at`).

Stage manifests seamlessly include stage/session identity propagated from the central orchestrator via environment variables.

## Artifact Roots

All data outputs reside securely rooted at `BACKTEST_DATA_ROOT`.

- `data/runs/<run_id>/...`: Run-scoped diagnostic execution files, checklists, sub-manifests, and intermediate registers.
- `data/events/<run_id>/...`: Centralized canonical event structures.
- `data/reports/<stage>/<run_id>/...`: Strategy builder exports, Phase 2 data profiles, walkforward outputs, reports.
- `data/lake/...`: Primary storage array for heavily reused artifacts like `raw/binance/*` or engineered `features/`.
