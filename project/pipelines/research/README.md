# Research Pipelines

This directory contains event discovery and strategy selection stages.

## Active Orchestrated Entry Points

Used by `project/pipelines/run_all.py`:

- Phase1 analyzers: `analyze_*.py` family scripts
- Registry build: `build_event_registry.py`
- Phase2 discovery: `phase2_candidate_discovery.py`
- Bridge: `bridge_evaluate_phase2.py`
- Candidate export: `export_edge_candidates.py`
- Blueprint compile: `compile_strategy_blueprints.py`

## Event Registry Contract

`build_event_registry.py` writes:

- `data/events/<run_id>/events.parquet`
- `data/events/<run_id>/event_flags.parquet`

Flags include both:

- `*_event` (impulse)
- `*_active` (window)

## Phase2 Contract (`phase2_candidate_discovery.py`)

- Loads registry events first.
- Prefers `enter_ts` for event-time alignment.
- Enforces `entry_lag_bars >= 1`.
- Applies family-level + global BH-FDR.
- Writes per-event-family outputs under:
  - `data/reports/phase2/<run_id>/<event_type>/`

## Notes on Legacy Files

`phase2_conditional_hypotheses.py` remains as legacy/refactor scaffolding and is not the orchestrated Phase2 entrypoint in current `run_all.py`.
