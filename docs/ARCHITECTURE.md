# Architecture Guide

Backtest is a subprocess-orchestrated pipeline. `project/pipelines/run_all.py` builds an ordered stage list and executes each stage as an independent script.

## Stage Graph

1. Ingest (`pipelines/ingest/*`)
2. Clean (`pipelines/clean/build_cleaned_5m.py`)
3. Features (`pipelines/features/build_features_v1.py`)
4. Context (`build_context_features.py`, `build_market_context.py`, `build_universe_snapshots.py`)
5. Phase1 analyzers (`pipelines/research/analyze_*.py`)
6. Registry (`pipelines/research/build_event_registry.py`)
7. Phase2 discovery (`pipelines/research/phase2_candidate_discovery.py`)
8. Bridge (`bridge_evaluate_phase2.py`)
9. Candidate export / blueprint compile / strategy builder
10. Optional backtest, walkforward, promotion, report

## Data Contracts

### Data root

All artifacts are rooted at `BACKTEST_DATA_ROOT` (default: `./data`).

### Registry contract

`build_event_registry.py` canonicalizes Phase1 rows into:
- `events.parquet`: normalized event table (`enter_ts`, `exit_ts`, `event_id`, `signal_column`)
- `event_flags.parquet`: symbol/time grid with:
  - impulse flags: `*_event`
  - active-window flags: `*_active`

### Phase2 timing contract

`phase2_candidate_discovery.py`:
- consumes registry events first,
- uses `enter_ts` for alignment when available,
- enforces `entry_lag_bars >= 1`.

## Integrity Controls

- PIT-safe backward joins with explicit tolerances.
- Funding coverage fail-closed in context/market-state.
- Family-level then global BH-FDR in Phase2.
- Cost/tradability gates before promotion.
- Run/stage manifests with config and provenance metadata.

## Main Runtime Paths

- Stage manifests: `data/runs/<run_id>/*.json`
- Registry artifacts: `data/events/<run_id>/`
- Phase2 artifacts: `data/reports/phase2/<run_id>/<event_type>/`
- Bridge artifacts: `data/reports/bridge_eval/<run_id>/<event_type>/`
- Blueprints: `data/reports/strategy_blueprints/<run_id>/`
