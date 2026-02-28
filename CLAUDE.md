# Backtest Assistant Context

## Project Summary
Backtest is an event-driven crypto research pipeline that discovers, filters, and compiles tradable strategy blueprints.

Primary orchestrator: `project/pipelines/run_all.py`

## Current Pipeline Flow
1. Ingest market datasets (OHLCV, optional funding/OI/liquidations, optional spot).
2. Build cleaned 5m bars.
3. Build `features_v1`.
4. Build context datasets (`funding_persistence`, `market_state`) and universe snapshots.
5. Run Phase 1 event analyzers.
6. Build event registry (`events.parquet`, `event_flags.parquet`).
7. Run Phase 2 candidate discovery.
8. Run bridge tradability checks.
9. Compile strategy blueprints and optional strategy builder artifacts.
10. Optional backtest, walkforward, promotion, and report stages.

## Integrity Contracts
- PIT joins use backward `merge_asof` with staleness limits.
- Event registry emits both `*_event` (impulse) and `*_active` (window) flags.
- Phase2 enforces `entry_lag_bars >= 1` to prevent same-bar fills.
- Bridge and walkforward defaults use 1-day embargo.
- Funding/context pipelines are fail-closed on missing critical coverage.

## Operator Commands

Environment:
```bash
export BACKTEST_DATA_ROOT=$(pwd)/data
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
```

Common runs:
```bash
make run
make discover-edges
make test-fast
./.venv/bin/python project/pipelines/run_all.py --help
```

## Current Canonical Docs
- `README.md`
- `docs/GETTING_STARTED.md`
- `docs/ARCHITECTURE.md`
- `docs/CONCEPTS.md`
- `docs/SPEC_FIRST.md`
- `docs/AUDIT.md`

Prefer those files for authoritative behavior and contracts.
