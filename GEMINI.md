# Backtest Project Context

## What This Repo Is
Backtest is a spec-driven, event-first trading research system for Binance perp/spot data.

Core entrypoint: `project/pipelines/run_all.py`

## Runtime Structure
- Data ingest and cleaning for 5m bar research datasets.
- Feature and context generation with PIT-safe joins.
- Phase 1 event analyzers by event family.
- Canonical event registry (`events.parquet`, `event_flags.parquet`).
- Phase 2 conditional discovery with multiplicity controls.
- Bridge tradability checks.
- Blueprint compile and optional downstream evaluation.

## Contracts That Matter
- Use registry outputs for event alignment (`*_event`, `*_active`).
- Enforce lagged entry for close-derived signals (`entry_lag_bars >= 1`).
- Keep missing-data states explicit (no false neutral fills).
- Treat specs in `spec/` as source of truth for concepts/events/gates.

## Working Commands
```bash
export BACKTEST_DATA_ROOT=$(pwd)/data
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt

make run
make discover-edges
make test-fast
./.venv/bin/python project/pipelines/run_all.py --help
```

## Doc Index
- `README.md`
- `docs/GETTING_STARTED.md`
- `docs/ARCHITECTURE.md`
- `docs/CONCEPTS.md`
- `docs/SPEC_FIRST.md`
- `docs/AUDIT.md`
