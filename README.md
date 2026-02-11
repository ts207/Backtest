# Backtest

A modular crypto-perpetual backtesting and research repository with run-scoped data pipelines, deterministic tests, and phase-gated research analyzers.

## Quick start

### 1) Install

```bash
python3 -m pip install -r requirements.txt
python3 -m pip install -r requirements-dev.txt
```

### 2) Optional data root override

```bash
export BACKTEST_DATA_ROOT=/absolute/path/to/backtest-data
```

### 3) Run with explicit objective targets

```bash
make run-core            # core production-ish flow
make run-research-vsr    # research flow for vol_shock_relaxation
make run-full            # core + research
```

## Common developer commands

```bash
make help
make test
make audit
```

## Repository layout

- `project/pipelines/`: ingest, clean, feature engineering, backtest, report, and research stages.
- `project/engine/`: return and PnL computation + strategy aggregation.
- `project/strategies/`: strategy implementations and registry.
- `project/features/`: context/feature logic consumed by engine + pipelines.
- `tests/`: regression and contract tests.
- `docs/spec/`: contracts, schemas, and interpretation locks.
- `docs/report/`: run analyses and audit narratives.
- `edges/`: overlay/promotion edge definitions.

## Data output layout (default)

- Raw ingest: `data/lake/raw/...`
- Run-scoped cleaned bars: `data/lake/runs/<run_id>/cleaned/...`
- Run-scoped features: `data/lake/runs/<run_id>/features/...`
- Engine outputs: `data/runs/<run_id>/engine/...`
- Reports: `data/reports/...`

## Canonical workflow order

`run_all.py` (and Make targets) execute with explicit workflow semantics:

1. ingest (optional)
2. cleaned bars
3. features
4. context
5. research stages (for `research` and `full`)
6. backtest + report (for `core` and `full`)

Use explicit targets (`run-core`, `run-research-*`, `run-full`) instead of a generic run command to avoid ambiguous artifacts.

## Documentation index

See [`docs/README.md`](docs/README.md) for organized docs by category (architecture, features, analyzers, and reports).
