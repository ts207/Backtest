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

### 3) Canonical command surface (run from repo root)

```bash
make run-core START=2023-01-01 END=2025-12-31 SYMBOLS=BTCUSDT,ETHUSDT
make run-research-vsr START=2023-01-01 END=2025-12-31 SYMBOLS=BTCUSDT,ETHUSDT
make run-full START=2023-01-01 END=2025-12-31 SYMBOLS=BTCUSDT,ETHUSDT
```

## Common developer commands

```bash
make help
make test
make smoke
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
- Run-scoped context: `data/lake/runs/<run_id>/context/...`
- Engine outputs + run metadata: `data/runs/<run_id>/...`
- Reports: `data/reports/...`

## Canonical workflow order

`run_all.py` executes explicit workflow semantics:

1. ingest (optional)
2. cleaned bars
3. base features
4. context features
5. phase1 → phase2 → checklist → promoted audits (`research` / `full`)
6. backtest + report (`core` / `full`)

## Contract verification

```bash
bash scripts/verify_run_contract.sh <RID> core
bash scripts/verify_run_contract.sh <RID> research directional_exhaustion_after_forced_flow
```

## Documentation index

See [`docs/README.md`](docs/README.md) for stable spec/report taxonomy.
