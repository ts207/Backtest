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

### 3) Run a full pipeline

```bash
python3 project/pipelines/run_all.py \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10
```

## Common developer commands

Use either the helper script or Make targets:

```bash
bash scripts/audit_repo.sh
make test
make audit
```

## Repository layout

- `project/pipelines/`: ingest, clean, feature engineering, backtest, report, and research stages.
- `project/engine/`: return and PnL computation + strategy aggregation.
- `project/strategies/`: strategy implementations and registry.
- `project/features/`: context/feature logic consumed by engine + pipelines.
- `tests/`: regression and contract tests.
- `docs/`: specifications, research locks, and reports.
- `edges/`: overlay/promotion edge definitions.

## Data output layout (default)

- Raw ingest: `data/lake/raw/...`
- Run-scoped cleaned bars: `data/lake/runs/<run_id>/cleaned/...`
- Run-scoped features: `data/lake/runs/<run_id>/features/...`
- Engine outputs: `data/runs/<run_id>/engine/...`
- Reports: `data/reports/...`

## Research flow (Phase 1 → Phase 2)

- **Phase 1**: event structure/mechanism validation only (no optimization loops).
- **Phase 2**: conditional edge hypotheses with explicit simplicity and stability gates.

Example:

```bash
python3 project/pipelines/research/analyze_vol_shock_relaxation.py \
  --run_id 20260101_120000 \
  --symbols BTCUSDT,ETHUSDT \
  --timeframe 15m

python3 project/pipelines/research/phase2_conditional_hypotheses.py \
  --run_id 20260101_120000 \
  --event_type vol_shock_relaxation \
  --symbols BTCUSDT,ETHUSDT
```

## Documentation index

See [`docs/README.md`](docs/README.md) for organized docs by category (architecture, features, analyzers, and reports).
