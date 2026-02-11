# Backtest (Discovery + Optional Execution Validation)

Crypto research pipeline for BTC/ETH and related futures sensors, with optional strategy backtesting/reporting.

## Scope
The active codebase supports:
- market data ingestion
- cleaned 15m canonical bars
- feature/context builds
- hypothesis generation
- Phase1/Phase2 event discovery
- edge candidate export
- conditional expectancy + robustness checks
- optional single-strategy backtesting (`vol_compression_v1`)
- optional backtest report generation

## Setup
```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
```

## Data root
Default data root is `data/` at repo root.

```bash
export BACKTEST_DATA_ROOT=/abs/path/to/data
```

## One-command discovery run
```bash
./.venv/bin/python project/pipelines/run_all.py \
  --run_id 20260212_000001 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10 \
  --run_hypothesis_generator 1 \
  --run_phase2_conditional 1 \
  --phase2_event_type all \
  --run_edge_candidate_universe 1 \
  --run_expectancy_analysis 1 \
  --run_expectancy_robustness 1
```

## Optional backtest + report
```bash
./.venv/bin/python project/pipelines/run_all.py \
  --run_id 20260212_000001 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10 \
  --strategies vol_compression_v1 \
  --run_backtest 1 \
  --run_make_report 1
```

## Key outputs
- `data/runs/<run_id>/*.json|*.log`
- `data/reports/hypothesis_generator/<run_id>/...`
- `data/reports/phase2/<run_id>/<event_type>/...`
- `data/reports/edge_candidates/<run_id>/edge_candidates_normalized.csv`
- `data/reports/expectancy/<run_id>/conditional_expectancy.json`
- `data/reports/expectancy/<run_id>/conditional_expectancy_robustness.json`
- `data/lake/trades/backtests/vol_compression_expansion_v1/<run_id>/metrics.json`
- `data/reports/vol_compression_expansion_v1/<run_id>/summary.md`

## Optional ingestion sensors
```bash
./.venv/bin/python project/pipelines/ingest/ingest_binance_um_liquidation_snapshot.py \
  --run_id 20260212_000001 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10

./.venv/bin/python project/pipelines/ingest/ingest_binance_um_open_interest_hist.py \
  --run_id 20260212_000001 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10 \
  --period 5m
```

## Clean local artifacts
```bash
project/scripts/clean_data.sh runtime
project/scripts/clean_data.sh all
project/scripts/clean_data.sh repo
```
