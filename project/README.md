# Crypto Research Pipeline

## Overview
This repository contains a deterministic, stage-based pipeline for crypto perpetual research.
Each stage accepts a `--run_id`, writes a manifest, and stores artifacts in `project/lake`.

## Run the full pipeline
```bash
python project/pipelines/run_all.py \
  --run_id 2024-01-01 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2023-01-01 \
  --end 2023-12-31
```

## Expected artifacts
- Raw data: `project/lake/raw/binance/perp/<symbol>/...`
- Cleaned bars: `project/lake/cleaned/perp/<symbol>/bars_15m/...`
- Features: `project/lake/features/perp/<symbol>/15m/features_v1/...`
- Trades and metrics: `project/lake/trades/backtests/vol_compression_expansion_v1/<run_id>/...`
- Report: `project/reports/vol_compression_expansion_v1/<run_id>/summary.md`
- Manifests: `project/runs/<run_id>/<stage>.json`

## Agent Mode
External orchestrators should call:
```bash
python project/pipelines/run_all.py --run_id <run_id> --symbols <symbols> --start <YYYY-MM-DD> --end <YYYY-MM-DD>
```
The pipeline exits non-zero on failure and is safe to re-run for the same `run_id`.
