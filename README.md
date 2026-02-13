# Backtest (Discovery + Strategy Builder + Manual Backtest)

Crypto research pipeline for futures/perp event discovery, edge promotion, and strategy candidate generation.  
This repository now uses one canonical flow:

1. Build canonical data/features/context
2. Run discovery and edge promotion
3. Build strategy candidates from promoted edges
4. Hand off to manual backtesting

## Scope
- Mainline runtime: ingest -> clean -> `features_v1` -> context (`funding_persistence` + `market_state`) -> phase1/phase2 -> edge export -> expectancy/robustness -> strategy builder.
- AlphaBundle runtime: parallel multi-signal research path (`project/pipelines/alpha_bundle/*`) evaluated with the same promotion standards.

## Setup
```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
export BACKTEST_DATA_ROOT=/abs/path/to/data
```

## Canonical discovery window
Standard discovery duration is:
- start: `2020-01-01`
- end: `2025-12-31`

## One-command discovery + strategy builder
```bash
./.venv/bin/python project/pipelines/run_all.py \
  --run_id edge_2020_2025_fresh \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-01-01 \
  --end 2025-12-31 \
  --run_hypothesis_generator 1 \
  --run_phase2_conditional 1 \
  --phase2_event_type all \
  --run_edge_candidate_universe 1 \
  --run_expectancy_analysis 1 \
  --run_expectancy_robustness 1 \
  --run_recommendations_checklist 1 \
  --run_strategy_builder 1
```

Cross-venue note:
- `cross_venue_desync` requires both perp and spot features.
- `run_all.py` now auto-runs spot ingest/clean/features when phase2 includes `cross_venue_desync` (or `all`).
- Disable spot archive download with `--skip_ingest_spot_ohlcv 1` if spot raw data already exists.

## What discovery actually does (stage order)
1. Ingest raw Binance data (perp OHLCV/funding, optional spot OHLCV for cross-venue).
2. Build cleaned bars and `features_v1` for each market.
3. Build context features (`funding_persistence`, `market_state`).
4. Run Phase 1 analyzers (event-family structure tests).
5. Run Phase 2 conditional hypotheses per event family (conditions/actions + gate decisions).
6. Export normalized edge candidates.
7. Run expectancy + robustness checks.
8. Build strategy candidates for manual backtests.

Key artifacts:
- phase1 outputs: `data/reports/<event_type>/<run_id>/...`
- phase2 outputs: `data/reports/phase2/<run_id>/<event_type>/...`
- promoted edges: `data/reports/edge_candidates/<run_id>/edge_candidates_normalized.csv`
- strategy candidates: `data/reports/strategy_builder/<run_id>/strategy_candidates.json`

## Why strategy candidates no longer share one base strategy
Strategy builder now maps event families to strategy templates (for example `funding_extreme_reversal_window -> funding_extreme_reversal_v1`).

`strategy_candidates.json` includes:
- `base_strategy`: template selected from event family.
- `backtest_ready`: `true` when adapter exists in strategy registry.

Current adapter-backed strategy ids include:
- `vol_compression_v1`
- `liquidity_refill_lag_v1`
- `liquidity_absence_gate_v1`
- `forced_flow_exhaustion_v1`
- `funding_extreme_reversal_v1`
- `cross_venue_desync_v1`
- `liquidity_vacuum_v1`

If `backtest_ready=false`, candidate is still valid research output, but needs adapter implementation before execution.

## Strategy-builder outputs
- `data/reports/strategy_builder/<run_id>/strategy_candidates.json`
- `data/reports/strategy_builder/<run_id>/selection_summary.md`
- `data/reports/strategy_builder/<run_id>/manual_backtest_instructions.md`

## Manual backtest handoff
Backtests are intentionally manual after strategy builder output:
```bash
./.venv/bin/python project/pipelines/backtest/backtest_strategies.py \
  --run_id <manual_backtest_run_id> \
  --symbols BTCUSDT,ETHUSDT \
  --strategies vol_compression_v1 \
  --force 1
```

## When to use AlphaBundle
Use AlphaBundle as an equal path when:
- you need cross-sectional or multi-signal composite alpha,
- you have a stable universe snapshot and labels,
- you will evaluate with the same robustness gates as mainline.

Do not default to AlphaBundle when:
- only a single event-family hypothesis is being validated,
- labels/panel alignment are incomplete,
- quick iteration is needed on a single mainline event strategy.

See `docs/operator_runbook.md`, `docs/strategy_builder.md`, `docs/discovery_pipeline.md`, and `docs/combined_model_1_3_architecture.md`.

## Liquidity vacuum gates (strict vs practical)
`analyze_liquidity_vacuum.py` now supports `--profile {strict,balanced,lenient}` (default: `balanced`) plus explicit threshold overrides.  
Use `strict` for conservative confirmation and `balanced/lenient` when event counts are too sparse.
