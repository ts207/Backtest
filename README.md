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

## Strategy-builder outputs
- `data/reports/strategy_builder/<run_id>/strategy_candidates.json`
- `data/reports/strategy_builder/<run_id>/selection_summary.md`
- `data/reports/strategy_builder/<run_id>/manual_backtest_instructions.md`

## Manual backtest handoff
Backtests are intentionally manual after strategy builder output:
```bash
./.venv/bin/python project/pipelines/backtest/backtest_vol_compression_v1.py \
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

See `docs/operator_runbook.md`, `docs/strategy_builder.md`, and `docs/combined_model_1_3_architecture.md`.
