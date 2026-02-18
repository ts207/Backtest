# Strategy Builder Guide

`project/pipelines/research/build_strategy_candidates.py` converts edge universe rows into ranked strategy-candidate handoff artifacts.

## Inputs

Primary input:
- `data/reports/edge_candidates/<run_id>/edge_candidates_normalized.csv`

Optional enrichment:
- `data/feature_store/alpha_bundle/alpha_bundle_scores.parquet` or `.csv`

## Command

```bash
./.venv/bin/python project/pipelines/research/build_strategy_candidates.py \
  --run_id RUN \
  --symbols BTCUSDT,ETHUSDT \
  --top_k_per_event 2 \
  --max_candidates 20 \
  --include_alpha_bundle 1
```

## Outputs

- `data/reports/strategy_builder/<run_id>/strategy_candidates.json`
- `data/reports/strategy_builder/<run_id>/deployment_manifest.json`
- `data/reports/strategy_builder/<run_id>/selection_summary.md`
- `data/reports/strategy_builder/<run_id>/manual_backtest_instructions.md`

## Empty Output Interpretation

If outputs are empty, check:
- upstream edge universe exists,
- checklist did not gate out all candidates,
- stage manifest stats (`edge_rows_seen`, `strategy_candidate_count`).

## Candidate Fields (core)

- source metadata (`event`, `condition`, `action`, `status`)
- ranking fields (`edge_score`, `stability_proxy`, `selection_score`)
- execution routing (`execution_family`, `base_strategy`)
- readiness (`backtest_ready`, `backtest_ready_reason`)
- risk controls (`entry_delay_bars`, size/overlay controls)

## Routing Policy

The builder uses explicit event-family routing. Unknown families fail closed:
- `execution_family=unmapped`
- `base_strategy=unmapped`
- `backtest_ready=false`

## Handoff

Use `manual_backtest_instructions.md` to run controlled backtests, then apply walkforward and promotion gates before any deployment decision.
