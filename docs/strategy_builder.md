# Strategy Builder Guide

`build_strategy_candidates.py` converts promoted edge candidates into manual-backtest strategy candidate artifacts.

## Inputs
- `data/reports/edge_candidates/<run_id>/edge_candidates_normalized.csv`
- referenced phase2 source artifacts (`promoted_candidates.json` or `phase2_candidates.csv`)
- optional AlphaBundle score artifact (`data/feature_store/alpha_bundle/alpha_bundle_scores.parquet` or `.csv`)

## Command
```bash
./.venv/bin/python project/pipelines/research/build_strategy_candidates.py \
  --run_id edge_2020_2025_fresh \
  --symbols BTCUSDT,ETHUSDT \
  --top_k_per_event 2 \
  --max_candidates 20 \
  --include_alpha_bundle 1
```

## Outputs
- `data/reports/strategy_builder/<run_id>/strategy_candidates.json`
- `data/reports/strategy_builder/<run_id>/selection_summary.md`
- `data/reports/strategy_builder/<run_id>/manual_backtest_instructions.md`

## Candidate structure
Each candidate includes:
- source metadata (`event`, `condition`, `action`, `status`)
- ranking metrics (`edge_score`, `stability_proxy`, `selection_score`)
- translated risk controls (`entry_delay_bars`, `size_scale`, `block_entries`)
- manual backtest command template

## Intended usage
- Use strategy-builder outputs as a deterministic handoff for manual backtests.
- Keep track-level parity: AlphaBundle and mainline candidates must pass the same gate philosophy.
