# Operator Runbook

This runbook contains three reference workflows:
1) baseline pipeline run
2) Phase 2 research run (single mechanism)
3) multi-edge validation run
4) discovery-first run (all mechanisms)
5) dataset-seeded hypothesis generation (non-leaking)

## 1) Baseline run (ingest -> report)

```bash
python3 project/pipelines/run_all.py \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10
```

Expected outputs:
- `data/runs/<run_id>/*.json|*.log`
- `data/lake/trades/backtests/vol_compression_expansion_v1/<run_id>/...`
- `data/reports/vol_compression_expansion_v1/<run_id>/summary.md`

## 2) Phase 2 research run

```bash
python3 project/pipelines/run_all.py \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10 \
  --run_phase2_conditional 1 \
  --phase2_event_type vol_shock_relaxation \
  --phase2_max_conditions 20 \
  --phase2_max_actions 9
```

Expected outputs:
- `data/reports/phase2/<run_id>/vol_shock_relaxation/phase2_candidates.csv`
- `data/reports/phase2/<run_id>/vol_shock_relaxation/phase2_summary.md`
- `data/reports/phase2/<run_id>/vol_shock_relaxation/promoted_candidates.json`

## 4) Discovery-first run (all mechanisms)

```bash
python3 project/pipelines/run_all.py \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10 \
  --run_backtest_baseline 0 \
  --run_make_report 0 \
  --run_phase2_conditional 1 \
  --phase2_event_type all \
  --run_edge_candidate_universe 1
```

Expected outputs:
- `data/reports/phase2/<run_id>/<event_type>/phase2_candidates.csv`
- `data/reports/edge_candidates/<run_id>/edge_candidates_normalized.csv`

## 5) Dataset-seeded hypothesis generation (non-leaking)

```bash
python3 project/pipelines/research/generate_hypothesis_queue.py \
  --run_id 20260210_000001 \
  --symbols BTCUSDT,ETHUSDT \
  --datasets auto \
  --max_fused 24
```

Expected outputs:
- `data/reports/hypothesis_generator/<run_id>/dataset_introspection.json`
- `data/reports/hypothesis_generator/<run_id>/template_hypotheses.json`
- `data/reports/hypothesis_generator/<run_id>/fusion_hypotheses.json`
- `data/reports/hypothesis_generator/<run_id>/phase1_hypothesis_queue.jsonl`

## Optional sensor ingest (Binance CM liquidationSnapshot + Binance OI)

```bash
python3 project/pipelines/ingest/ingest_binance_um_liquidation_snapshot.py \
  --run_id 20260210_000001 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10

python3 project/pipelines/ingest/ingest_binance_um_open_interest_hist.py \
  --run_id 20260210_000001 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10 \
  --period 5m
```

Hybrid one-command run:

```bash
python3 project/pipelines/run_all.py \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10 \
  --run_backtest_baseline 0 \
  --run_make_report 0 \
  --run_ingest_liquidation_snapshot 1 \
  --run_ingest_open_interest_hist 1 \
  --run_hypothesis_generator 1 \
  --run_phase2_conditional 1 \
  --phase2_event_type all \
  --run_edge_candidate_universe 1
```

## Data cleanup commands

```bash
# Remove run/report artifacts only
project/scripts/clean_data.sh runtime

# Remove all local data artifacts (raw/cleaned/features/trades/runs/reports)
project/scripts/clean_data.sh all
```

## 3) Multi-edge validation run

```bash
python3 project/pipelines/run_all.py \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10 \
  --run_edge_candidate_universe 1 \
  --run_multi_edge_portfolio 1 \
  --run_multi_edge_validation 1 \
  --multi_edge_symbols TOP10 \
  --multi_edge_modes equal_risk,score_weighted,constrained_optimizer
```

Expected outputs:
- `data/lake/trades/backtests/multi_edge_portfolio/<run_id>/metrics.json`
- `data/reports/multi_edge_portfolio/<run_id>/summary.md`
- `data/reports/multi_edge_validation/<run_id>/verdict.json`

## Strategy set update (P2 action)

The registry now supports:
- `vol_compression_v1`
- `vol_compression_momentum_v1`
- `vol_compression_reversion_v1`

You can run multiple strategies via:

```bash
python3 project/pipelines/backtest/backtest_vol_compression_v1.py \
  --run_id 20260210_000001 \
  --symbols BTCUSDT,ETHUSDT \
  --strategies vol_compression_v1,vol_compression_momentum_v1,vol_compression_reversion_v1
```
