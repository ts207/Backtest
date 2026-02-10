# Operator Runbook

This runbook contains three reference workflows:
1) baseline pipeline run
2) Phase 2 research run
3) multi-edge validation run

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
