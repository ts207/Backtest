# Operator Runbook (Discovery Core)

This runbook is discovery-only.

## 1) Build datasets and core features
```bash
./.venv/bin/python project/pipelines/run_all.py \
  --run_id 20260212_000001 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10
```

Expected outputs:
- `data/runs/<run_id>/*.json|*.log`
- `data/lake/runs/<run_id>/cleaned/...`
- `data/lake/runs/<run_id>/features/...`
- `data/features/context/funding_persistence/...`

## 2) Generate non-leaking hypothesis queue
```bash
./.venv/bin/python project/pipelines/research/generate_hypothesis_queue.py \
  --run_id 20260212_000001 \
  --symbols BTCUSDT,ETHUSDT \
  --datasets auto \
  --max_fused 24
```

Expected outputs:
- `data/reports/hypothesis_generator/<run_id>/dataset_introspection.json`
- `data/reports/hypothesis_generator/<run_id>/template_hypotheses.json`
- `data/reports/hypothesis_generator/<run_id>/fusion_hypotheses.json`
- `data/reports/hypothesis_generator/<run_id>/phase1_hypothesis_queue.jsonl`

## 3) Run Phase1 + Phase2 across all mechanisms and export edge candidates
```bash
./.venv/bin/python project/pipelines/run_all.py \
  --run_id 20260212_000001 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10 \
  --run_hypothesis_generator 1 \
  --run_phase2_conditional 1 \
  --phase2_event_type all \
  --run_edge_candidate_universe 1
```

Expected outputs:
- `data/reports/phase2/<run_id>/<event_type>/phase2_candidates.csv`
- `data/reports/phase2/<run_id>/<event_type>/phase2_summary.md`
- `data/reports/edge_candidates/<run_id>/edge_candidates_normalized.csv`

## 4) Run expectancy and robustness checks
```bash
./.venv/bin/python project/pipelines/research/analyze_conditional_expectancy.py \
  --run_id 20260212_000001 \
  --symbols BTCUSDT,ETHUSDT

./.venv/bin/python project/pipelines/research/validate_expectancy_traps.py \
  --run_id 20260212_000001 \
  --symbols BTCUSDT,ETHUSDT
```

Expected outputs:
- `data/reports/expectancy/<run_id>/conditional_expectancy.json`
- `data/reports/expectancy/<run_id>/conditional_expectancy_robustness.json`

## 5) Optional sensor ingestion
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

## Cleanup
```bash
project/scripts/clean_data.sh runtime
project/scripts/clean_data.sh all
```
