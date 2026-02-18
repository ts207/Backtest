# Backtest Project Guide

This repository is a research and backtesting system for crypto event-driven strategy discovery.

It supports:
- ingestion and normalization of perp/spot data,
- feature + context construction,
- phase-1/phase-2 edge discovery,
- strategy blueprint compilation (DSL),
- optional backtesting, walkforward evaluation, promotion gating, and reporting.

## 1) Quick Start

### Prerequisites
- Python 3.10+
- Linux/macOS shell

### Environment setup
```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
```

### Data root
Always set this explicitly:
```bash
export BACKTEST_DATA_ROOT=/home/tstuv/backtest/Backtest/data
```

## 2) Canonical Commands

### A) Discovery-only run (default downstream-off)
This runs ingest -> clean -> features -> context -> phase1/phase2 -> blueprint/compiler/builder.
```bash
BACKTEST_DATA_ROOT=/home/tstuv/backtest/Backtest/data \
./.venv/bin/python project/pipelines/run_all.py \
  --run_id RUN \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-01-01 \
  --end 2025-12-31 \
  --run_phase2_conditional 1 \
  --phase2_event_type all
```

### B) Full end-to-end run (includes backtest + walkforward + promotion + report)
```bash
BACKTEST_DATA_ROOT=/home/tstuv/backtest/Backtest/data \
./.venv/bin/python project/pipelines/run_all.py \
  --run_id RUN_FULL \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-01-01 \
  --end 2025-12-31 \
  --run_phase2_conditional 1 \
  --phase2_event_type all \
  --run_edge_candidate_universe 1 \
  --run_backtest 1 \
  --run_walkforward_eval 1 \
  --run_make_report 1 \
  --blueprints_path data/reports/strategy_blueprints/RUN_FULL/blueprints.jsonl \
  --blueprints_top_k 10 \
  --blueprints_filter_event_type all \
  --fees_bps 4 \
  --slippage_bps 2 \
  --cost_bps 6
```

### C) Reuse local raw data without re-downloading
```bash
--skip_ingest_ohlcv 1 --skip_ingest_funding 1 --skip_ingest_spot_ohlcv 1 --force 0
```

## 3) Important `run_all.py` Defaults

From current code:
- `--run_backtest`: `0`
- `--run_walkforward_eval`: `0`
- `--run_make_report`: `0`
- `--run_strategy_blueprint_compiler`: `1`
- `--run_strategy_builder`: `1`
- `--run_recommendations_checklist`: `1`

Implication: a plain discovery run will not generate backtest/eval/promotion/final report artifacts unless you enable downstream flags.

## 4) Pipeline Stages (High Level)

1. Ingest (`project/pipelines/ingest/`)
2. Clean (`project/pipelines/clean/`)
3. Features + context (`project/pipelines/features/`)
4. Phase-1 analyzers (`project/pipelines/research/analyze_*.py`)
5. Phase-2 hypotheses (`project/pipelines/research/phase2_conditional_hypotheses.py`)
6. Edge/strategy prep (`compile_strategy_blueprints.py`, `build_strategy_candidates.py`)
7. Optional execution path:
- `backtest_strategies.py`
- `run_walkforward.py`
- `promote_blueprints.py`
- `make_report.py`

## 5) Artifact Map

### Run manifests/logs
- `data/runs/<run_id>/*.json`
- `data/runs/<run_id>/*.log`

### Research outputs
- `data/reports/<event_family>/<run_id>/...`
- `data/reports/phase2/<run_id>/<event_family>/...`

### Blueprint + strategy-builder outputs
- `data/reports/strategy_blueprints/<run_id>/blueprints.jsonl`
- `data/reports/strategy_builder/<run_id>/strategy_candidates.json`

### Backtest outputs (when enabled)
- `data/runs/<run_id>/engine/strategy_returns_*.csv`
- `data/lake/trades/backtests/vol_compression_expansion_v1/<run_id>/metrics.json`

### Eval/promotion/report outputs (when enabled)
- `data/reports/eval/<run_id>/walkforward_summary.json`
- `data/reports/promotions/<run_id>/promotion_report.json`
- `data/reports/vol_compression_expansion_v1/<run_id>/summary.json`

## 6) Testing

Run full tests:
```bash
./.venv/bin/pytest -q
```

## 7) Documentation Index

- `docs/README.md`
- `docs/discovery_pipeline.md`
- `docs/operator_runbook.md`
- `docs/strategy_builder.md`
- `docs/hypothesis_generator.md`
- `docs/data_strategy.md`
- `docs/repo_hygiene.md`

## 8) Notes on Costs and Runtime

- Running locally uses your machine compute.
- Running through Codex chat still uses model/chat usage.
- Running in cloud adds cloud compute/storage/network costs.
- API costs only apply if a paid external API is called.
