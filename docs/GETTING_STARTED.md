# Getting Started

This guide details the standard local workflows utilized to test hypotheses or evaluate changes against the pipeline.

## Prerequisites

- Python 3.12 (Strict minimum)
- POSIX shell (`bash`)
- `make` utility

## Primary Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt

# Designate external or local storage paths for massive artifact arrays
export BACKTEST_DATA_ROOT=$(pwd)/data
```

## Exploratory Research Runs

```bash
# Core pipeline without combinatorial hypothesis engine
python project/pipelines/run_all.py \
  --run_id base_run \
  --symbols BTCUSDT \
  --start 2024-01-01 \
  --end 2024-01-31

# Targeted Multi-Phase Discovery (Expectancy Evaluation enabled via make)
make discover-hybrid \
  RUN_ID=quick_discovery \
  SYMBOLS=BTCUSDT \
  START=2024-01-01 \
  END=2024-06-30
```

## Large Scale Discovery & Execution

Run full Phase 2 discovery spanning across all declared architectures and eventually compile + backtest validated blueprint strategies.

```bash
# Broad hypothesis mining and structural checks
make discover-edges RUN_ID=universal_discovery SYMBOLS=BTCUSDT,ETHUSDT

# Unified pipeline execution testing promoted strategies
make discover-hybrid-backtest \
  RUN_ID=backtest_demo \
  SYMBOLS=BTCUSDT \
  STRATEGIES=candidate_1
```

## Validating Integrations before Commit

Ensure changes haven't structurally invalidated the schema boundaries or core unit testing behavior.

```bash
# Pre-flight repo integrity checks and ontology validation
make check-hygiene

# Quick deterministic validation suite 
make test-fast
```

## Navigating Pipeline Artifacts

Since operations span hundreds of multi-process outputs, use `data/` to track states:
- Pipeline structural footprints: `data/runs/<run_id>/run_manifest.json`
- Low-level executor logs: `data/runs/<run_id>/*.log`
- Registered Phase 1 events: `data/events/<run_id>/...`
- Strategy compilation details: `data/reports/strategy_builder/<run_id>/...`
- Evaluated performance matrices: `data/reports/bridge_eval/` and walk-forward evaluations locally under respective namespaces.

## Fail-Closed Blocks

The pipeline defends statistical integrity aggressively:
- **`KEEP_RESEARCH` blocks:** By default, stages testing naive execution will fail back to research states unless explicit checklist promotion conditions are satisfied.
- **Funding or Price Integrity Gaps:** Data layers containing unresolved inconsistencies will immediately panic to block cascading issues from propagating toward the execution phases.
