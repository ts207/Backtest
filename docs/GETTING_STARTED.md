# Getting Started

This guide covers the current, supported way to run the pipeline.

## Prerequisites

- Python 3.10+
- `pip`
- Optional: `make`

## Setup

```bash
git clone <repo-url>
cd Backtest
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
export BACKTEST_DATA_ROOT=$(pwd)/data
```

## First Run: Build Core Data

```bash
./.venv/bin/python project/pipelines/run_all.py \
  --run_id first_run \
  --symbols BTCUSDT \
  --start 2026-01-01 \
  --end 2026-01-31
```

This executes ingest -> cleaned bars -> features -> context -> market context by default.

## Run Discovery for One Event Family

```bash
./.venv/bin/python project/pipelines/run_all.py \
  --run_id first_discovery \
  --symbols BTCUSDT \
  --start 2026-01-01 \
  --end 2026-01-31 \
  --run_hypothesis_generator 0 \
  --run_phase2_conditional 1 \
  --phase2_event_type LIQUIDITY_VACUUM \
  --run_bridge_eval_phase2 1
```

## Run Discovery for All Event Families

```bash
make discover-edges RUN_ID=discover_all SYMBOLS=BTCUSDT,ETHUSDT START=2026-01-01 END=2026-01-31
```

## Optional: Backtest and Walkforward

```bash
./.venv/bin/python project/pipelines/run_all.py \
  --run_id full_eval \
  --symbols BTCUSDT,ETHUSDT \
  --start 2026-01-01 \
  --end 2026-01-31 \
  --run_phase2_conditional 1 \
  --phase2_event_type all \
  --run_backtest 1 \
  --run_walkforward_eval 1 \
  --run_make_report 1
```

## Key Outputs

- Registry events and flags:
  - `data/events/<run_id>/events.parquet`
  - `data/events/<run_id>/event_flags.parquet`
- Phase2 candidates:
  - `data/reports/phase2/<run_id>/<event_type>/phase2_candidates.csv`
- Blueprints:
  - `data/reports/strategy_blueprints/<run_id>/blueprints.jsonl`

## Sanity Checks

```bash
make test-fast
make check-hygiene
```

## Common Fail-Closed Stops

- Funding coverage gaps in context/market-state stages.
- Missing Phase1 events file for selected event family.
- Non-executable blueprint condition/action in compile path.
- Attempt to enable fallback blueprint compilation in protected flows.
