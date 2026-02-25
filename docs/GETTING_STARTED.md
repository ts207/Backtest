# Getting Started

This guide covers the supported local workflow for running and validating the pipeline.

## Prerequisites

- Python 3.12 recommended
- POSIX shell (`bash`)
- Optional: `make`

## Setup

```bash
python3 -m venv .venv
. .venv/bin/activate
.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
export BACKTEST_DATA_ROOT=$(pwd)/data
```

## First Run (Core Pipeline)

```bash
./.venv/bin/python project/pipelines/run_all.py \
  --run_id first_run \
  --symbols BTCUSDT \
  --start 2024-01-01 \
  --end 2024-01-31
```

## Run Discovery (Single Event)

```bash
./.venv/bin/python project/pipelines/run_all.py \
  --run_id first_discovery \
  --symbols BTCUSDT \
  --start 2024-01-01 \
  --end 2024-01-31 \
  --run_hypothesis_generator 0 \
  --run_phase2_conditional 1 \
  --phase2_event_type LIQUIDITY_VACUUM \
  --run_bridge_eval_phase2 1
```

## Run Discovery (All Events)

```bash
make discover-edges
```

## Validate Before Claims

```bash
make check-hygiene
make test-fast
```

## Where to Inspect Outputs

- Run manifest: `data/runs/<run_id>/run_manifest.json`
- Stage manifests/logs: `data/runs/<run_id>/*.json`, `data/runs/<run_id>/*.log`
- Event registry: `data/events/<run_id>/`
- Phase2: `data/reports/phase2/<run_id>/<event_type>/`
- Bridge: `data/reports/bridge_eval/<run_id>/<event_type>/`

## Common Fail-Closed Stops

- Checklist gate: decision `KEEP_RESEARCH` with execution requested.
- Missing/empty promotion artifacts when compile/builder require promoted candidates.
- Funding/context integrity gates in context and market-state stages.
- Protected evaluation guard violations (fallback blueprint path in measurement flow).
