# Backtest: Spec-First Crypto Research and Backtesting

Backtest is a discovery-first quantitative research platform for crypto markets. It is designed for reproducibility, point-in-time correctness, and auditability across the full lifecycle from event discovery to evaluation.

## What This Repository Does

- Ingests and normalizes perp/spot market data.
- Builds cleaned 5m bars and feature/context datasets.
- Detects event families and builds canonical event registries.
- Runs Phase2 conditional discovery with multiplicity controls.
- Applies bridge/economic gates and promotion filters.
- Optionally runs backtest, walkforward, and report stages.

Primary orchestrator: `project/pipelines/run_all.py`

## Core Principles

- Spec-first contracts in `spec/` are the source of truth.
- No lookahead joins (PIT-safe joins and lagged entry controls).
- Fail-closed gating for checklist/execution and protected flows.
- Run-level provenance captured in `data/runs/<run_id>/run_manifest.json`.

## Run Integrity Model

Run outputs are tracked with both logical and instance-level traces.

- `planned_stages`: logical stage names.
- `planned_stage_instances`: expanded stage instances (for event-specific stages).
- `stage_timings_sec`: timing by logical stage.
- `stage_instance_timings_sec`: timing by stage instance.
- `pipeline_session_id`: session token propagated into stage manifests.
- `artifact_cutoff_utc`, `late_artifact_count`, `late_artifact_examples`: terminal audit metadata.

Stage manifests use stage-instance naming when available (for example `build_event_registry_<EVENT>.json`) to prevent event trace overwrites.

## Quick Start

### 1) Environment

```bash
python3 -m venv .venv
. .venv/bin/activate
.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
export BACKTEST_DATA_ROOT=$(pwd)/data
```

### 2) Baseline run

```bash
./.venv/bin/python project/pipelines/run_all.py \
  --run_id quick_start \
  --symbols BTCUSDT \
  --start 2024-01-01 \
  --end 2024-01-31
```

### 3) Discovery run (all event families)

```bash
make discover-edges
```

### 4) Validation

```bash
make check-hygiene
make test-fast
```

## Standard Commands

- Fast tests: `make test-fast`
- Full tests: `make test`
- Hygiene checks: `make check-hygiene`
- Compile check: `make compile`
- Discovery run: `make discover-edges`
- Baseline run: `make baseline STRATEGIES=<comma-separated>`

## Artifact Layout

- Run manifests/logs: `data/runs/<run_id>/`
- Event registry artifacts: `data/events/<run_id>/`
- Stage reports: `data/reports/<stage>/<run_id>/...`
- Phase2 candidates: `data/reports/phase2/<run_id>/<event_type>/`
- Bridge eval: `data/reports/bridge_eval/<run_id>/<event_type>/`

## Documentation

- [Getting Started](docs/GETTING_STARTED.md)
- [Core Concepts](docs/CONCEPTS.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Spec-First Development](docs/SPEC_FIRST.md)
- [Current Audit Baseline](docs/AUDIT.md)
- [Project Package Guide](project/README.md)
