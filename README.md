# Backtest: Event-Driven Crypto Research Pipeline

Backtest is a discovery-first research platform for Binance perp/spot data. It is built to find edges, reject weak candidates, and only compile executable strategy blueprints when statistical and economic gates pass.

## What Is Implemented Today

Core orchestrator: `project/pipelines/run_all.py`

Stage flow (high level):
1. Ingest raw market data (OHLCV, optional funding/OI/liquidations, optional spot).
2. Build cleaned 5m bars.
3. Build `features_v1`.
4. Build context datasets (`funding_persistence`, `market_state`) and universe snapshots.
5. Run Phase 1 event analyzers.
6. Build canonical event registry (`events.parquet`, `event_flags.parquet`).
7. Run Phase 2 candidate discovery with multiplicity control.
8. Run bridge tradability checks.
9. Compile strategy blueprints and optional strategy builder outputs.
10. Optional backtest, walkforward, promotion, and reporting.

## Integrity Contracts

- PIT joins use backward `merge_asof` with explicit staleness tolerances.
- Funding alignment is fail-closed in context/market-state stages when coverage gaps exist.
- Event registry emits both impulse and active flags:
  - `*_event` for anchor timestamp
  - `*_active` for `[enter_ts, exit_ts]`
- Phase 2 discovery uses registry events first (not raw analyzer CSV by default).
- Phase 2 enforces lagged entry (`entry_lag_bars >= 1`) to prevent same-bar fill leakage.
- Bridge and walkforward embargo defaults are `1` day.

## Quick Start

### 1) Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
export BACKTEST_DATA_ROOT=$(pwd)/data
```

### 2) Build data/features/context only

```bash
make run RUN_ID=quick_btc SYMBOLS=BTCUSDT START=2026-01-01 END=2026-01-31
```

### 3) Run one-family discovery (example: liquidity vacuum)

```bash
./.venv/bin/python project/pipelines/run_all.py \
  --run_id liqvac_1m \
  --symbols BTCUSDT \
  --start 2026-01-01 \
  --end 2026-01-31 \
  --run_hypothesis_generator 0 \
  --run_phase2_conditional 1 \
  --phase2_event_type LIQUIDITY_VACUUM \
  --run_bridge_eval_phase2 1
```

### 4) Run tests

```bash
make test-fast
make check-hygiene
```

## Where Artifacts Go

- Run manifests/logs: `data/runs/<run_id>/`
- Lake data: `data/lake/...`
- Registry outputs: `data/events/<run_id>/`
- Phase2 outputs: `data/reports/phase2/<run_id>/<event_type>/`
- Bridge outputs: `data/reports/bridge_eval/<run_id>/<event_type>/`
- Blueprints: `data/reports/strategy_blueprints/<run_id>/blueprints.jsonl`

## Documentation

- [Getting Started](docs/GETTING_STARTED.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Core Concepts](docs/CONCEPTS.md)
- [Spec-First Development](docs/SPEC_FIRST.md)
- [Current Audit Baseline](docs/AUDIT.md)
- [Project Package Guide](project/README.md)
