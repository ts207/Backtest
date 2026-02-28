# Backtest

An event-first, spec-driven quantitative research platform for Binance perpetual and spot markets.

The system converts raw OHLCV + market microstructure data into statistically validated strategy blueprints through a sequential pipeline: ingest → clean → features → event detection → Phase 2 discovery → bridge evaluation → blueprint compilation.

---

## Quick start

```bash
# Setup
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
export BACKTEST_DATA_ROOT=$(pwd)/data

# Full discovery run (raw data assumed present)
make discover-edges-from-raw \
  RUN_ID=my_run \
  SYMBOLS=BTCUSDT,ETHUSDT \
  START=2020-06-01 \
  END=2025-07-10
```

See [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) for first-time setup and data bootstrap.

---

## Core make targets

```bash
make run                  # ingest + clean + features
make discover-edges       # full discovery (all 57 event types, parallel)
make discover-edges-from-raw  # discovery skipping ingest
make discover-hybrid      # discovery + expectancy analysis + robustness
make baseline STRATEGIES=...  # run + backtest + report
make test-fast            # fast test suite
make monitor              # data freshness check
make clean-runtime        # wipe run/report artifacts
make check-hygiene        # enforce repo hygiene constraints
```

---

## Key numbers

| Metric | Value |
|--------|-------|
| Event types | 57 |
| Canonical event families | 10 |
| Rule templates | 5–6 per event |
| Horizons | 15m (primary), 5m, 60m |
| Market states | vol_regime × carry_state |
| Phase 2 hypotheses per run | ~3,000–10,000 |
| FDR control method | Benjamini-Hochberg (grouped by family) |
| Default FDR threshold | q ≤ 0.05 |
| Shrinkage model | Hierarchical James-Stein (family → event → state) |
| Exec modes | `close`, `next_open` |

---

## Documentation

| Document | Contents |
|----------|---------|
| [GETTING_STARTED.md](docs/GETTING_STARTED.md) | Installation, first run, viewing results, troubleshooting |
| [NON_TECHNICAL_GUIDE.md](docs/NON_TECHNICAL_GUIDE.md) | A beginner-friendly introduction to what the project does and how it works |
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | Pipeline diagram, module map, data lake layout, Phase 2 statistical detail |
| [CONCEPTS.md](docs/CONCEPTS.md) | PIT safety, market states, shrinkage math, gates, blueprint schema |
| [DEVELOPER.md](docs/DEVELOPER.md) | Adding events/features/gates, spec workflow, testing guide |
| [OPERATIONS.md](docs/OPERATIONS.md) | Runbook, monitoring, kill switch, OOS validation, cleanup |
| [PERFORMANCE.md](docs/PERFORMANCE.md) | Bottleneck inventory, patches, tuning guide, profiling |
| [SPEC_FIRST.md](docs/SPEC_FIRST.md) | Spec-first development contracts and change workflow |
| [AUDIT.md](docs/AUDIT.md) | Audit findings, data integrity status, maturity scores |

---

## System contracts

1. **Spec is source of truth** — gate thresholds, event definitions, and feature formulas live in `spec/`. Code references spec, not the reverse.
2. **PIT-safe joins only** — all feature joins use `merge_asof(direction="backward")`; entry always lags event by ≥1 bar.
3. **Registry outputs for event alignment** — downstream stages read `events.parquet` / `event_flags.parquet`; no inline re-detection.
4. **Missing data is explicit** — gap-masked bars are NaN, never zero-filled.
5. **Ontology hash traceability** — candidate plans carry `ontology_spec_hash` tied to the generating `spec/` state.

---

## Working commands

```bash
export BACKTEST_DATA_ROOT=$(pwd)/data
source .venv/bin/activate

# Tests
make test-fast

# Single event type discovery (faster for development)
python project/pipelines/run_all.py \
  --run_id dev_run \
  --symbols BTCUSDT \
  --start 2022-01-01 --end 2024-12-31 \
  --phase2_event_type VOL_SHOCK \
  --skip_ingest_ohlcv 1 --skip_ingest_funding 1 \
  --run_phase2_conditional 1 \
  --mode research

# Monitoring
make monitor SYMBOLS=BTCUSDT,ETHUSDT

# Kill switch
python project/scripts/kill_switch.py --run_id my_run --symbol BTCUSDT
```
