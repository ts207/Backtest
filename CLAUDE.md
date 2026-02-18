# Backtest

Crypto event-driven strategy discovery and backtesting system (Binance perp/spot, 15m candles).

## Environment

```bash
export BACKTEST_DATA_ROOT=/path/to/data   # Required - all artifacts live here
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
```

## Commands

```bash
# Tests
pytest -q                          # All tests
pytest -q -m "not slow"            # Fast profile (CI-equivalent)
pytest tests/test_sanity.py -v     # Single file

# Pipeline (via Makefile)
make run                           # ingest + clean + features + context
make discover-edges                # Full discovery (phase1 + phase2 + export)
make discover-edges-from-raw       # Discovery using existing raw data (skip ingest)
make test                          # Full test suite
make test-fast                     # Fast tests only
make check-hygiene                 # Enforce repo hygiene constraints

# Direct entry point
python project/pipelines/run_all.py \
  --run_id discovery_2020_2025 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 --end 2025-07-10
```

## Architecture

```
project/
├── pipelines/run_all.py      # MAIN ENTRY POINT - master orchestrator (1179 lines)
│   ├── ingest/               # Binance API → raw Parquet
│   ├── clean/                # Normalization
│   ├── features/             # Feature + context construction
│   ├── research/             # Phase-1 hypothesis gen, Phase-2 conditional analysis
│   ├── backtest/             # Strategy execution
│   ├── eval/                 # Walkforward splits
│   └── report/               # Promotion and reporting
├── engine/runner.py          # Execution engine (P&L, risk, fills)
├── strategies/               # Strategy implementations + DSL interpreter
├── strategy_dsl/             # Blueprint schema and contract validation
├── features/                 # Reusable feature logic
└── configs/                  # pipeline.yaml, fees.yaml, venue configs

tests/                        # 75+ test files, 269 functions
data/                         # Runtime artifacts (gitignored)
docs/                         # Operational docs and runbooks
```

## Key Gotchas

**Subprocess orchestration**: `run_all.py` spawns each pipeline stage as a subprocess, not function calls. Stages are independent scripts — fault-isolated, independently restartable.

**Discovery-only by default**: These flags default to `0` — you must explicitly enable them:
```
--run_backtest 1
--run_walkforward_eval 1
--run_make_report 1
```

**Phase-2 event chain**: `PHASE2_EVENT_CHAIN` (lines 18-28 of `run_all.py`) defines a hard-coded sequence. Phase-1 must complete before Phase-2. Output of each event type feeds the next.

**Frozen canonical run**: `RUN_2022_2023` (2021-01-01 to 2022-12-31) is the canonical evaluation window for bridge policy and Codex operations (see `AGENTS.md`).

**Overlay-only candidates**: Overlay strategies must NOT appear as standalone executables in backtest — delta-vs-base only.

**Raw data skip**: `--skip_ingest_*` assumes data already exists in `data/lake/raw/`. Will not download if skipped.

**Registry parity**: Phase-2 validates event counts against `data/events/{run_id}/events.parquet`. Mismatch is a fatal error.

## Testing Patterns

Tests use `conftest.py` fixture `backtest_data_root` which creates an isolated `tmp_path` and sets `BACKTEST_DATA_ROOT`. All test files follow `tests/test_*.py` naming. Import path patching (`sys.path`) is common — tests import directly from `project/`.

## Data Model

File-based only (no SQL). All market data and features in Parquet. Blueprints in JSONL. Stage outputs tracked in `$BACKTEST_DATA_ROOT/runs/{run_id}/{stage}.json` manifests + `.log` files.

## CI

GitHub Actions runs `pytest -q` + `check_repo_hygiene.sh` on every push/PR. Codex review triggered by `@codex` mention in PR comments.
