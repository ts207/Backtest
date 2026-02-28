# Getting Started

Step-by-step guide to set up the Backtest platform and run your first discovery.

---

## Prerequisites

- Python 3.10+
- Linux or macOS (WSL2 supported)
- ~10 GB disk space for a 5-year 2-symbol dataset
- Binance API access (for live ingest; not required if you have existing raw parquets)

---

## Installation

```bash
git clone <repo>
cd backtest

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt -r requirements-dev.txt

# Set data root (add to your shell profile)
export BACKTEST_DATA_ROOT=$(pwd)/data

# Verify setup
make test-fast
```

---

## First run: clean and features only

If you already have raw OHLCV data in `data/lake/raw/`:

```bash
make discover-edges-from-raw \
  RUN_ID=first_run \
  SYMBOLS=BTCUSDT,ETHUSDT \
  START=2022-01-01 \
  END=2024-12-31
```

If you need to ingest from Binance API:

```bash
make run \
  RUN_ID=first_run \
  SYMBOLS=BTCUSDT,ETHUSDT \
  START=2022-01-01 \
  END=2024-12-31
```

This runs: ingest → clean → features. Takes 5–20 minutes depending on data range.

---

## Running discovery

```bash
# Phase 1 + Phase 2 discovery (no ingest, uses existing raw data)
make discover-edges-from-raw \
  RUN_ID=discovery_$(date +%Y%m%d) \
  SYMBOLS=BTCUSDT,ETHUSDT \
  START=2020-06-01 \
  END=2025-07-10
```

This runs all 57 event analyzers in parallel (default: up to 8 workers), then Phase 2 discovery, bridge evaluation, and produces `blueprints.jsonl`.

On a 4-core machine with 2 symbols over 5 years: **~30–40 minutes**.

---

## Viewing results

```bash
# Run manifest (timing, status, git hash)
cat data/runs/discovery_$(date +%Y%m%d)/run_manifest.json | python3 -m json.tool

# Top promoted candidates
python3 -c "
import pandas as pd, glob, os
run_id = 'discovery_$(date +%Y%m%d)'
root = f'data/reports/phase2/{run_id}'
files = glob.glob(f'{root}/*/candidates.parquet')
if not files:
    print('No candidates found')
else:
    df = pd.concat([pd.read_parquet(f) for f in files])
    passed = df[df.get('gate_phase2_final', False)]
    print(passed.sort_values('after_cost_expectancy', ascending=False)[
        ['event_type','horizon','template_verb','state_id',
         'after_cost_expectancy','p_value','n_events']
    ].head(20).to_string())
"

# Strategy blueprints
python3 -c "
import json
with open('data/reports/strategy_blueprints/discovery_$(date +%Y%m%d)/blueprints.jsonl') as f:
    for line in f:
        b = json.loads(line)
        print(b['event_type'], b['horizon'], b['template_verb'], b.get('promotion_track',''))
"
```

---

## Running a backtest

Once you have blueprints, run a strategy backtest:

```bash
make discover-hybrid-backtest \
  RUN_ID=discovery_$(date +%Y%m%d) \
  SYMBOLS=BTCUSDT,ETHUSDT \
  STRATEGIES=vol_shock_mean_reversion_v1
```

Or run the full hybrid discovery + backtest in one command:

```bash
make discover-hybrid-backtest \
  RUN_ID=full_$(date +%Y%m%d) \
  SYMBOLS=BTCUSDT,ETHUSDT \
  START=2020-06-01 \
  END=2025-07-10 \
  STRATEGIES=vol_shock_mean_reversion_v1
```

---

## Configuring the run

All parameters can be passed as CLI flags to `run_all.py`:

```bash
python project/pipelines/run_all.py \
  --run_id my_run \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 --end 2025-07-10 \
  --phase2_event_type VOL_SHOCK \      # run only one event type
  --max_analyzer_workers 4 \           # parallel workers
  --mode research \                    # discovery gate profile
  --entry_lag_bars 1 \                 # PIT-safe entry lag
  --enable_time_decay 1 \              # exponential weighting
  --run_phase2_conditional 1
```

See `python project/pipelines/run_all.py --help` for the full parameter list.

---

## Performance tips

- Use `make discover-edges-from-raw` instead of `make discover-edges` to skip re-ingesting data you already have
- Set `--max_analyzer_workers` to your CPU core count for maximum parallelism
- Enable `BACKTEST_STAGE_CACHE=1` to skip unchanged stages on re-runs
- Reduce `START`/`END` range or `SYMBOLS` for faster iteration during development

See [PERFORMANCE.md](PERFORMANCE.md) for the full performance guide.

---

## Troubleshooting

### "No data found for symbol"

Check that the raw data exists:
```bash
ls data/lake/raw/perp/BTCUSDT/5m/ohlcv/
```

If empty, run `make run` first to ingest.

### Stage failed

```bash
# Find which stage failed
cat data/runs/<run_id>/run_manifest.json | python3 -m json.tool | grep failed_stage

# Read the log
cat data/runs/<run_id>/<failed_stage>.log
```

### Out of memory

Reduce `--max_analyzer_workers`. See [PERFORMANCE.md](PERFORMANCE.md#memory-pressure).

### `ontology_spec_hash` mismatch

Your `spec/` files changed since the candidate plan was created. Either regenerate the plan or pass `--allow_ontology_hash_mismatch 1` (use with caution — results may not be reproducible against the current spec).

---

## Next steps

- [ARCHITECTURE.md](ARCHITECTURE.md) — full pipeline diagram and module map
- [CONCEPTS.md](CONCEPTS.md) — PIT safety, shrinkage, gates, event taxonomy
- [DEVELOPER.md](DEVELOPER.md) — adding events, features, and gates
- [OPERATIONS.md](OPERATIONS.md) — monitoring, kill switch, OOS validation, cleanup
- [PERFORMANCE.md](PERFORMANCE.md) — bottlenecks, tuning, caching
- [AUDIT.md](AUDIT.md) — current audit findings and maturity assessment
- [SPEC_FIRST.md](SPEC_FIRST.md) — spec-driven development workflow
