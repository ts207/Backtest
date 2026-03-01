# Getting Started

Step-by-step guide to set up the Backtest Alpha Discovery platform and run your first research discovery.

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

## First run: Ingest and Features

If you need to ingest from Binance API and prepare features:

```bash
make run \
  RUN_ID=first_run \
  SYMBOLS=BTCUSDT,ETHUSDT \
  START=2022-01-01 \
  END=2024-12-31
```

This runs: ingest → clean → features. Takes 5–20 minutes depending on data range.

---

## Running Discovery

To run the full research pipeline and produce **Strategy Blueprints**:

```bash
# Phase 1 + Phase 2 discovery + Blueprint Compilation
make discover-blueprints \
  RUN_ID=discovery_$(date +%Y%m%d) \
  SYMBOLS=BTCUSDT,ETHUSDT \
  START=2020-06-01 \
  END=2025-07-10
```

This runs all 57 event analyzers in parallel, then Phase 2 discovery, bridge evaluation, and produces `blueprints.jsonl` in `data/reports/strategy_blueprints/`.

On a 4-core machine with 2 symbols over 5 years: **~30–40 minutes**.

---

## Viewing Results

### 1. Run Manifest
Check the timing, status, and metadata of your discovery run:
```bash
cat data/runs/discovery_$(date +%Y%m%d)/run_manifest.json | python3 -m json.tool
```

### 2. Top Promoted Candidates
Inspect the statistically validated candidates before they are compiled into blueprints:
```bash
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
```

### 3. Strategy Blueprints
View the final executable "Alpha DNA" artifacts:
```bash
python3 -c "
import json
with open('data/reports/strategy_blueprints/discovery_$(date +%Y%m%d)/blueprints.jsonl') as f:
    for line in f:
        b = json.loads(line)
        print(b['event_type'], b['horizon'], b['template_verb'], b.get('promotion_track',''))
"
```

---

## Next Steps

Now that you have your Blueprints, the next stage is to import them into **NautilusTrader** for high-fidelity backtesting and live execution.

- [ARCHITECTURE.md](ARCHITECTURE.md) — research pipeline diagram and module map
- [CONCEPTS.md](CONCEPTS.md) — PIT safety, shrinkage, gates, event taxonomy
- [DEVELOPER.md](DEVELOPER.md) — adding events, features, and gates
- [PERFORMANCE.md](PERFORMANCE.md) — bottlenecks, tuning, caching
- [SPEC_FIRST.md](SPEC_FIRST.md) — spec-driven development workflow
