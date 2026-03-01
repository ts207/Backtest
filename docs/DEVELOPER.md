# Developer Guide

How to extend and maintain the Backtest Alpha Discovery system.

---

## Code Conventions

- **Python 3.10+** with `from __future__ import annotations`
- All pipeline stages are standalone scripts callable as `python script.py --arg value`
- Each stage writes a JSON manifest on completion (via `pipelines/_lib/run_manifest.py`)
- No global mutable state — stages communicate only through the data lake
- All timestamps are UTC-aware `pd.Timestamp` or ISO-8601 strings

### Import Discipline

```python
# project/pipelines/research/ scripts must import via:
from pipelines._lib.io_utils import read_parquet, list_parquet_files
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from events.registry import EVENT_REGISTRY_SPECS
# never: import pandas as pd; df.to_parquet("data/...")  — use io_utils
```

---

## Adding a New Event Type

### Step 1 — Write the Spec

Create `spec/events/<MY_EVENT>.yaml`:
```yaml
event_type: MY_EVENT
canonical_family: LIQUIDITY_DISLOCATION
description: "Brief description"
detection:
  source_features: [spread_bps, vol_regime]
  lookback_bars: 12
  prevalence_per_10k: "1.0 - 50.0"
horizons: ["15m", "60m"]
```

### Step 2 — Write the Analyzer Script

Create `project/pipelines/research/analyze_my_event.py`.

The script must:
1. Accept `--run_id`, `--symbols`, `--event_type`, `--timeframe` args
2. Write a manifest via `start_manifest` / `finalize_manifest`
3. Read event registry outputs from `data/events/`
4. Compute forward returns using `calculate_expectancy_stats` from `phase2_candidate_discovery`
5. Write candidates to `data/reports/phase2/<run_id>/MY_EVENT/candidates.parquet`

### Step 3 — Register in `PHASE2_EVENT_CHAIN`

In `project/pipelines/run_all.py` (or the relevant event chain definition), ensure the new analyzer is included so `run_all.py` can orchestrate it.

---

## Adding a New Feature

### Step 1 — Write the Spec

Create `spec/features/<feature_name>.yaml`:
```yaml
name: my_feature
description: "..."
formula: "rolling std of log returns"
lookback_bars: 24
pit_safe: true
output_dtype: float64
```

### Step 2 — Implement in `project/pipelines/features/build_features_v1.py`

PIT contract: features must be computed using only past data. No "centered" windows or future-leaking signals.

---

## Spec-First Workflow

Specs in `spec/` are the source of truth. Any code change that alters event definitions, gate thresholds, or feature contracts must update the corresponding YAML first.

1. Edit `spec/` YAML
2. Code changes reference updated spec values via `spec_loader.py`
3. Re-run `make discover-blueprints` with a new `RUN_ID`
4. Old candidate plans become invalid (by hash mismatch) — do not reuse them

---

## Testing

```bash
# Fast CI profile
make test-fast

# Full suite
make test

# Single test file
.venv/bin/python -m pytest tests/test_phase2_gates.py -v
```

---

## Project Structure Quick-Reference

```
project/
├── engine/          "Bridge" validation core (PnL, execution cost, risk)
├── eval/            Research evaluation (robustness, splits)
├── events/          Event registry loaders
├── features/        Feature computation modules
├── pipelines/
│   ├── _lib/        Shared utilities (io, specs, stats, manifest, ontology)
│   ├── alpha_bundle/ Cross-section alpha combination & orthogonalization
│   ├── clean/       OHLCV cleaning, ToB, basis state
│   ├── features/    build_features_v1, context, market context
│   ├── research/    Event analyzers, Phase 2 discovery, Blueprint Compiler
│   └── run_all.py   Master orchestrator
└── scripts/
    ├── clean_data.sh            Data lake cleanup
    ├── check_repo_hygiene.sh    Repo standards enforcement
    ├── pipeline_governance.py   Spec auditing and schema sync
    └── monitor_data_freshness.py

spec/
├── events/        Event definitions + canonical registry
├── features/      Feature definitions
├── states/        Market state registry (vol_regime, etc.)
├── multiplicity/  Family taxonomy for FDR control
├── hypotheses/    Template verb lexicon
├── strategies/    Blueprint metadata
└── gates.yaml     Promotion gate thresholds
```
