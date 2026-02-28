# Developer Guide

How to extend and maintain the Backtest system.

---

## Code conventions

- **Python 3.10+** with `from __future__ import annotations`
- All pipeline stages are standalone scripts callable as `python script.py --arg value`
- Each stage writes a JSON manifest on completion (via `pipelines/_lib/run_manifest.py`)
- No global mutable state — stages communicate only through the data lake
- Missing data is always explicit NaN, never zero-filled
- All timestamps are UTC-aware `pd.Timestamp` or ISO-8601 strings

### Import discipline

```python
# project/pipelines/research/ scripts must import via:
from pipelines._lib.io_utils import read_parquet, list_parquet_files
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from events.registry import EVENT_REGISTRY_SPECS
# never: import pandas as pd; df.to_parquet("data/...")  — use io_utils
```

---

## Adding a new event type

### Step 1 — Write the spec

Create `spec/events/<MY_EVENT>.yaml`:
```yaml
event_type: MY_EVENT
canonical_family: LIQUIDITY_DISLOCATION   # or your family
description: "One-line description"
detection:
  source_features: [spread_bps, vol_regime]
  lookback_bars: 12
  prevalence_per_10k: "1.0 - 50.0"
families:
  - LIQUIDITY_DISLOCATION
templates:
  - mean_reversion
  - continuation
horizons:
  - 15m
```

Register it in `spec/events/canonical_event_registry.yaml`:
```yaml
events:
  MY_EVENT:
    canonical_family: LIQUIDITY_DISLOCATION
    ...
```

### Step 2 — Add to multiplicity spec

In `spec/multiplicity/families.yaml`, add an entry under the appropriate family (or create a new one with `canonical_family: <YOUR_FAMILY>`).

### Step 3 — Write the analyzer script

Create `project/pipelines/research/analyze_my_event.py`.

The script must:
1. Accept `--run_id`, `--symbols`, `--event_type`, `--timeframe` args
2. Write a manifest via `start_manifest` / `finalize_manifest`
3. Read event registry outputs from `data/events/`
4. Compute forward returns using `calculate_expectancy_stats` from `phase2_candidate_discovery`
5. Write candidates to `data/reports/phase2/<run_id>/MY_EVENT/candidates.parquet`

Use an existing analyzer as template: `analyze_volatility_transition_events.py` is the simplest.

### Step 4 — Register in `PHASE2_EVENT_CHAIN`

In `project/pipelines/run_all.py`:
```python
PHASE2_EVENT_CHAIN: List[Tuple[str, str, List[str]]] = [
    ...
    ('MY_EVENT', 'analyze_my_event.py', ['--event_type', 'MY_EVENT', '--timeframe', '5m']),
]
```

### Step 5 — Add tests

At minimum:
```python
# tests/test_my_event_spec.py
from events.registry import EVENT_REGISTRY_SPECS
def test_my_event_registered():
    assert "MY_EVENT" in EVENT_REGISTRY_SPECS
```

### Step 6 — Validate

```bash
make test-fast
python project/pipelines/run_all.py --help  # check chain validation passes
```

---

## Adding a new feature

### Step 1 — Write the spec

Create `spec/features/<feature_name>.yaml`:
```yaml
name: my_feature
description: "..."
formula: "rolling std of log returns over lookback_bars"
lookback_bars: 24
pit_safe: true          # must be true — no future data
output_dtype: float64
null_policy: explicit   # never fill with 0
```

### Step 2 — Implement in `project/pipelines/features/build_features_v1.py`

Or create a new feature module under `project/pipelines/features/` and import it there.

PIT contract: features must be computed using only past data:
```python
# CORRECT — shifted so bar N uses data through bar N
df["my_feature"] = df["close"].pct_change().rolling(24).std()
# WRONG — leaks bar N+1 into bar N
df["my_feature"] = df["close"].pct_change().rolling(24, center=True).std()
```

### Step 3 — Add to features schema

If introducing a new schema version, bump `BACKTEST_FEATURE_SCHEMA_VERSION` in `run_all.py` and update `spec/` accordingly.

---

## Spec change workflow

Specs in `spec/` are the source of truth. Any code change that alters event definitions, gate thresholds, or feature contracts must update the corresponding YAML first.

1. Edit `spec/` YAML
2. Code changes reference updated spec values via `spec_loader.py`
3. Re-run `make discover-edges` with a new `RUN_ID` — the `ontology_spec_hash` in the manifest will change
4. Old candidate plans become invalid (by hash mismatch) — do not reuse them

**Never hardcode gate thresholds in code.** Always load from `spec/gates.yaml` via `_load_gates_spec()`.

---

## Adding a new gate

1. Add the threshold to `spec/gates.yaml` under `gate_v1_phase2` (or a new gate key)
2. Add per-event overrides under `gate_v1_phase2_profiles.discovery.event_overrides` if needed
3. Implement the gate logic in `_refresh_phase2_metrics_after_shrinkage` in `phase2_candidate_discovery.py`
4. Add a vectorized `gate_<name>` boolean column to the DataFrame (no `.apply`)
5. Include it in `fail_reasons` and `phase2_quality_score`
6. Add a test

---

## Engine: adding an execution mode

`engine/pnl.py` supports `exec_mode` in `compute_pnl_components`:
- `"close"` — entry at bar close (default)
- `"next_open"` — entry at next bar's open (realistic for end-of-bar signals)

To add a new mode (e.g. `"vwap"`):
1. Add a helper `compute_returns_vwap(features_df, horizon_bars) -> pd.Series`
2. Add a branch in `compute_pnl_components`
3. Wire the new mode name into `engine/runner.py` via `strategy_params.get("exec_mode", "close")`
4. Add the mode to the strategy spec YAML

---

## Testing

```bash
# Fast CI profile
make test-fast

# Full suite (includes slow integration tests)
make test

# Single test file
.venv/bin/python -m pytest tests/test_phase2_gates.py -v

# With coverage
.venv/bin/python -m pytest --cov=project --cov-report=term-missing -q
```

### Test conventions

- Unit tests in `tests/` are plain `pytest` — no test class required
- Slow tests are marked `@pytest.mark.slow`
- Fixtures that touch the filesystem use `tmp_path`
- Use `tests/conftest.py` for shared fixtures

---

## Project structure quick-reference

```
project/
├── engine/          simulation core (runner, risk, pnl, execution, monitoring)
├── eval/            evaluation helpers (splits, robustness, ablation)
├── events/          event registry loader
├── features/        feature computation modules (imported by pipeline scripts)
├── pipelines/
│   ├── _lib/        shared utilities (io, specs, stats, manifest, ontology)
│   ├── alpha_bundle/ cross-section alpha overlay
│   ├── backtest/    strategy simulation entry point
│   ├── clean/       OHLCV cleaning, ToB, basis state
│   ├── eval/        walkforward, robustness, microstructure
│   ├── features/    build_features_v1, context, market context
│   ├── research/    event analyzers, phase2 discovery, bridge eval
│   └── run_all.py   master orchestrator
└── scripts/
    ├── clean_data.sh            fast rm-rf cleanup script
    ├── check_repo_hygiene.sh    git-tracked hygiene checks
    ├── validate_oos_window.py   OOS/discovery overlap guard
    ├── monitor_data_freshness.py
    ├── monitor_feature_drift.py
    └── kill_switch.py

spec/
├── events/        one YAML per event + canonical_event_registry.yaml
├── features/      feature definitions
├── states/        market state registry
├── multiplicity/  BH-FDR family taxonomy + per-event templates/horizons
├── hypotheses/    template verb lexicon, lift-state templates
├── strategies/    strategy specs for backtest
├── gates.yaml     promotion gate thresholds
└── global_defaults.yaml
```
