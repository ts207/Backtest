# Operations Runbook

Operational reference for running, monitoring, and maintaining the Backtest system.

---

## Environment setup

```bash
# Required once
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt

# Required every session
export BACKTEST_DATA_ROOT=$(pwd)/data
```

---

## Make targets

| Target | What it does |
|--------|-------------|
| `make run` | Ingest + clean + features only (no discovery) |
| `make baseline STRATEGIES=...` | Full pipeline including backtest and report |
| `make discover-edges` | Phase 1 + Phase 2 discovery, no ingest |
| `make discover-edges-from-raw` | Discovery chain using existing raw lake (skips ingest) |
| `make discover-hybrid` | Discovery + expectancy analysis + robustness |
| `make discover-hybrid-backtest STRATEGIES=...` | Full hybrid discovery + backtest |
| `make test` | Full test suite |
| `make test-fast` | Fast test profile (excludes `@pytest.mark.slow`) |
| `make compile` | Byte-compile all Python under `project/` |
| `make monitor` | Data freshness check for `SYMBOLS` |
| `make clean-runtime` | Wipe `data/runs/`, `data/reports/`, `data/events/`, `data/lake/runs/` |
| `make clean-all-data` | Wipe all data including raw + cleaned + features |
| `make clean-repo` | Remove `.pyc`, caches, `Zone.Identifier` files |
| `make check-hygiene` | Enforce repo hygiene constraints |

### Common variables

```bash
RUN_ID=discovery_2020_2025   # Tag for this run's artifacts
SYMBOLS=BTCUSDT,ETHUSDT      # Comma-separated symbols
START=2020-06-01
END=2025-07-10
STRATEGIES=vol_shock_mr_v1   # Required for backtest targets
```

---

## Typical research workflow

### 1. First-time data bootstrap

```bash
export BACKTEST_DATA_ROOT=$(pwd)/data
make run START=2020-06-01 END=2025-07-10 SYMBOLS=BTCUSDT,ETHUSDT
```

This runs ingest → clean → features. Raw parquets land in `data/lake/raw/`, cleaned in `data/lake/cleaned/`.

### 2. Discovery run (core loop)

```bash
# Skip ingest if raw data already exists
make discover-edges-from-raw RUN_ID=run_$(date +%Y%m%d)
```

Artifacts written to:
- `data/runs/<run_id>/` — stage manifests + logs
- `data/reports/phase2/<run_id>/<EVENT>/` — per-event candidates
- `data/reports/strategy_blueprints/<run_id>/blueprints.jsonl`

### 3. Inspect results

```bash
# Run manifest (timing, status, hashes)
cat data/runs/<run_id>/run_manifest.json | python -m json.tool

# Top candidates by after-cost expectancy
python -c "
import pandas as pd, glob
files = glob.glob('data/reports/phase2/<run_id>/*/candidates.parquet')
df = pd.concat([pd.read_parquet(f) for f in files])
print(df[df.gate_phase2_final].sort_values('after_cost_expectancy', ascending=False)[
  ['event_type','horizon','template_verb','state_id','after_cost_expectancy','p_value','n_events']
].head(20).to_string())
"

# Strategy blueprints
python -c "
import json
with open('data/reports/strategy_blueprints/<run_id>/blueprints.jsonl') as f:
    for line in f: print(json.loads(line)['event_type'], json.loads(line)['horizon'])
"
```

### 4. Backtest a strategy

```bash
make discover-hybrid-backtest \
  RUN_ID=run_20250228 \
  STRATEGIES=vol_shock_mean_reversion_v1,liquidity_dislocation_mr_v1
```

---

## Monitoring

### Data freshness check

```bash
make monitor
# or
python project/scripts/monitor_data_freshness.py \
  --symbols BTCUSDT,ETHUSDT \
  --timeframe 5m \
  --max_staleness_bars 3
```

Exit codes: `0` = OK, `1` = data stale.

### Feature drift (PSI)

```bash
python project/scripts/monitor_feature_drift.py \
  --run_id <run_id> \
  --symbol BTCUSDT \
  --reference_window_days 30 \
  --live_window_days 7
```

Exit codes: `0` = OK, `1` = significant drift detected.

### Kill switch

```bash
python project/scripts/kill_switch.py \
  --run_id <run_id> \
  --symbol BTCUSDT \
  --max_staleness_bars 3 \
  --max_drawdown -0.15 \
  --max_slippage_ratio 2.5
```

Exit codes: `0` = OK (PASS), `1` = warning (WARN), `2` = halt (HALT).

Writes a status artifact to `data/runs/<run_id>/kill_switch_status.json`.

---

## OOS window validation

Before running evaluation on any window, verify there is no overlap with the discovery window:

```bash
python project/scripts/validate_oos_window.py \
  --run_id <run_id> \
  --eval_start 2025-01-01 \
  --eval_end 2025-07-01
```

Fails (`exit 1`) if the requested eval window overlaps with the recorded discovery window in `run_manifest.json`.

---

## Stage debugging

Each stage writes two artifacts:
- `data/runs/<run_id>/<stage_instance>.json` — stage manifest (status, timing, input hash)
- `data/runs/<run_id>/<stage_instance>.log` — full stdout/stderr

```bash
# Check which stage failed
cat data/runs/<run_id>/run_manifest.json | python -m json.tool | grep failed_stage

# Read the failing stage's log
cat data/runs/<run_id>/<stage>.log
```

### Re-run a single event type

```bash
export BACKTEST_DATA_ROOT=$(pwd)/data
source .venv/bin/activate
python project/pipelines/research/phase2_candidate_discovery.py \
  --run_id <run_id> \
  --event_type VOL_SHOCK \
  --symbols BTCUSDT,ETHUSDT \
  --mode research
```

### Enable stage caching (skip unchanged stages)

```bash
BACKTEST_STAGE_CACHE=1 make discover-edges-from-raw RUN_ID=my_run
```

---

## Cleanup

```bash
# Remove all run artifacts (keeps raw + cleaned + features)
make clean-runtime

# Remove everything including raw data
make clean-all-data

# Remove repo clutter (pyc, caches, Zone.Identifier)
make clean-repo

# Verify repo hygiene
make check-hygiene
```

---

## Repo hygiene policy

`make check-hygiene` enforces:
1. No runtime artifacts tracked in git (`data/runs/`, `data/reports/`, `data/lake/cleaned/`)
2. No `Zone.Identifier` Windows sidecar files
3. No tracked files exceeding 5 MB

If hygiene fails, run `make clean-repo` to fix clutter + `make clean-hygiene` for Zone.Identifier files.
