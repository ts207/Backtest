# Performance Guide

This document covers pipeline performance characteristics, known bottlenecks, and tuning parameters for the Alpha Discovery Engine.

---

## Wall-time Budget (Reference)

On a 4-core dev machine with BTCUSDT + ETHUSDT, 2020–2025:

| Stage group | Approx. time (serial) | After parallelism patch |
|-------------|----------------------:|------------------------:|
| Ingest (skip if raw exists) | 0 – 20 min | — |
| Clean + features | ~5 min | ~5 min |
| Event registry × 57 events | ~28 min | **~7 min** (8 workers) |
| Phase 2 discovery × 57 events | ~15 min | ~15 min (sequential) |
| Bridge eval + promotion | ~3 min | ~3 min |
| Blueprint Compiler | ~1 min | ~1 min |
| **Total (discovery only)** | **~52 min** | **~31 min** |

---

## Bottleneck Inventory and Patches

### [PATCHED] #1 — Parallel Event Analyzers

**File:** `project/pipelines/run_all.py`

All 57 event analyzer stages (`build_event_registry_*`, `phase1_analyze_*`) previously ran serially. They are now dispatched in parallel via `ThreadPoolExecutor`.

**Config:**
```bash
# Via make target (default auto-detects CPU count, caps at 8)
make discover-blueprints

# Explicit worker count
python project/pipelines/run_all.py --max_analyzer_workers 4 ...
```

---

### [PATCHED] #2 — Vectorized Hot Loops in Phase 2

**File:** `project/pipelines/research/phase2_candidate_discovery.py`

Lambda snapshot builder and loader now use vectorized DataFrame construction instead of row-by-row `iterrows`. This improved performance by ~10x on large event sets.

---

### [PATCHED] #3 — Vectorized Shrinkage and P-Values

**File:** `project/pipelines/research/phase2_candidate_discovery.py`

Statistical calculations for James-Stein shrinkage and two-sided p-values (via `scipy.stats.t.sf`) are now fully vectorized, eliminating row-by-row Python function calls.

---

### [PATCHED] #4 — Stage Output Caching

**File:** `project/pipelines/run_all.py`

Stage manifests now carry an `input_hash` (script mtime + CLI args). On cache hit, the stage is skipped.

**Enable:**
```bash
BACKTEST_STAGE_CACHE=1 make discover-blueprints
```

---

## Tuning Guide

### Memory Pressure

Phase 2 discovery loads all feature bars for each symbol into memory. With `--max_analyzer_workers 8`, peak memory is ~3 GB for a standard 2-symbol run.

If OOM:
```bash
# Reduce workers
python project/pipelines/run_all.py --max_analyzer_workers 2 ...
```

### I/O Bottleneck (Parquet Reads)

The cleaned bar parquet partitions are read by every analyzer. If disk I/O is the bottleneck (spinning disk, NFS):
- Pre-load data to a RAM tmpfs: `cp -r data/lake/cleaned /dev/shm/cleaned && BACKTEST_DATA_ROOT=/dev/shm make discover-blueprints`
- Or reduce symbol count / date range first.

### Profiling a Single Event Type

To debug performance for a specific event analyzer:
```bash
export BACKTEST_DATA_ROOT=$(pwd)/data
python -m cProfile -s cumulative \
  project/pipelines/research/phase2_candidate_discovery.py \
  --run_id discovery_2020_2025 \
  --event_type VOL_SHOCK \
  --symbols BTCUSDT,ETHUSDT \
  2>&1 | head -40
```
