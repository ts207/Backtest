# Performance Guide

This document covers pipeline performance characteristics, known bottlenecks, applied patches, and tuning parameters.

---

## Wall-time budget (reference)

On a 4-core dev machine with BTCUSDT + ETHUSDT, 2020–2025:

| Stage group | Approx. time (serial) | After parallelism patch |
|-------------|----------------------:|------------------------:|
| Ingest (skip if raw exists) | 0 – 20 min | — |
| Clean + features | ~5 min | ~5 min |
| Event registry × 57 events | ~28 min | **~7 min** (8 workers) |
| Phase 2 discovery × 57 events | ~15 min | ~15 min (sequential) |
| Bridge eval + promotion | ~3 min | ~3 min |
| Backtest (optional) | ~5 min | ~5 min |
| **Total (discovery only)** | **~51 min** | **~30 min** |

---

## Bottleneck inventory and patches

### [PATCHED] #1 — Parallel event analyzers

**File:** `project/pipelines/run_all.py`

All 57 event analyzer stages (`build_event_registry_*`, `phase1_analyze_*`) previously ran serially as blocking `subprocess.run` calls. They are now dispatched in parallel via `ThreadPoolExecutor`.

**Config:**
```bash
# Via make target (default auto-detects CPU count, caps at 8)
make discover-edges

# Explicit worker count
python project/pipelines/run_all.py --max_analyzer_workers 4 ...

# Disable parallelism (sequential fallback for debugging)
python project/pipelines/run_all.py --max_analyzer_workers 1 ...
```

---

### [PATCHED] #2 — `iterrows` in Phase 2 hot loops

**File:** `project/pipelines/research/phase2_candidate_discovery.py`

Lambda snapshot builder and loader previously iterated frame rows one at a time with `iterrows`, building Python dicts. Replaced with vectorized DataFrame slice construction and `pd.concat`.

---

### [PATCHED] #3 — `.apply(lambda)` for `fail_reasons` and `phase2_quality_components`

**File:** `project/pipelines/research/phase2_candidate_discovery.py`

`phase2_quality_components` JSON was serialized row-by-row via `df.apply(json.dumps, axis=1)`. Replaced with vectorized string concatenation — 5–30× faster on large frames.

`fail_reasons` was similarly rebuilt row-by-row. Replaced with vectorized boolean mask columns + `pd.Series.str.cat`.

---

### [PATCHED] #4 — `_aggregate_effect_units` Python list builder

**File:** `phase2_candidate_discovery.py:486`

The shrinkage aggregation loop built a Python list of dicts inside a `groupby`. Refactored to use vectorized `groupby().agg()` over the whole frame.

---

### [PATCHED] #5 — Row-by-row `p_value_shrunk`

**File:** `phase2_candidate_discovery.py:877`

`t_shrunk.apply(_two_sided_p_from_t)` was calling `scipy.stats.t.sf` one row at a time. Replaced with a single vectorized call:
```python
from scipy.stats import t as _scipy_t
p_shrunk.loc[valid_se] = (2.0 * _scipy_t.sf(t_shrunk.loc[valid_se].abs(), df=_df_vals)).clip(0.0, 1.0)
```

---

### [PATCHED] #6 — Stage output caching

**File:** `project/pipelines/run_all.py`

Every `make discover-edges` previously re-ran all stages even when inputs were unchanged. Stage manifests now carry an `input_hash` (script mtime + CLI args). On cache hit the stage is skipped instantly.

**Enable:**
```bash
BACKTEST_STAGE_CACHE=1 make discover-edges
```

> **Note:** Cache is disabled by default to preserve correctness. Enable only when you are confident specs and input data have not changed since the last run.

---

## Tuning guide

### Memory pressure

Phase 2 discovery loads all feature bars for each symbol into memory simultaneously. For 5 years of 5m bars on 2 symbols this is ~400 MB. With `--max_analyzer_workers 8`, peak memory is ~3 GB (8 workers × ~400 MB each).

If OOM:
```bash
# Reduce workers
python project/pipelines/run_all.py --max_analyzer_workers 2 ...

# Or process symbols sequentially
python project/pipelines/run_all.py --symbols BTCUSDT ... && \
python project/pipelines/run_all.py --symbols ETHUSDT ...
```

### I/O bottleneck (parquet reads)

The cleaned bar parquet partitions are read by every analyzer. If disk I/O is the bottleneck (spinning disk, NFS):
- Pre-load data to a RAM tmpfs: `cp -r data/lake/cleaned /dev/shm/cleaned && BACKTEST_DATA_ROOT=/dev/shm make discover-edges`
- Or reduce symbol count / date range first.

### Phase 2 shrinkage time

`_apply_hierarchical_shrinkage` is O(unique_groups × unique_states). With `--grid_expansion` enabled (more horizons/templates), this grows quickly. Profile with:
```bash
python -c "
import cProfile, pstats
# import and call phase2 main
"
```

### Profiling a single event type

```bash
export BACKTEST_DATA_ROOT=$(pwd)/data
python -m cProfile -s cumulative \
  project/pipelines/research/phase2_candidate_discovery.py \
  --run_id discovery_2020_2025 \
  --event_type VOL_SHOCK \
  --symbols BTCUSDT,ETHUSDT \
  2>&1 | head -40
```

---

## Monitoring

```bash
# Check data freshness (exits non-zero if staleness > threshold)
make monitor

# Feature drift (PSI-based)
python project/scripts/monitor_feature_drift.py \
  --run_id discovery_2020_2025 --symbol BTCUSDT --reference_window_days 30

# Full safety check (staleness + drawdown + slippage)
python project/scripts/kill_switch.py \
  --run_id discovery_2020_2025 --symbol BTCUSDT
```
