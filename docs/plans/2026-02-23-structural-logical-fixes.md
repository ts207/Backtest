# Backtest Structural and Logical Fixes Implementation Plan

> **For Gemini:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix confirmed structural failures, syntax errors, and data integrity issues in the Backtest research pipeline to ensure end-to-end runnability and logical correctness.

**Architecture:** Surgical fixes to existing scripts to resolve import inconsistencies, indentation errors, syntax errors, and miscalculations in data processing stages.

**Tech Stack:** Python 3.x, Pandas, NumPy, Parquet (PyArrow).

---

### Task 1: Fix Incorrect Package Imports

**Files:**
- Modify: `project/pipelines/features/build_market_context.py`
- Modify: `project/eval/debug_microstructure.py`
- Modify: `project/eval/microstructure_acceptance.py`
- Modify: `project/eval/microstructure_calibration.py`

**Step 1: Fix imports in `build_market_context.py`**
Replace `from project.features...` with `from features...`.
Also ensure `PROJECT_ROOT` is handled consistently.

**Step 2: Fix imports in `eval/` scripts**
Ensure these scripts can run by either fixing the imports to `from features...` or ensuring the root is in `sys.path` correctly. Since they currently add both root and `project/` to `sys.path`, `from features` is safer.

**Step 3: Verify fix**
Run `python project/pipelines/features/build_market_context.py --help` to ensure no `ModuleNotFoundError`.

### Task 2: Fix `run_walkforward.py` IndentationError

**Files:**
- Modify: `project/pipelines/eval/run_walkforward.py`

**Step 1: Correct indentation around line 1018**
The `summary = {` block is currently misaligned.

**Step 2: Verify fix**
Run `python -m compileall project/pipelines/eval/run_walkforward.py`.

### Task 3: Fix `runner.py` SyntaxError

**Files:**
- Modify: `project/pipelines/engine/runner.py`

**Step 1: Correct the escaped dictionary key**
Change `"dsl": dsl_diagnostics,` to `"dsl": dsl_diagnostics,` at line 436.

**Step 2: Verify fix**
Run `python -m compileall project/pipelines/engine/runner.py`.

### Task 4: Fix Funding Scaling in `build_cleaned_5m.py`

**Files:**
- Modify: `project/pipelines/clean/build_cleaned_5m.py`

**Step 1: Update `bars_per_event` for 5m bars**
Change `bars_per_event = int((interval_hours * 60) / 1)` to `bars_per_event = int((interval_hours * 60) / 5)`.
This ensures funding is correctly distributed across 5m bars in an 8h window (96 bars).

**Step 2: Verify fix**
Check the calculation logic in the file.

### Task 5: Fix Ingest Completeness Calculation

**Files:**
- Modify: `project/pipelines/ingest/ingest_binance_um_ohlcv_5m.py`

**Step 1: Update `_expected_bars` for 5m bars**
Change `(15 * 60)` to `(5 * 60)` in the divisor.

**Step 2: Verify fix**
Check the calculation logic in the file.

### Task 6: Synchronize Feature Schema Timeframe

**Files:**
- Modify: `project/schemas/feature_schema_v1.json`

**Step 1: Rename schema key to match storage**
Change `"features_v1_15m_v1"` to `"features_v1_5m_v1"` to reflect the actual 5m timeframe used in the project.

### Task 7: Resolve Phase 2 Quality Floor Contradiction

**Files:**
- Modify: `project/pipelines/research/phase2_candidate_discovery.py`

**Step 1: Update quality score logic**
Ensure `robustness_score` (and `phase2_quality_score`) is assigned a value that allows valid fallback candidates to pass the 0.66 floor.
Specifically, verify if `(econ + econ_cons + stability) / 3` is sufficient or if it should be more generous for fallback tracks.
Actually, I will change it to be more explicit about the 0.66 floor if it passes 2 out of 3 gates.

**Step 2: Verify fix**
Check that `compile_eligible_phase2_fallback` calculation uses the correct logic.

---

### Final Verification Task

**Step 1: Run `compileall` on the whole project**
Run `python -m compileall project` to ensure no remaining syntax or indentation errors.

**Step 2: (Optional) Run a dry-run of the pipeline**
Run `python project/pipelines/run_all.py --help` to ensure the orchestrator is still valid.
