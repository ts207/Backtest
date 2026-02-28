# Refactor Event Registry to be Spec-Driven Implementation Plan

> **For Gemini:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Transition the Event Registry from a hardcoded Python dictionary to a fully dynamic, Spec-First architecture that loads event definitions from `spec/events/*.yaml`.

**Architecture:** Create YAML specifications for all missing events, implement a spec loader in `registry.py`, and refactor the registry to populate `EVENT_REGISTRY_SPECS` dynamically at runtime.

**Tech Stack:** Python 3.x, PyYAML, Pytest.

---

### Phase 1: Backfill Event Specifications

**Task 1: Create Specs for Volatility Events**

**Files:**
- Create: `spec/events/vol_shock_relaxation.yaml`
- Create: `spec/events/vol_aftershock_window.yaml`
- Create: `spec/events/range_compression_breakout_window.yaml`

**Step 1: Write the failing test**
Create `tests/specs/test_missing_specs.py` to assert these files exist.

```python
import os
from pathlib import Path

def test_volatility_specs_exist():
    root = Path("spec/events")
    assert (root / "vol_shock_relaxation.yaml").exists()
    assert (root / "vol_aftershock_window.yaml").exists()
    assert (root / "range_compression_breakout_window.yaml").exists()
```

**Step 2: Run test to verify it fails**
Run: `pytest tests/specs/test_missing_specs.py -v`

**Step 3: Write minimal implementation**
Create the YAML files with content matching `registry.py`'s hardcoded values:
```yaml
# spec/events/vol_shock_relaxation.yaml
event_type: "vol_shock_relaxation"
reports_dir: "vol_shock_relaxation"
events_file: "vol_shock_relaxation_events.csv"
signal_column: "vol_shock_relaxation_event"
```
(Repeat pattern for others)

**Step 4: Run test to verify it passes**
Run: `pytest tests/specs/test_missing_specs.py -v`

**Step 5: Commit**
```bash
git add spec/events/ tests/specs/test_missing_specs.py
git commit -m "feat(spec): backfill volatility event specifications"
```

**Task 2: Create Specs for Liquidity Events**

**Files:**
- Create: `spec/events/liquidity_refill_lag_window.yaml`
- Create: `spec/events/liquidity_absence_window.yaml`
- Create: `spec/events/liquidity_vacuum.yaml`

**Step 1: Write the failing test**
Update `tests/specs/test_missing_specs.py` to check for these files.

**Step 2: Run test to verify it fails**

**Step 3: Write minimal implementation**
Create YAMLs based on `registry.py` definitions.

**Step 4: Run test to verify it passes**

**Step 5: Commit**
```bash
git add spec/events/
git commit -m "feat(spec): backfill liquidity event specifications"
```

**Task 3: Create Specs for Funding & OI Events**

**Files:**
- Create: `spec/events/funding_extreme_reversal_window.yaml`
- Create: `spec/events/funding_extreme_onset.yaml`
- Create: `spec/events/funding_persistence_window.yaml`
- Create: `spec/events/funding_normalization.yaml`
- Create: `spec/events/oi_spike_positive.yaml`
- Create: `spec/events/oi_spike_negative.yaml`
- Create: `spec/events/oi_flush.yaml`

**Step 1: Write the failing test**
Update test file.

**Step 2: Run test to verify it fails**

**Step 3: Write minimal implementation**
Create YAMLs.

**Step 4: Run test to verify it passes**

**Step 5: Commit**
```bash
git add spec/events/
git commit -m "feat(spec): backfill funding and OI event specifications"
```

**Task 4: Create Specs for Remaining Events**

**Files:**
- Create: `spec/events/directional_exhaustion_after_forced_flow.yaml`
- Create: `spec/events/cross_venue_desync.yaml`

**Step 1: Write the failing test**
Update test file.

**Step 2: Run test to verify it fails**

**Step 3: Write minimal implementation**
Create YAMLs.

**Step 4: Run test to verify it passes**

**Step 5: Commit**
```bash
git add spec/events/
git commit -m "feat(spec): backfill remaining event specifications"
```

### Phase 2: Refactor Registry to Dynamic Loading

**Task 5: Implement Spec Loader in Registry**

**Files:**
- Modify: `project/events/registry.py`
- Test: `tests/events/test_registry_loader.py`

**Step 1: Write the failing test**
```python
from events.registry import _load_event_specs, EVENT_REGISTRY_SPECS

def test_dynamic_loading():
    # Ensure loaded specs match the hardcoded baseline (sanity check)
    loaded = _load_event_specs()
    assert "vol_shock_relaxation" in loaded
    assert loaded["vol_shock_relaxation"].signal_column == "vol_shock_relaxation_event"
```

**Step 2: Run test to verify it fails**
Run: `pytest tests/events/test_registry_loader.py -v` (Function won't exist)

**Step 3: Implement loader**
In `project/events/registry.py`:
1. Import `yaml`.
2. Add `_load_event_specs()` function to scan `spec/events/*.yaml`.
3. Replace hardcoded `EVENT_REGISTRY_SPECS` with a call to `_load_event_specs()`.

**Step 4: Run test to verify it passes**

**Step 5: Commit**
```bash
git add project/events/registry.py tests/events/test_registry_loader.py
git commit -m "refactor(registry): switch to dynamic spec loading"
```

### Phase 3: Cleanup and Centralization

**Task 6: Centralize Global Defaults**

**Files:**
- Create: `spec/global_defaults.yaml`
- Modify: `project/pipelines/research/generate_candidate_templates.py`
- Modify: `project/pipelines/research/phase2_candidate_discovery.py`

**Step 1: Create defaults spec**
```yaml
defaults:
  horizons: ["5m", "15m", "60m"]
  rule_templates: ["mean_reversion", "continuation", "trend_continuation", "liquidity_reversion_v2"]
  conditioning:
    vol_regime: ["high", "low"]
    # ... others
```

**Step 2: Write failing test**
Check that scripts load these defaults instead of hardcoded values.

**Step 3: Update scripts**
Modify both Python scripts to load `spec/global_defaults.yaml`.

**Step 4: Run tests**

**Step 5: Commit**
```bash
git add .
git commit -m "refactor(config): centralize global defaults"
```
