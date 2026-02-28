# Event Taxonomy Deepening Implementation Plan

> **For Gemini:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Formalize event family taxonomy and convert market physics into automatic hypothesis generation templates.

**Architecture:** Create a `spec/multiplicity/taxonomy.yaml` file defining the mapping between event types, event families, and rule templates. Update `generate_candidate_templates.py` to load this taxonomy and apply specific rule templates based on the event's family.

**Tech Stack:** Python 3.x, YAML, Pandas.

---

### Task 1: Define Taxonomy Specification

**Files:**
- Create: `spec/multiplicity/taxonomy.yaml`

**Step 1: Create the taxonomy file**

```yaml
families:
  liquidity_dislocation:
    description: "Temporary liquidity imbalance causing price overshoot and repair."
    expected_behavior: "mean_reversion"
    templates: ["mean_reversion", "liquidity_reversion_v2"]
    events:
      - "liquidity_vacuum"
      - "liquidity_refill_lag_window"
      - "liquidity_absence_window"

  volatility_transition:
    description: "Variance regime change resulting in a repricing phase."
    expected_behavior: "continuation"
    templates: ["continuation", "trend_continuation"]
    events:
      - "vol_shock_relaxation"
      - "vol_aftershock_window"
      - "range_compression_breakout_window"

  positioning_extremes:
    description: "Crowded trades leading to forced liquidation paths."
    expected_behavior: "reversal_or_squeeze"
    templates: ["mean_reversion", "continuation", "trend_continuation"]
    events:
      - "funding_extreme_reversal_window"
      - "funding_extreme_onset"
      - "funding_persistence_window"
      - "funding_normalization"
      - "oi_spike_positive"
      - "oi_spike_negative"
      - "oi_flush"
      - "LIQUIDATION_CASCADE"

  exhaustion_reversion:
    description: "Energy depletion leading to reversal."
    expected_behavior: "mean_reversion"
    templates: ["mean_reversion"]
    events:
      - "directional_exhaustion_after_forced_flow"

  cross_signal:
    description: "Multi-variable coincidences indicating structural desync."
    expected_behavior: "variable"
    templates: ["mean_reversion", "continuation"]
    events:
      - "cross_venue_desync"

default_feature_templates: ["feature_conditioned_prediction"]
```

**Step 2: Commit**

```bash
git add spec/multiplicity/taxonomy.yaml
git commit -m "feat(spec): add event taxonomy specification"
```

### Task 2: Implement Taxonomy Loading in Generator

**Files:**
- Modify: `project/pipelines/research/generate_candidate_templates.py`
- Test: `tests/pipelines/research/test_taxonomy_mapping.py`

**Step 1: Write the failing test**

```python
import pandas as pd
import pytest
from pathlib import Path
from pipelines.research.generate_candidate_templates import _get_templates_for_event

def test_taxonomy_mapping():
    # Test liquidity dislocation
    assert "mean_reversion" in _get_templates_for_event("liquidity_vacuum")
    assert "liquidity_reversion_v2" in _get_templates_for_event("liquidity_vacuum")
    
    # Test volatility transition
    assert "continuation" in _get_templates_for_event("vol_shock_relaxation")
    assert "trend_continuation" in _get_templates_for_event("vol_shock_relaxation")
    
    # Test unknown event (should return defaults)
    assert "mean_reversion" in _get_templates_for_event("unknown_event")
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/pipelines/research/test_taxonomy_mapping.py -v`
Expected: FAIL with "ImportError: cannot import name '_get_templates_for_event'"

**Step 3: Implement taxonomy loading and lookup**

Modify `project/pipelines/research/generate_candidate_templates.py`:
1. Add `import yaml`.
2. Add `_load_taxonomy()` helper.
3. Add `_get_templates_for_event(event_type, taxonomy)` helper.
4. Update `main()` to load taxonomy and use it when creating templates.

**Step 4: Run test to verify it passes**

Run: `pytest tests/pipelines/research/test_taxonomy_mapping.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add project/pipelines/research/generate_candidate_templates.py tests/pipelines/research/test_taxonomy_mapping.py
git commit -m "feat(atlas): integrate taxonomy-based template generation"
```

### Task 3: Verify End-to-End Generation

**Step 1: Run generator**

Run: `source .venv/bin/activate && make discover-edges RUN_ID=taxonomy_verify`

**Step 2: Inspect output**

Verify `data/reports/hypothesis_generator/taxonomy_verify/candidate_plan.jsonl` contains the expected rule templates for different event types.

**Step 3: Commit final changes**

```bash
git add .
git commit -m "chore: finalize taxonomy integration and verify generation"
```
