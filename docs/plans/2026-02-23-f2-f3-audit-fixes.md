# F-2 + F-3 Audit Fix Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix two pre-gate audit findings — F-3 (BH families pool symbols, inflating per-symbol discovery rate) and F-2 (ablation lift has no multiplicity correction, producing ~30 false lift discoveries).

**Architecture:**
- F-3 is a one-line change in two places: prepend `{symbol}_` to `family_id` in `phase2_candidate_discovery.py`. The BH groupby already uses `family_id`, so the fix flows automatically.
- F-2 adds a BH pass inside `ablation.py`: after computing lift rows within each (event, rule, horizon, symbol) group, collect the phase2 `p_value` for each conditioned row and call `benjamini_hochberg` from the existing `project/eval/multiplicity.py`. Output gains `lift_q_value` and `is_lift_discovery` columns.

**Tech Stack:** Python, pandas, numpy, pytest. No new dependencies. BH implementation already in `project/eval/multiplicity.py`.

---

## Task 1 (F-3): Write the failing test for symbol-stratified family_id

**Files:**
- Create: `tests/pipelines/research/test_family_id_symbol_stratified.py`

**Step 1: Write the failing test**

```python
"""
F-3: family_id must include symbol so BH-FDR is applied per-symbol, not pooled.
"""
from __future__ import annotations
import pandas as pd
import pytest

SAMPLE_ROWS = [
    {"event_type": "VOL_SHOCK", "rule_template": "mean_reversion", "horizon": "5m",
     "symbol": "BTCUSDT", "conditioning": "all"},
    {"event_type": "VOL_SHOCK", "rule_template": "mean_reversion", "horizon": "5m",
     "symbol": "ETHUSDT", "conditioning": "all"},
    {"event_type": "VOL_SHOCK", "rule_template": "mean_reversion", "horizon": "5m",
     "symbol": "BTCUSDT", "conditioning": "vol_regime_high"},
]

def _build_family_id(row: dict) -> str:
    """Reproduce the family_id construction from phase2_candidate_discovery.py."""
    from project.pipelines.research.phase2_candidate_discovery import _make_family_id
    return _make_family_id(row["symbol"], row["event_type"], row["rule_template"],
                           row["horizon"], row["conditioning"])

class TestFamilyIdIncludesSymbol:
    def test_family_id_starts_with_symbol(self):
        for row in SAMPLE_ROWS:
            fid = _build_family_id(row)
            assert fid.startswith(row["symbol"]), (
                f"family_id '{fid}' must start with symbol '{row['symbol']}'"
            )

    def test_btc_and_eth_have_different_family_ids_for_same_template(self):
        btc = _build_family_id(SAMPLE_ROWS[0])
        eth = _build_family_id(SAMPLE_ROWS[1])
        assert btc != eth, "BTC and ETH must produce different family_ids for same (event, rule, horizon, cond)"

    def test_family_id_format_is_symbol_prefixed(self):
        fid = _build_family_id(SAMPLE_ROWS[0])
        parts = fid.split("_", 1)
        assert parts[0] == "BTCUSDT", f"First segment must be symbol, got '{parts[0]}'"
```

**Step 2: Run to verify it fails**

```bash
source .venv/bin/activate && python -m pytest tests/pipelines/research/test_family_id_symbol_stratified.py -v
```

Expected: `ImportError` — `_make_family_id` does not exist yet.

---

## Task 2 (F-3): Expose `_make_family_id` helper and fix both `family_id` assignments

**Files:**
- Modify: `project/pipelines/research/phase2_candidate_discovery.py` (two `family_id` lines + add helper)

**Step 1: Add the helper function near the top of the module** (after imports, before the main logic)

Find the first function definition in the file and insert before it:

```python
def _make_family_id(symbol: str, event_type: str, rule: str, horizon: str, cond_label: str) -> str:
    """BH family key: stratified by symbol so FDR is controlled per-symbol (F-3 fix)."""
    return f"{symbol}_{event_type}_{rule}_{horizon}_{cond_label}"
```

**Step 2: Fix line ~542** (inside the inner loop in `_run_single_event_analysis` or equivalent)

Find:
```python
"family_id": f"{event_type}_{rule}_{horizon}_{cond_label}",
```
Replace with:
```python
"family_id": _make_family_id(symbol, event_type, rule, horizon, cond_label),
```

**Step 3: Fix line ~638** (second loop, atlas mode path)

Find:
```python
"family_id": f"{args.event_type}_{rule}_{horizon}_{cond_name}",
```
Replace with:
```python
"family_id": _make_family_id(symbol, args.event_type, rule, horizon, cond_name),
```

**Step 4: Update the report metadata string**

Find:
```python
"family_definition": "Option A (event_type, rule_template, horizon)",
```
Replace with:
```python
"family_definition": "Option B (symbol, event_type, rule_template, horizon) — F-3 fix",
```

**Step 5: Run the test to verify it passes**

```bash
source .venv/bin/activate && python -m pytest tests/pipelines/research/test_family_id_symbol_stratified.py -v
```

Expected: 3 passed.

**Step 6: Run the full suite to catch regressions**

```bash
source .venv/bin/activate && python -m pytest tests/ -q \
  --ignore=tests/pipelines/backtest/test_compile_fallback.py \
  --ignore=tests/pipelines/research/test_compile_fallback.py
```

Expected: all pass.

**Step 7: Commit**

```bash
git add project/pipelines/research/phase2_candidate_discovery.py \
        tests/pipelines/research/test_family_id_symbol_stratified.py
git commit -m "fix(F-3): stratify BH family by symbol in phase2_candidate_discovery

family_id now includes symbol prefix so BH-FDR is applied per-symbol
rather than pooled across symbols. Prevents cross-symbol power leakage
where ETH/SOL events inflate BTC discovery rate and vice versa.

Adds _make_family_id() helper; used in both result-building loops.
Audit finding F-3 resolved."
```

---

## Task 3 (F-2): Write the failing test for BH-corrected ablation lift

**Files:**
- Create: `tests/eval/test_ablation_bh.py`

**Step 1: Write the failing tests**

```python
"""
F-2: ablation lift must apply BH multiplicity correction across conditions.

Tests that calculate_lift (or a new calculate_lift_with_bh) returns
lift_q_value and is_lift_discovery columns.
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from eval.ablation import calculate_lift


def _make_group(conditions: list[dict]) -> pd.DataFrame:
    """Build a group_df as ablation.py would receive it."""
    return pd.DataFrame(conditions)


class TestAblationBH:
    def test_output_has_lift_q_value_column(self):
        group = _make_group([
            {"candidate_id": "c1", "condition_key": "all",           "expectancy": 0.001, "n_events": 100, "p_value": 1.0},
            {"candidate_id": "c2", "condition_key": "vol_regime_high","expectancy": 0.003, "n_events":  50, "p_value": 0.01},
            {"candidate_id": "c3", "condition_key": "vol_regime_low", "expectancy": 0.001, "n_events":  50, "p_value": 0.80},
        ])
        result = calculate_lift(group)
        assert "lift_q_value" in result.columns, "lift_q_value column missing from ablation output"

    def test_output_has_is_lift_discovery_column(self):
        group = _make_group([
            {"candidate_id": "c1", "condition_key": "all",           "expectancy": 0.001, "n_events": 100, "p_value": 1.0},
            {"candidate_id": "c2", "condition_key": "vol_regime_high","expectancy": 0.003, "n_events":  50, "p_value": 0.01},
        ])
        result = calculate_lift(group)
        assert "is_lift_discovery" in result.columns

    def test_significant_condition_is_flagged_as_discovery(self):
        """A condition with p=0.001 should survive BH at alpha=0.10."""
        group = _make_group([
            {"candidate_id": "c1", "condition_key": "all",           "expectancy": 0.001, "n_events": 200, "p_value": 1.0},
            {"candidate_id": "c2", "condition_key": "vol_regime_high","expectancy": 0.005, "n_events": 100, "p_value": 0.001},
            {"candidate_id": "c3", "condition_key": "vol_regime_low", "expectancy": 0.001, "n_events": 100, "p_value": 0.90},
        ])
        result = calculate_lift(group)
        sig = result[result["condition"] == "vol_regime_high"]
        assert not sig.empty
        assert bool(sig.iloc[0]["is_lift_discovery"]) is True

    def test_noise_condition_is_not_flagged_as_discovery(self):
        """A condition with p=0.90 should NOT survive BH at alpha=0.10."""
        group = _make_group([
            {"candidate_id": "c1", "condition_key": "all",          "expectancy": 0.001, "n_events": 200, "p_value": 1.0},
            {"candidate_id": "c2", "condition_key": "vol_regime_low","expectancy": 0.001, "n_events": 100, "p_value": 0.90},
        ])
        result = calculate_lift(group)
        noise = result[result["condition"] == "vol_regime_low"]
        assert not noise.empty
        assert bool(noise.iloc[0]["is_lift_discovery"]) is False

    def test_bh_uses_alpha_010_threshold(self):
        """lift_q_value ≤ 0.10 iff is_lift_discovery is True."""
        group = _make_group([
            {"candidate_id": "c1", "condition_key": "all",           "expectancy": 0.001, "n_events": 200, "p_value": 1.0},
            {"candidate_id": "c2", "condition_key": "cond_a",        "expectancy": 0.005, "n_events": 100, "p_value": 0.02},
            {"candidate_id": "c3", "condition_key": "cond_b",        "expectancy": 0.001, "n_events": 100, "p_value": 0.80},
        ])
        result = calculate_lift(group)
        for _, row in result.iterrows():
            if row["lift_q_value"] <= 0.10:
                assert row["is_lift_discovery"] is True or row["is_lift_discovery"] == True
            else:
                assert row["is_lift_discovery"] is False or row["is_lift_discovery"] == False

    def test_empty_result_when_no_baseline(self):
        """Group with no 'all' condition returns empty DataFrame (existing behavior preserved)."""
        group = _make_group([
            {"candidate_id": "c2", "condition_key": "vol_regime_high", "expectancy": 0.003, "n_events": 50, "p_value": 0.01},
        ])
        result = calculate_lift(group)
        assert result.empty
```

**Step 2: Run to verify it fails**

```bash
source .venv/bin/activate && python -m pytest tests/eval/test_ablation_bh.py -v
```

Expected: 5 failures — `lift_q_value` and `is_lift_discovery` columns do not exist yet.

---

## Task 4 (F-2): Add BH correction to `calculate_lift` in `ablation.py`

**Files:**
- Modify: `project/eval/ablation.py`

**Step 1: Add the import for `benjamini_hochberg`**

At the top of `project/eval/ablation.py`, after existing imports, add:

```python
from eval.multiplicity import benjamini_hochberg
```

**Step 2: Extend `calculate_lift` to accept and use `p_value`**

The `out_rows` dict currently has: `candidate_id`, `condition`, `baseline_expectancy`, `conditioned_expectancy`, `lift_bps`, `lift_pct`, `n_events`.

Replace the entire `calculate_lift` function with:

```python
def calculate_lift(group_df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-condition lift vs baseline ('all') with BH multiplicity correction.

    Requires columns: candidate_id, condition_key, expectancy, n_events.
    Optional column: p_value (used for BH; defaults to 1.0 if absent).
    Returns DataFrame with lift_q_value and is_lift_discovery added (F-2 fix).
    """
    rows = []
    for _, row in group_df.iterrows():
        condition = str(row.get("condition_key", "unknown")).strip()
        rows.append({
            "candidate_id": row["candidate_id"],
            "condition": condition,
            "expectancy": row.get("expectancy", 0.0),
            "n_events": row.get("n_events", 0),
            "p_value": float(row.get("p_value", 1.0)),
        })

    df = pd.DataFrame(rows)

    baseline = df[df["condition"] == "all"]
    if baseline.empty:
        return pd.DataFrame()

    base_exp = baseline["expectancy"].mean()

    out_rows = []
    for _, row in df.iterrows():
        if row["condition"] == "all":
            continue
        lift = row["expectancy"] - base_exp
        lift_pct = (lift / abs(base_exp)) if base_exp != 0 else 0.0
        out_rows.append({
            "candidate_id": row["candidate_id"],
            "condition": row["condition"],
            "baseline_expectancy": base_exp,
            "conditioned_expectancy": row["expectancy"],
            "lift_bps": lift * 10000.0,
            "lift_pct": lift_pct,
            "n_events": row["n_events"],
            "p_value": row["p_value"],
        })

    if not out_rows:
        return pd.DataFrame()

    result = pd.DataFrame(out_rows)

    # BH multiplicity correction across all conditions in this group (F-2 fix).
    _BH_ALPHA = 0.10
    _, q_values = benjamini_hochberg(result["p_value"].tolist(), alpha=_BH_ALPHA)
    result["lift_q_value"] = q_values
    result["is_lift_discovery"] = result["lift_q_value"] <= _BH_ALPHA

    return result.drop(columns=["p_value"])
```

**Step 3: Run the tests**

```bash
source .venv/bin/activate && python -m pytest tests/eval/test_ablation_bh.py -v
```

Expected: 6 passed.

**Step 4: Run the full suite**

```bash
source .venv/bin/activate && python -m pytest tests/ -q \
  --ignore=tests/pipelines/backtest/test_compile_fallback.py \
  --ignore=tests/pipelines/research/test_compile_fallback.py
```

Expected: all pass.

**Step 5: Commit**

```bash
git add project/eval/ablation.py tests/eval/test_ablation_bh.py
git commit -m "fix(F-2): add BH multiplicity correction to ablation lift

calculate_lift() now applies benjamini_hochberg() across conditions
within each (event, rule, horizon, symbol) group. Adds lift_q_value
and is_lift_discovery columns to ablation_report.parquet and
lift_summary.csv. Only conditions with q ≤ 0.10 are flagged as
lift discoveries, preventing ~30 false positive lift claims.

Audit finding F-2 resolved."
```

---

## Task 5: Update AUDIT.md to mark F-2 and F-3 resolved

**Files:**
- Modify: `docs/AUDIT.md`

Mark F-2 and F-3 as `✅ RESOLVED` in:
1. Section 5 findings (`#: F-2` and `#: F-3` headers)
2. Section 6.2 Statistical Integrity checklist (`Ablation multiplicity correction` and `Symbol stratification in families` rows)
3. Section 7.1 action items (`PR-2` and `PR-3`)

```bash
git add docs/AUDIT.md
git commit -m "docs: mark F-2 and F-3 resolved in AUDIT.md"
```

---

## Acceptance Criteria

| Check | Command | Expected |
|---|---|---|
| F-3 unit tests | `pytest tests/pipelines/research/test_family_id_symbol_stratified.py -v` | 3 passed |
| F-2 unit tests | `pytest tests/eval/test_ablation_bh.py -v` | 6 passed |
| Full suite | `pytest tests/ -q --ignore=...compile_fallback...` | all pass |
| lift_summary.csv schema | run ablation on any phase2 output | has `lift_q_value` + `is_lift_discovery` columns |
| phase2 report family_def | inspect `phase2_report.json` | `"family_definition"` includes "symbol" |
