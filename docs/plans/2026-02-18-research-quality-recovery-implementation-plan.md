# Research Quality Recovery Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Improve discovery quality, strategy candidate quality, and testing confidence so promoted strategies have better OOS behavior.

**Architecture:** Add a quality-summary stage after phase2, tighten phase2 OOS scoring/gates, enforce quality-ranked strategy selection, and enrich eval metrics using proven quant diagnostics. Keep changes contract-driven with test-first execution and deterministic artifact paths.

**Tech Stack:** Python 3.10, pandas, numpy, pytest, existing pipeline manifests, optional `empyrical-reloaded` from quant resources.

---

### Task 1: Add Discovery Quality Summary Stage

**Files:**
- Create: `project/pipelines/research/summarize_discovery_quality.py`
- Modify: `project/pipelines/run_all.py`
- Test: `tests/test_summarize_discovery_quality.py`
- Test: `tests/test_run_all_phase2.py`

**Step 1: Write the failing test**

```python
def test_summarize_discovery_quality_writes_expected_schema(tmp_path):
    run_id = "qsum_case"
    # Arrange a minimal phase2 candidate csv with gate/fail fields.
    # Run summarize_discovery_quality.main([...]).
    # Assert discovery_quality_summary.json contains:
    #   run_id, total_candidates, gate_pass_rate, top_fail_reasons, by_event_family.
```

**Step 2: Run test to verify it fails**

Run: `./.venv/bin/pytest tests/test_summarize_discovery_quality.py::test_summarize_discovery_quality_writes_expected_schema -v`  
Expected: FAIL with module/script missing.

**Step 3: Write minimal implementation**

```python
def build_summary(df: pd.DataFrame) -> dict:
    gate_pass = pd.to_numeric(df.get("gate_pass", 0), errors="coerce").fillna(0).astype(int)
    fail_reasons = df.get("fail_reasons", "").fillna("").astype(str)
    return {
        "total_candidates": int(len(df)),
        "gate_pass_rate": float(gate_pass.mean()) if len(df) else 0.0,
        "top_fail_reasons": _top_fail_reasons(fail_reasons),
        "by_event_family": _event_family_stats(df),
    }
```

**Step 4: Run test to verify it passes**

Run: `./.venv/bin/pytest tests/test_summarize_discovery_quality.py tests/test_run_all_phase2.py -q`  
Expected: PASS for new summary stage contract.

**Step 5: Commit**

```bash
git add project/pipelines/research/summarize_discovery_quality.py project/pipelines/run_all.py tests/test_summarize_discovery_quality.py tests/test_run_all_phase2.py
git commit -m "feat: add discovery quality summary stage and orchestration hook"
```

### Task 2: Tighten Phase2 Quality Scoring and OOS Gates

**Files:**
- Modify: `project/pipelines/research/phase2_conditional_hypotheses.py`
- Test: `tests/test_phase2_conditional_hypotheses.py`
- Test: `tests/test_analyze_conditional_expectancy_multiplicity.py`

**Step 1: Write the failing test**

```python
def test_phase2_outputs_quality_score_and_strict_oos_gate():
    # Build tiny synthetic candidate frame with mixed OOS quality.
    # Assert output columns contain:
    #   quality_score, gate_oos_consistency_strict
    # Assert low OOS rows fail strict gate.
```

**Step 2: Run test to verify it fails**

Run: `./.venv/bin/pytest tests/test_phase2_conditional_hypotheses.py::test_phase2_outputs_quality_score_and_strict_oos_gate -v`  
Expected: FAIL because new columns/gates do not exist yet.

**Step 3: Write minimal implementation**

```python
df["quality_score"] = (
    0.35 * df["expectancy_after_multiplicity"].clip(lower=0.0)
    + 0.25 * df["robustness_score"].clip(lower=0.0)
    + 0.20 * df["delay_robustness_score"].clip(lower=0.0)
    + 0.20 * df["profit_density_score"].clip(lower=0.0)
)
df["gate_oos_consistency_strict"] = (
    df["gate_oos_validation"].astype(bool)
    & df["gate_oos_validation_test"].astype(bool)
    & (df["validation_samples"] >= 120)
    & (df["test_samples"] >= 120)
)
```

**Step 4: Run test to verify it passes**

Run: `./.venv/bin/pytest tests/test_phase2_conditional_hypotheses.py tests/test_analyze_conditional_expectancy_multiplicity.py -q`  
Expected: PASS with deterministic score/gate behavior.

**Step 5: Commit**

```bash
git add project/pipelines/research/phase2_conditional_hypotheses.py tests/test_phase2_conditional_hypotheses.py tests/test_analyze_conditional_expectancy_multiplicity.py
git commit -m "feat: add strict oos quality scoring and gating in phase2"
```

### Task 3: Quality-Ranked and Diversity-Aware Strategy Candidate Selection

**Files:**
- Modify: `project/pipelines/research/build_strategy_candidates.py`
- Test: `tests/test_build_strategy_candidates.py`

**Step 1: Write the failing test**

```python
def test_builder_prefers_high_quality_and_enforces_per_event_caps(tmp_path):
    # Arrange promoted + phase2 candidates across multiple events.
    # Run builder with max_per_event=2.
    # Assert highest quality_score selected first and cap enforced.
```

**Step 2: Run test to verify it fails**

Run: `./.venv/bin/pytest tests/test_build_strategy_candidates.py::test_builder_prefers_high_quality_and_enforces_per_event_caps -v`  
Expected: FAIL because selection policy/caps not implemented.

**Step 3: Write minimal implementation**

```python
def _candidate_rank_key(row: dict) -> tuple:
    return (
        -_safe_float(row.get("quality_score"), 0.0),
        -_safe_float(row.get("expectancy_after_multiplicity"), 0.0),
        -_safe_float(row.get("robustness_score"), 0.0),
        SOURCE_PRIORITY.get(str(row.get("source", "")), 9),
    )
```

```python
# During selection, track per-event/per-symbol counts and skip rows that exceed caps.
if per_event_count[event] >= args.max_candidates_per_event:
    continue
```

**Step 4: Run test to verify it passes**

Run: `./.venv/bin/pytest tests/test_build_strategy_candidates.py -q`  
Expected: PASS with stable selection order and cap behavior.

**Step 5: Commit**

```bash
git add project/pipelines/research/build_strategy_candidates.py tests/test_build_strategy_candidates.py
git commit -m "feat: enforce quality-ranked diversified strategy candidate selection"
```

### Task 4: Add Standardized Walk-Forward Metrics (Empyrical-Style)

**Files:**
- Modify: `requirements.txt`
- Modify: `project/pipelines/eval/run_walkforward.py`
- Test: `tests/test_run_walkforward.py`

**Step 1: Write the failing test**

```python
def test_walkforward_emits_standardized_metrics_payload(tmp_path):
    # Run minimal walkforward fixture.
    # Assert walkforward_summary.json includes:
    #   sharpe, sortino, calmar, max_drawdown, annual_return
    # and metrics_source in {"empyrical_reloaded", "fallback"}.
```

**Step 2: Run test to verify it fails**

Run: `./.venv/bin/pytest tests/test_run_walkforward.py::test_walkforward_emits_standardized_metrics_payload -v`  
Expected: FAIL because metrics payload/fields are missing.

**Step 3: Write minimal implementation**

```python
try:
    import empyrical as ep
    metrics_source = "empyrical_reloaded"
    metrics["sharpe"] = float(ep.sharpe_ratio(returns))
    metrics["sortino"] = float(ep.sortino_ratio(returns))
    metrics["calmar"] = float(ep.calmar_ratio(returns))
except Exception:
    metrics_source = "fallback"
    metrics["sharpe"] = _fallback_sharpe(returns)
    metrics["sortino"] = _fallback_sortino(returns)
    metrics["calmar"] = _fallback_calmar(returns)
```

**Step 4: Run test to verify it passes**

Run: `./.venv/bin/pytest tests/test_run_walkforward.py -q`  
Expected: PASS with deterministic metrics contract.

**Step 5: Commit**

```bash
git add requirements.txt project/pipelines/eval/run_walkforward.py tests/test_run_walkforward.py
git commit -m "feat: add standardized walkforward risk-return metrics"
```

### Task 5: Improve Test Iteration Speed and Reliability

**Files:**
- Create: `pytest.ini`
- Modify: `project/Makefile`
- Modify: `Makefile`
- Create: `tests/conftest.py`
- Test: `tests/test_run_all_phase2.py`
- Test: `tests/test_build_strategy_candidates.py`

**Step 1: Write the failing test**

```python
def test_shared_data_root_fixture_isolation(backtest_data_root):
    assert backtest_data_root.exists()
    # This ensures tests do not leak into repo data/ paths.
```

**Step 2: Run test to verify it fails**

Run: `./.venv/bin/pytest tests/test_run_all_phase2.py::test_shared_data_root_fixture_isolation -v`  
Expected: FAIL because fixture/config target does not exist.

**Step 3: Write minimal implementation**

```python
# tests/conftest.py
@pytest.fixture
def backtest_data_root(tmp_path, monkeypatch):
    root = tmp_path / "data"
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(root))
    return root
```

```make
# project/Makefile
test-fast:
	$(PYTHON) -m pytest -q -m "not slow" --maxfail=1
```

**Step 4: Run test to verify it passes**

Run: `./.venv/bin/pytest tests/test_run_all_phase2.py tests/test_build_strategy_candidates.py -q`  
Expected: PASS with shared fixture isolation and fast profile available.

**Step 5: Commit**

```bash
git add pytest.ini tests/conftest.py project/Makefile Makefile tests/test_run_all_phase2.py tests/test_build_strategy_candidates.py
git commit -m "chore: add fast deterministic pytest profile and shared fixtures"
```

### Task 6: End-to-End Quality Regression Gate

**Files:**
- Create: `tests/test_quality_pipeline_contract.py`
- Modify: `project/pipelines/run_all.py`

**Step 1: Write the failing test**

```python
def test_quality_pipeline_contract_from_phase2_to_builder(tmp_path):
    # Arrange minimal phase2 + promotion artifacts.
    # Execute orchestrated stages.
    # Assert:
    #  - discovery_quality_summary.json exists,
    #  - strategy_candidates.json exists,
    #  - selected candidates all satisfy gate_oos_consistency_strict.
```

**Step 2: Run test to verify it fails**

Run: `./.venv/bin/pytest tests/test_quality_pipeline_contract.py::test_quality_pipeline_contract_from_phase2_to_builder -v`  
Expected: FAIL before integrated contract is implemented.

**Step 3: Write minimal implementation**

```python
# run_all.py
parser.add_argument("--run_discovery_quality_summary", type=int, default=1)
# Add stage invocation after phase2 and before strategy builder.
```

**Step 4: Run test to verify it passes**

Run: `./.venv/bin/pytest tests/test_quality_pipeline_contract.py -q`  
Expected: PASS with full contract behavior.

**Step 5: Commit**

```bash
git add tests/test_quality_pipeline_contract.py project/pipelines/run_all.py
git commit -m "test: add end-to-end quality regression contract for discovery-to-builder flow"
```

