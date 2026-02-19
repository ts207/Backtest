# Design: WF Trim as First-Class Stage Gate

**Date**: 2026-02-19
**Status**: Approved
**Scope**: `compile_strategy_blueprints.py`, `strategy_dsl/schema.py`, downstream consumers, tests

---

## Problem

Three audit gaps identified from `expansion_run_v1`:

1. **WF trim is a hidden gate**: Blueprints dropped by walkforward historical trim simply vanish — no per-blueprint record, no audit trail, no evidence source.
2. **No evidence source hash**: `walkforward_summary.json` is consumed but never fingerprinted, so runs cannot prove they used the same evidence.
3. **min_events silent bypass**: `run_promotion_pipeline.py` passes `--min_events_floor 1`, overriding the constant `QUALITY_MIN_EVENTS = 100`. The summary records the constant, not the actual value used.

---

## Architecture Change: Write-then-Flag (Option A)

### Current flow
```
build blueprints → WF trim (drops blueprints) → behavior dedupe → write survivors to blueprints.jsonl
```

### New flow
```
build blueprints → behavior dedupe → annotate WF status per-blueprint → write ALL to blueprints.jsonl → report funnel
```

Key principle: **no blueprint is silently dropped**. Every built blueprint is written; trimmed ones are flagged via `lineage.wf_status`.

---

## Schema Changes (`strategy_dsl/schema.py`)

Add three optional fields to `LineageSpec`:

```python
@dataclass(frozen=True)
class LineageSpec:
    source_path: str
    compiler_version: str
    generated_at_utc: str
    wf_evidence_hash: str = ""           # SHA-256 of walkforward_summary.json; empty = no evidence used
    wf_status: str = "pass"              # "pass" | "trimmed_zero_trade" | "trimmed_worst_negative" | "pending"
    events_count_used_for_gate: int = 0  # actual n_events from candidate row used in quality floor check
    min_events_threshold: int = 0        # actual --min_events_floor value applied at compile time
```

All fields are optional with safe defaults, preserving backwards compatibility with existing blueprints (missing fields → defaults).

`validate()` must allow `wf_status` in `{"pass", "trimmed_zero_trade", "trimmed_worst_negative", "pending"}`.

---

## Compiler Changes (`compile_strategy_blueprints.py`)

### 1. Evidence hashing

`_load_walkforward_strategy_metrics()` returns a `(dict, str)` tuple:
- `dict`: existing per-strategy split metrics
- `str`: SHA-256 hex digest of the raw file bytes; `""` if file absent

### 2. WF annotation instead of trim

`_trim_blueprints_with_walkforward_evidence()` is **replaced** by `_annotate_blueprints_with_walkforward_evidence()`:
- Returns `(List[Blueprint], Dict[str, object])` — same signature concept, but returns all blueprints
- Blueprints to be "trimmed" instead get `lineage.wf_status` set to `"trimmed_zero_trade"` or `"trimmed_worst_negative"`
- Blueprint objects are frozen dataclasses, so annotation requires rebuilding with `dataclasses.replace(bp, lineage=new_lineage)`
- Trim logic (zero-trade detection, worst-K negative) is identical to current

### 3. min_events stamping in quality floor

`_passes_quality_floor()` gains a new return type or the caller stamps fields after selection. The cleaner approach: stamp `events_count_used_for_gate` and `min_events_threshold` onto the blueprint's lineage at build time — `_build_blueprint()` receives `min_events` as a parameter and stamps it.

### 4. Fail-closed behavior change

- **Before**: `wf_trimmed_all=True` → `raise ValueError` → exit 1, no blueprints written
- **After**: All blueprints written (including trimmed), but if ALL are trimmed → raise ValueError → exit 1 (still fail-closed; operator must investigate)

The only case that changes is partial trim: previously partial trim dropped blueprints silently; now they're written with `wf_status` flags.

### 5. Funnel report in `blueprints_summary.json`

New top-level `compile_funnel` key:

```json
{
  "compile_funnel": {
    "selected_for_build": 10,
    "build_success": 10,
    "build_fail_non_executable": 0,
    "build_fail_naive_entry": 0,
    "build_fail_exception": 0,
    "behavior_dedup_drop": 0,
    "wf_trim_zero_trade": 2,
    "wf_trim_worst_negative": 0,
    "written_total": 10,
    "written_active": 8,
    "written_trimmed": 2
  },
  "wf_evidence_hash": "sha256:abc123...",
  "wf_evidence_source": "reports/eval/{run_id}/walkforward_summary.json",
  "min_events_threshold_used": 1
}
```

The existing `historical_trim` key is kept for backwards compatibility but superseded by `compile_funnel`.

---

## Downstream Consumers

Stages that consume `blueprints.jsonl` must filter to active blueprints:

```python
active_blueprints = [
    bp for bp in all_blueprints
    if not bp.get("lineage", {}).get("wf_status", "pass").startswith("trimmed")
]
```

Files to audit: `stress_test_blueprints.py`, `run_walkforward.py`.

---

## Test Changes

| Test | Change |
|------|--------|
| `test_compiler_trims_zero_trade_blueprint_from_walkforward` | Trimmed blueprint IS in JSONL with `wf_status="trimmed_zero_trade"`; not in active set |
| `test_compiler_walkforward_trim_all_fails_closed` | All blueprints written but all flagged; compiler still returns 1 |
| `test_compiler_walkforward_trim_uses_validation_split_only` | Trimmed blueprint in JSONL flagged, surviving one active |
| **New**: `test_compiler_wf_evidence_hash_present_when_evidence_exists` | `lineage.wf_evidence_hash` non-empty when WF data present |
| **New**: `test_compiler_wf_evidence_hash_empty_when_no_evidence` | `lineage.wf_evidence_hash == ""` when no WF data |
| **New**: `test_compiler_min_events_threshold_stamped_in_lineage` | `lineage.min_events_threshold` matches `--min_events_floor` arg; `lineage.events_count_used_for_gate` matches candidate `n_events` |
| **New**: `test_compiler_summary_contains_compile_funnel` | `blueprints_summary.json` has `compile_funnel` with correct counts |

---

## Files to Change

1. `project/strategy_dsl/schema.py` — extend `LineageSpec`
2. `project/pipelines/research/compile_strategy_blueprints.py` — replace trim with annotate, add hashing, stamp min_events, update summary
3. `project/pipelines/research/stress_test_blueprints.py` — filter active blueprints
4. `project/pipelines/eval/run_walkforward.py` — filter active blueprints (if it consumes blueprints.jsonl directly)
5. `tests/test_compile_strategy_blueprints.py` — update 3 tests, add 4 new tests
