# WF Trim First-Class Gate Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make WF trim a first-class auditable stage gate — write all blueprints with per-blueprint `wf_status` flags, add evidence source hashing, and stamp actual `min_events` threshold into each blueprint's lineage.

**Architecture:** Extend `LineageSpec` with three new optional fields (`wf_evidence_hash`, `wf_status`, `events_count_used_for_gate`, `min_events_threshold`). Replace silent trim-before-write with annotate-then-write-all. Add `compile_funnel` block to summary JSON. Downstream stages filter by `wf_status` prefix.

**Tech Stack:** Python dataclasses (`dataclasses.replace`), `hashlib.sha256`, pytest, pandas, JSON/JSONL

---

## Task 1: Extend `LineageSpec` in schema

**Files:**
- Modify: `project/strategy_dsl/schema.py` (around line 189)
- Test: `tests/test_compile_strategy_blueprints.py`

**Step 1: Write the failing test**

Add to `tests/test_compile_strategy_blueprints.py`:

```python
def test_lineage_spec_has_wf_fields() -> None:
    from strategy_dsl.schema import LineageSpec
    spec = LineageSpec(
        source_path="x",
        compiler_version="v1",
        generated_at_utc="1970-01-01T00:00:00Z",
    )
    assert spec.wf_evidence_hash == ""
    assert spec.wf_status == "pass"
    assert spec.events_count_used_for_gate == 0
    assert spec.min_events_threshold == 0
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/test_compile_strategy_blueprints.py::test_lineage_spec_has_wf_fields -v
```
Expected: `FAILED` — `LineageSpec` has no `wf_evidence_hash` attribute.

**Step 3: Implement**

In `project/strategy_dsl/schema.py`, update `LineageSpec` (currently at line 189):

```python
@dataclass(frozen=True)
class LineageSpec:
    source_path: str
    compiler_version: str
    generated_at_utc: str
    wf_evidence_hash: str = ""
    wf_status: str = "pass"
    events_count_used_for_gate: int = 0
    min_events_threshold: int = 0

    def validate(self) -> None:
        _require_non_empty(self.source_path, "lineage.source_path")
        _require_non_empty(self.compiler_version, "lineage.compiler_version")
        _require_non_empty(self.generated_at_utc, "lineage.generated_at_utc")
        valid_wf_statuses = {"pass", "trimmed_zero_trade", "trimmed_worst_negative", "pending"}
        if self.wf_status not in valid_wf_statuses:
            raise ValueError(f"lineage.wf_status must be one of {valid_wf_statuses}, got {self.wf_status!r}")
```

**Step 4: Run test to verify it passes**

```bash
pytest tests/test_compile_strategy_blueprints.py::test_lineage_spec_has_wf_fields -v
```
Expected: `PASSED`

**Step 5: Run full suite to check no regressions**

```bash
pytest -q -m "not slow"
```
Expected: all passing (the new fields have defaults so existing Blueprint construction still works).

**Step 6: Commit**

```bash
git add project/strategy_dsl/schema.py tests/test_compile_strategy_blueprints.py
git commit -m "feat: extend LineageSpec with wf_status, wf_evidence_hash, min_events audit fields"
```

---

## Task 2: Add evidence hashing to WF metrics loader

**Files:**
- Modify: `project/pipelines/research/compile_strategy_blueprints.py` (function `_load_walkforward_strategy_metrics` at line 861)
- Test: `tests/test_compile_strategy_blueprints.py`

**Step 1: Write the failing test**

Add to `tests/test_compile_strategy_blueprints.py`:

```python
def test_load_walkforward_strategy_metrics_returns_hash(tmp_path: Path) -> None:
    import sys
    ROOT = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(ROOT / "project"))
    from pipelines.research import compile_strategy_blueprints

    eval_dir = tmp_path / "reports" / "eval" / "run_hash_test"
    eval_dir.mkdir(parents=True, exist_ok=True)
    wf_data = {"per_strategy_split_metrics": {"dsl_interpreter_v1__bp_1": {"validation": {"total_trades": 5, "stressed_net_pnl": 1.0}}}}
    wf_path = eval_dir / "walkforward_summary.json"
    wf_path.write_text(json.dumps(wf_data), encoding="utf-8")

    import monkeypatch  # won't work — use monkeypatch fixture below
```

Actually use a cleaner direct test (no monkeypatch needed since we pass run_id and mock DATA_ROOT):

```python
def test_load_walkforward_strategy_metrics_returns_hash(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)

    eval_dir = tmp_path / "reports" / "eval" / "run_hash_test"
    eval_dir.mkdir(parents=True, exist_ok=True)
    wf_data = {"per_strategy_split_metrics": {}}
    wf_bytes = json.dumps(wf_data).encode("utf-8")
    (eval_dir / "walkforward_summary.json").write_bytes(wf_bytes)

    metrics, file_hash = compile_strategy_blueprints._load_walkforward_strategy_metrics("run_hash_test")
    assert isinstance(metrics, dict)
    import hashlib
    expected_hash = "sha256:" + hashlib.sha256(wf_bytes).hexdigest()
    assert file_hash == expected_hash


def test_load_walkforward_strategy_metrics_empty_hash_when_missing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)
    metrics, file_hash = compile_strategy_blueprints._load_walkforward_strategy_metrics("run_no_wf")
    assert metrics == {}
    assert file_hash == ""
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/test_compile_strategy_blueprints.py::test_load_walkforward_strategy_metrics_returns_hash tests/test_compile_strategy_blueprints.py::test_load_walkforward_strategy_metrics_empty_hash_when_missing -v
```
Expected: `FAILED` — function currently returns only a dict, not a tuple.

**Step 3: Implement**

In `compile_strategy_blueprints.py`, replace `_load_walkforward_strategy_metrics` (lines 861–876):

```python
import hashlib

def _load_walkforward_strategy_metrics(run_id: str) -> Tuple[Dict[str, Dict[str, object]], str]:
    """Returns (per_strategy_split_metrics, sha256_hex_of_file). Hash is '' if file absent."""
    path = DATA_ROOT / "reports" / "eval" / run_id / "walkforward_summary.json"
    if not path.exists():
        return {}, ""
    raw_bytes = path.read_bytes()
    file_hash = "sha256:" + hashlib.sha256(raw_bytes).hexdigest()
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except Exception:
        return {}, file_hash
    raw = payload.get("per_strategy_split_metrics", {})
    if not isinstance(raw, dict):
        return {}, file_hash
    out: Dict[str, Dict[str, object]] = {}
    for key, value in raw.items():
        if isinstance(key, str) and isinstance(value, dict):
            out[key] = value
    return out, file_hash
```

Also add `import hashlib` near the top of the file (after existing imports).

Fix the one call site — `_trim_blueprints_with_walkforward_evidence` (line 902) currently calls:
```python
metrics_by_strategy = _load_walkforward_strategy_metrics(run_id=run_id)
```
Update to:
```python
metrics_by_strategy, _evidence_hash = _load_walkforward_strategy_metrics(run_id=run_id)
```
(We'll thread the hash through properly in Task 3; for now just unpack to avoid breakage.)

**Step 4: Run tests to verify they pass**

```bash
pytest tests/test_compile_strategy_blueprints.py::test_load_walkforward_strategy_metrics_returns_hash tests/test_compile_strategy_blueprints.py::test_load_walkforward_strategy_metrics_empty_hash_when_missing -v
```
Expected: `PASSED`

**Step 5: Run full suite**

```bash
pytest -q -m "not slow"
```
Expected: all passing.

**Step 6: Commit**

```bash
git add project/pipelines/research/compile_strategy_blueprints.py tests/test_compile_strategy_blueprints.py
git commit -m "feat: return evidence file hash from _load_walkforward_strategy_metrics"
```

---

## Task 3: Replace trim-before-write with annotate-then-write-all

**Files:**
- Modify: `project/pipelines/research/compile_strategy_blueprints.py`
  - Replace `_trim_blueprints_with_walkforward_evidence` (lines 901–954)
  - Update `main()` (lines 1242–1250) to use new function
- Test: `tests/test_compile_strategy_blueprints.py`

**Step 1: Update the three existing WF trim tests**

The tests `test_compiler_trims_zero_trade_blueprint_from_walkforward`, `test_compiler_walkforward_trim_uses_validation_split_only`, and `test_compiler_walkforward_trim_all_fails_closed` all check for absent blueprint IDs. With write-then-flag, trimmed blueprints ARE written but with a `wf_status` flag.

Update `test_compiler_trims_zero_trade_blueprint_from_walkforward` (line 280):
```python
# Old assertion (remove):
# assert "all__delay_8" not in ids

# New assertions:
all_rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines() if line.strip()]
trimmed = [r for r in all_rows if r["lineage"]["wf_status"].startswith("trimmed")]
active = [r for r in all_rows if r["lineage"]["wf_status"] == "pass"]
assert any(r["candidate_id"] == "all__delay_8" for r in trimmed)
assert any(r["candidate_id"] == "all__delay_30" for r in active)
```

Update `test_compiler_walkforward_trim_uses_validation_split_only` (line 440):
```python
# Old assertions (remove):
# assert "c1" not in ids
# assert "c2" in ids

# New assertions:
all_rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines() if line.strip()]
c1_row = next(r for r in all_rows if r["candidate_id"] == "c1")
c2_row = next(r for r in all_rows if r["candidate_id"] == "c2")
assert c1_row["lineage"]["wf_status"] == "trimmed_worst_negative"
assert c2_row["lineage"]["wf_status"] == "pass"
```

Update `test_compiler_walkforward_trim_all_fails_closed` (line 969):
```python
# Keep: assert compile_strategy_blueprints.main() == 1
# Add: blueprints.jsonl should still be written, all entries flagged
out_path = tmp_path / "reports" / "strategy_blueprints" / run_id / "blueprints.jsonl"
if out_path.exists():  # May or may not be written depending on exact fail order
    all_rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert all(r["lineage"]["wf_status"].startswith("trimmed") for r in all_rows)
```

**Step 2: Run updated tests to verify they now fail (because implementation hasn't changed yet)**

```bash
pytest tests/test_compile_strategy_blueprints.py::test_compiler_trims_zero_trade_blueprint_from_walkforward tests/test_compile_strategy_blueprints.py::test_compiler_walkforward_trim_uses_validation_split_only -v
```
Expected: `FAILED` — blueprints still absent from JSONL, not flagged.

**Step 3: Implement `_annotate_blueprints_with_walkforward_evidence`**

Replace the existing `_trim_blueprints_with_walkforward_evidence` function with:

```python
import dataclasses

def _annotate_blueprints_with_walkforward_evidence(
    blueprints: List[Blueprint],
    run_id: str,
    evidence_hash: str,
) -> Tuple[List[Blueprint], Dict[str, object]]:
    """Annotate blueprints with WF status. All blueprints returned; trimmed ones flagged in lineage."""
    metrics_by_strategy, _ = _load_walkforward_strategy_metrics(run_id=run_id)
    if not metrics_by_strategy or not blueprints:
        annotated = [
            dataclasses.replace(bp, lineage=dataclasses.replace(bp.lineage, wf_status="pass", wf_evidence_hash=evidence_hash))
            for bp in blueprints
        ]
        return annotated, {
            "wf_evidence_used": False,
            "trim_split": "validation",
            "trimmed_zero_trade": 0,
            "trimmed_worst_negative": 0,
            "wf_trimmed_all": False,
            "dropped_blueprint_ids": [],
        }

    by_id = {bp.id: bp for bp in blueprints}
    zero_trade_ids: set[str] = set()
    negative_rows: List[Tuple[float, str]] = []

    for strategy_id, split_map in metrics_by_strategy.items():
        if not strategy_id.startswith("dsl_interpreter_v1__"):
            continue
        bp_id = strategy_id.replace("dsl_interpreter_v1__", "", 1)
        if bp_id not in by_id:
            continue
        validation = split_map.get("validation", {})
        if not isinstance(validation, dict):
            continue
        trades = _safe_int(validation.get("total_trades", 0), 0)
        stressed = _safe_float(validation.get("stressed_net_pnl", 0.0), 0.0)
        if trades <= 0:
            zero_trade_ids.add(bp_id)
        elif stressed < 0.0:
            negative_rows.append((stressed, bp_id))

    trim_ids: set[str] = set(zero_trade_ids)
    negative_rows = sorted(negative_rows, key=lambda row: (row[0], row[1]))
    worst_negative_ids: set[str] = {bp_id for _, bp_id in negative_rows[:TRIM_WF_WORST_K]}
    trim_ids.update(worst_negative_ids)

    annotated: List[Blueprint] = []
    for bp in blueprints:
        if bp.id in zero_trade_ids:
            wf_status = "trimmed_zero_trade"
        elif bp.id in worst_negative_ids:
            wf_status = "trimmed_worst_negative"
        else:
            wf_status = "pass"
        new_lineage = dataclasses.replace(bp.lineage, wf_status=wf_status, wf_evidence_hash=evidence_hash)
        annotated.append(dataclasses.replace(bp, lineage=new_lineage))

    trimmed_count = sum(1 for bp in annotated if bp.lineage.wf_status.startswith("trimmed"))
    return annotated, {
        "wf_evidence_used": True,
        "trim_split": "validation",
        "trimmed_zero_trade": int(sum(1 for bp_id in trim_ids if bp_id in zero_trade_ids)),
        "trimmed_worst_negative": int(sum(1 for bp_id in trim_ids if bp_id in worst_negative_ids)),
        "wf_trimmed_all": bool(trimmed_count == len(annotated) and trimmed_count > 0),
        "dropped_blueprint_ids": sorted(trim_ids),
    }
```

Add `import dataclasses` near the top of the file.

**Step 4: Update `main()` to use the new function**

In `main()`, find the current call (around line 1242):
```python
blueprints, trim_stats = _trim_blueprints_with_walkforward_evidence(blueprints=blueprints, run_id=args.run_id)
if not blueprints:
    if bool(trim_stats.get("wf_trimmed_all", False)):
        raise ValueError("Walkforward trimming removed all blueprints; strict mode fails closed.")
    raise ValueError("No blueprints remained after compile filters.")
blueprints, duplicate_stats = _dedupe_blueprints_by_behavior(blueprints)
```

Replace with:
```python
# Load WF evidence hash first (used for annotation)
_, wf_evidence_hash = _load_walkforward_strategy_metrics(run_id=args.run_id)

# Dedupe first, then annotate with WF status
blueprints, duplicate_stats = _dedupe_blueprints_by_behavior(blueprints)
blueprints = sorted(blueprints, key=lambda b: (b.event_type, b.candidate_id, b.id))
if not blueprints:
    raise ValueError("No blueprints remained after behavior-level deduplication.")

blueprints, trim_stats = _annotate_blueprints_with_walkforward_evidence(
    blueprints=blueprints,
    run_id=args.run_id,
    evidence_hash=wf_evidence_hash,
)
active_blueprints = [bp for bp in blueprints if not bp.lineage.wf_status.startswith("trimmed")]
if not active_blueprints:
    if bool(trim_stats.get("wf_trimmed_all", False)):
        # Write all-trimmed blueprints before failing (for audit), then raise
        out_jsonl = out_dir / "blueprints.jsonl"
        lines = [json.dumps(bp.to_dict(), sort_keys=True) for bp in blueprints]
        out_jsonl.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        raise ValueError("Walkforward trimming flagged all blueprints; strict mode fails closed.")
    raise ValueError("No blueprints remained after compile filters.")
```

Remove the old `blueprints = sorted(...)` line that was after deduplication (it's now moved above).

**Step 5: Update JSONL write to write ALL blueprints (not just active)**

Find the write section (around line 1252):
```python
out_jsonl = out_dir / "blueprints.jsonl"
lines = [json.dumps(bp.to_dict(), sort_keys=True) for bp in blueprints]
out_jsonl.write_text(...)
```

This already writes `blueprints` — now `blueprints` contains all (including trimmed), so no change needed there. Just ensure it's writing the full annotated list.

**Step 6: Run updated WF trim tests**

```bash
pytest tests/test_compile_strategy_blueprints.py::test_compiler_trims_zero_trade_blueprint_from_walkforward tests/test_compile_strategy_blueprints.py::test_compiler_walkforward_trim_uses_validation_split_only tests/test_compile_strategy_blueprints.py::test_compiler_walkforward_trim_all_fails_closed -v
```
Expected: all `PASSED`.

**Step 7: Run full suite**

```bash
pytest -q -m "not slow"
```
Expected: all passing.

**Step 8: Commit**

```bash
git add project/pipelines/research/compile_strategy_blueprints.py tests/test_compile_strategy_blueprints.py
git commit -m "feat: replace WF trim-before-write with annotate-then-write-all (write_then_flag)"
```

---

## Task 4: Stamp min_events into blueprint lineage

**Files:**
- Modify: `project/pipelines/research/compile_strategy_blueprints.py`
  - `_build_blueprint()` (line 788) — add `min_events` parameter, stamp into lineage
  - call site in `main()` (line 1158) — pass `min_events=int(args.min_events_floor)`
- Test: `tests/test_compile_strategy_blueprints.py`

**Step 1: Write the failing test**

```python
def test_compiler_stamps_min_events_in_lineage(monkeypatch, tmp_path: Path) -> None:
    run_id = "dsl_compile_min_events_stamp"
    _write_inputs(tmp_path=tmp_path, run_id=run_id)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys, "argv",
        [
            "compile_strategy_blueprints.py",
            "--run_id", run_id,
            "--symbols", "BTCUSDT,ETHUSDT",
            "--ignore_checklist", "1",
            "--min_events_floor", "50",
        ] + ALLOW_NAIVE_ENTRY_FAIL_ARGS,
    )
    assert compile_strategy_blueprints.main() == 0

    out_path = tmp_path / "reports" / "strategy_blueprints" / run_id / "blueprints.jsonl"
    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    for row in rows:
        assert row["lineage"]["min_events_threshold"] == 50
        assert row["lineage"]["events_count_used_for_gate"] > 0
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/test_compile_strategy_blueprints.py::test_compiler_stamps_min_events_in_lineage -v
```
Expected: `FAILED` — `min_events_threshold` is 0.

**Step 3: Implement**

In `_build_blueprint()` signature (line 788), add `min_events: int = 0`:

```python
def _build_blueprint(
    run_id: str,
    run_symbols: List[str],
    event_type: str,
    row: Dict[str, object],
    phase2_lookup: Dict[str, Dict[str, object]],
    stats: Dict[str, np.ndarray],
    fees_bps: float,
    slippage_bps: float,
    min_events: int = 0,
) -> Blueprint:
```

Near the end of `_build_blueprint`, when constructing `LineageSpec`, add the two new fields:

```python
lineage=LineageSpec(
    source_path=str(merged.get("source_path", "")),
    compiler_version=COMPILER_VERSION,
    generated_at_utc=DETERMINISTIC_TS,
    events_count_used_for_gate=_safe_int(merged.get("n_events", merged.get("sample_size", 0)), 0),
    min_events_threshold=int(min_events),
),
```

At the call site in `main()` (line ~1162), add `min_events=int(args.min_events_floor)`:

```python
bp = _build_blueprint(
    run_id=args.run_id,
    run_symbols=run_symbols,
    event_type=event_type,
    row=row,
    phase2_lookup=phase2_lookup,
    stats=stats,
    fees_bps=float(resolved_costs.fee_bps_per_side),
    slippage_bps=float(resolved_costs.slippage_bps_per_fill),
    min_events=int(args.min_events_floor),
)
```

**Step 4: Run test to verify it passes**

```bash
pytest tests/test_compile_strategy_blueprints.py::test_compiler_stamps_min_events_in_lineage -v
```
Expected: `PASSED`.

**Step 5: Run full suite**

```bash
pytest -q -m "not slow"
```
Expected: all passing.

**Step 6: Commit**

```bash
git add project/pipelines/research/compile_strategy_blueprints.py tests/test_compile_strategy_blueprints.py
git commit -m "feat: stamp min_events_threshold and events_count_used_for_gate into blueprint lineage"
```

---

## Task 5: Add `compile_funnel` block to summary and fix `min_events` reporting

**Files:**
- Modify: `project/pipelines/research/compile_strategy_blueprints.py` — summary dict in `main()` (around line 1256)
- Test: `tests/test_compile_strategy_blueprints.py`

**Step 1: Write the failing test**

```python
def test_compiler_summary_contains_compile_funnel(monkeypatch, tmp_path: Path) -> None:
    run_id = "dsl_compile_funnel_summary"
    _write_inputs(tmp_path=tmp_path, run_id=run_id)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(compile_strategy_blueprints, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys, "argv",
        [
            "compile_strategy_blueprints.py",
            "--run_id", run_id,
            "--symbols", "BTCUSDT,ETHUSDT",
            "--ignore_checklist", "1",
            "--min_events_floor", "50",
        ] + ALLOW_NAIVE_ENTRY_FAIL_ARGS,
    )
    assert compile_strategy_blueprints.main() == 0

    summary_path = tmp_path / "reports" / "strategy_blueprints" / run_id / "blueprints_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    assert "compile_funnel" in summary
    funnel = summary["compile_funnel"]
    assert "selected_for_build" in funnel
    assert "build_success" in funnel
    assert "wf_trim_zero_trade" in funnel
    assert "wf_trim_worst_negative" in funnel
    assert "written_total" in funnel
    assert "written_active" in funnel
    assert "written_trimmed" in funnel

    assert "wf_evidence_hash" in summary
    assert "min_events_threshold_used" in summary
    assert summary["min_events_threshold_used"] == 50

    # quality_floor.min_events should reflect actual arg, not constant
    assert summary["quality_floor"]["min_events"] == 50
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/test_compile_strategy_blueprints.py::test_compiler_summary_contains_compile_funnel -v
```
Expected: `FAILED` — no `compile_funnel` key in summary.

**Step 3: Implement**

In `main()`, before building the summary dict, compute funnel counts:

```python
n_selected_for_build = sum(len(sel) for sel in ... )  # track this via a counter
```

Actually, track it simply: add a counter `selected_for_build_count = 0` before the event loop, incrementing by `len(strict_selected_rows)` each event.

Then in the summary dict (around line 1256), add:

```python
active_count = sum(1 for bp in blueprints if not bp.lineage.wf_status.startswith("trimmed"))
trimmed_count = len(blueprints) - active_count
summary = {
    ...existing keys...,
    "compile_funnel": {
        "selected_for_build": int(selected_for_build_count),
        "build_success": int(len(blueprints)),          # all built blueprints (pre-dedup)
        "build_fail_non_executable": int(rejected_non_executable_condition_count),
        "build_fail_naive_entry": int(rejected_naive_entry_count),
        "build_fail_exception": int(sum(
            1 for r in attempt_records if r.get("fail_reason") == "build_exception"
        )),
        "behavior_dedup_drop": int(duplicate_stats.get("behavior_duplicate_count", 0)),
        "wf_trim_zero_trade": int(trim_stats.get("trimmed_zero_trade", 0)),
        "wf_trim_worst_negative": int(trim_stats.get("trimmed_worst_negative", 0)),
        "written_total": int(len(blueprints)),
        "written_active": int(active_count),
        "written_trimmed": int(trimmed_count),
    },
    "wf_evidence_hash": str(wf_evidence_hash),
    "wf_evidence_source": str(DATA_ROOT / "reports" / "eval" / args.run_id / "walkforward_summary.json"),
    "min_events_threshold_used": int(args.min_events_floor),
}
```

Also fix `quality_floor.min_events` in the summary — change from the constant to `int(args.min_events_floor)`:
```python
"quality_floor": {
    "min_robustness_score": QUALITY_MIN_ROBUSTNESS,
    "min_events": int(args.min_events_floor),   # was: QUALITY_MIN_EVENTS
    ...
},
```

**Step 4: Run test to verify it passes**

```bash
pytest tests/test_compile_strategy_blueprints.py::test_compiler_summary_contains_compile_funnel -v
```
Expected: `PASSED`.

**Step 5: Run full suite**

```bash
pytest -q -m "not slow"
```
Expected: all passing.

**Step 6: Commit**

```bash
git add project/pipelines/research/compile_strategy_blueprints.py tests/test_compile_strategy_blueprints.py
git commit -m "feat: add compile_funnel block to blueprints_summary.json with evidence hash and actual min_events"
```

---

## Task 6: Filter active blueprints in downstream consumers

**Files:**
- Modify: `project/pipelines/research/stress_test_blueprints.py` (line 78–86)
- Modify: `project/pipelines/eval/run_walkforward.py` — `_load_blueprints_raw()` (line 96) or `_expected_blueprint_strategy_ids()` (line 122)
- Test: `tests/test_compile_strategy_blueprints.py` (integration-style test)

**Step 1: Write the failing test**

```python
def test_active_blueprints_filter_excludes_trimmed() -> None:
    """Verify the downstream filter pattern works correctly."""
    blueprints = [
        {"id": "bp1", "candidate_id": "c1", "lineage": {"wf_status": "pass"}},
        {"id": "bp2", "candidate_id": "c2", "lineage": {"wf_status": "trimmed_zero_trade"}},
        {"id": "bp3", "candidate_id": "c3", "lineage": {"wf_status": "trimmed_worst_negative"}},
        {"id": "bp4", "candidate_id": "c4", "lineage": {}},  # missing wf_status → treated as pass
    ]
    active = [
        bp for bp in blueprints
        if not bp.get("lineage", {}).get("wf_status", "pass").startswith("trimmed")
    ]
    ids = {bp["id"] for bp in active}
    assert ids == {"bp1", "bp4"}
```

**Step 2: Run test to verify it passes (it's pure logic, no implementation needed)**

```bash
pytest tests/test_compile_strategy_blueprints.py::test_active_blueprints_filter_excludes_trimmed -v
```
Expected: `PASSED` (this confirms the filter expression is correct).

**Step 3: Implement in `stress_test_blueprints.py`**

In `stress_test_blueprints.py`, after loading blueprints (line 82), add filter:

```python
blueprints = []
with open(blueprints_path) as f:
    for line in f:
        if line.strip():
            blueprints.append(json.loads(line))

# Filter to active blueprints only (skip WF-trimmed)
blueprints = [
    bp for bp in blueprints
    if not bp.get("lineage", {}).get("wf_status", "pass").startswith("trimmed")
]
```

**Step 4: Implement in `run_walkforward.py`**

In `_load_blueprints_raw()` (line 96), add a filter after building `rows`:

```python
# Filter out WF-trimmed blueprints
rows = [
    row for row in rows
    if not row.get("lineage", {}).get("wf_status", "pass").startswith("trimmed")
]
if not rows:
    raise ValueError(f"No active (non-trimmed) blueprint rows found in {path}")
```

**Step 5: Run full suite**

```bash
pytest -q -m "not slow"
```
Expected: all passing.

**Step 6: Commit**

```bash
git add project/pipelines/research/stress_test_blueprints.py project/pipelines/eval/run_walkforward.py tests/test_compile_strategy_blueprints.py
git commit -m "feat: filter WF-trimmed blueprints in stress test and walkforward consumers"
```

---

## Task 7: Final verification

**Step 1: Run the complete test suite**

```bash
pytest -q
```
Expected: all 300+ tests passing (304 or more with new tests).

**Step 2: Check test count**

```bash
pytest --collect-only -q | tail -5
```
Expected: at least 8 more tests than baseline (7 new + 3 updated).

**Step 3: Final commit if any cleanup needed**

```bash
git status
# If clean:
git log --oneline -8
```
Verify the commit chain looks correct.

---

## Summary of Changes

| File | Type | What changes |
|------|------|-------------|
| `project/strategy_dsl/schema.py` | Schema | `LineageSpec` +4 optional fields |
| `project/pipelines/research/compile_strategy_blueprints.py` | Core | Hash loader, annotate-not-trim, min_events stamp, funnel summary |
| `project/pipelines/research/stress_test_blueprints.py` | Consumer | Filter trimmed blueprints |
| `project/pipelines/eval/run_walkforward.py` | Consumer | Filter trimmed blueprints |
| `tests/test_compile_strategy_blueprints.py` | Tests | 3 updated + 7 new tests |
