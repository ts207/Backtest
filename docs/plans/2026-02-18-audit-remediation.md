# RUN_2022_2023 Audit Remediation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix the three code-level audit findings from the RUN_2022_2023 audit: overlay candidates flowing into the builder, non-production override flags not recorded in the run manifest, and the missing `ended_at` field.

**Architecture:** Three isolated fixes — one in `build_strategy_candidates.py` (overlay gate), one in `run_all.py` (override recording + `ended_at`). Each fix has its own failing test first; the existing test fixtures are reused to keep setup minimal.

**Tech Stack:** Python 3.x, pytest, pandas, pathlib; no new dependencies.

---

## Context for Implementer

The project is a crypto backtesting pipeline. `run_all.py` orchestrates pipeline stages as subprocesses and writes a top-level `run_manifest.json`. `build_strategy_candidates.py` reads an edge candidate CSV and builds strategy objects to backtest. Overlay candidates (`candidate_type='overlay'`) are meant to modify a base strategy at execution time — they must never enter the builder as standalone entries.

**Test conventions:**
- All test files live in `tests/`, named `test_*.py`
- Tests import project code via `sys.path.insert(0, str(ROOT / "project"))` where `ROOT = Path(__file__).resolve().parents[1]`
- `BACKTEST_DATA_ROOT` env var is set via `monkeypatch.setenv` or direct env dict
- Existing similar test: `tests/test_build_strategy_candidates.py` (see `_write_edge_inputs` helper)
- Existing manifest test: `tests/test_manifest_provenance.py` (see monkeypatch pattern for `run_all.main()`)

---

## Task 1: Overlay Candidate Filter — Failing Test

**Files:**
- Create: `tests/test_overlay_candidate_filter.py`

**Step 1: Write the failing test**

```python
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import build_strategy_candidates


def _write_mixed_edge_inputs(tmp_path: Path, run_id: str) -> None:
    """Write edge CSV with both 'edge' and 'overlay' candidate_type rows."""
    event = "vol_shock_relaxation"
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    phase2_dir = tmp_path / "reports" / "phase2" / run_id / event
    edge_dir.mkdir(parents=True, exist_ok=True)
    phase2_dir.mkdir(parents=True, exist_ok=True)

    source_json = phase2_dir / "promoted_candidates.json"
    source_json.write_text(
        json.dumps({
            "run_id": run_id,
            "event_type": event,
            "candidates": [
                {
                    "candidate_id": f"{event}_edge",
                    "condition": "all",
                    "action": "delay_30",
                    "sample_size": 120,
                    "gate_oos_consistency_strict": True,
                    "gate_bridge_tradable": True,
                },
                {
                    "candidate_id": f"{event}_overlay",
                    "condition": "all",
                    "action": "risk_throttle_half",
                    "sample_size": 80,
                    "gate_oos_consistency_strict": True,
                    "gate_bridge_tradable": True,
                },
            ],
        }),
        encoding="utf-8",
    )

    pd.DataFrame([
        {
            "run_id": run_id,
            "event": event,
            "candidate_id": f"{event}_edge",
            "candidate_type": "edge",
            "overlay_base_candidate_id": "",
            "status": "PROMOTED",
            "edge_score": 0.55,
            "expected_return_proxy": 0.01,
            "expectancy_per_trade": 0.01,
            "stability_proxy": 0.9,
            "robustness_score": 0.9,
            "event_frequency": 0.5,
            "capacity_proxy": 1.0,
            "profit_density_score": 0.005,
            "n_events": 120,
            "source_path": str(source_json),
            "gate_oos_consistency_strict": True,
            "gate_bridge_tradable": True,
        },
        {
            "run_id": run_id,
            "event": event,
            "candidate_id": f"{event}_overlay",
            "candidate_type": "overlay",
            "overlay_base_candidate_id": f"{event}_edge",
            "status": "PROMOTED",
            "edge_score": 0.60,
            "expected_return_proxy": 0.02,
            "expectancy_per_trade": 0.02,
            "stability_proxy": 0.9,
            "robustness_score": 0.9,
            "event_frequency": 0.5,
            "capacity_proxy": 1.0,
            "profit_density_score": 0.006,
            "n_events": 80,
            "source_path": str(source_json),
            "gate_oos_consistency_strict": True,
            "gate_bridge_tradable": True,
        },
    ]).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)


def test_overlay_candidates_excluded_from_builder(tmp_path, monkeypatch):
    """Overlay candidates must never appear in strategy_candidates.json."""
    run_id = "overlay_filter_test"
    _write_mixed_edge_inputs(tmp_path, run_id)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(
        sys, "argv",
        [
            "build_strategy_candidates.py",
            "--run_id", run_id,
            "--symbols", "BTCUSDT",
            "--allow_non_promoted", "1",
            "--ignore_checklist", "1",
        ],
    )

    build_strategy_candidates.main()

    out = tmp_path / "reports" / "strategy_candidates" / run_id / "strategy_candidates.json"
    assert out.exists(), "strategy_candidates.json not written"
    candidates = json.loads(out.read_text(encoding="utf-8"))

    candidate_ids = [c.get("candidate_id", c.get("strategy_candidate_id", "")) for c in candidates]
    assert any("_edge" in cid for cid in candidate_ids), "edge candidate should be included"
    assert not any("_overlay" in cid for cid in candidate_ids), (
        f"overlay candidate must not appear in output; found: {candidate_ids}"
    )


def test_overlay_skipped_count_in_diagnostics(tmp_path, monkeypatch):
    """deployment_manifest.json builder_diagnostics must track skipped overlay count."""
    run_id = "overlay_diag_test"
    _write_mixed_edge_inputs(tmp_path, run_id)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(
        sys, "argv",
        [
            "build_strategy_candidates.py",
            "--run_id", run_id,
            "--symbols", "BTCUSDT",
            "--allow_non_promoted", "1",
            "--ignore_checklist", "1",
        ],
    )

    build_strategy_candidates.main()

    deploy = tmp_path / "reports" / "strategy_candidates" / run_id / "deployment_manifest.json"
    diag = json.loads(deploy.read_text(encoding="utf-8"))["builder_diagnostics"]
    assert diag.get("skipped_overlay_count", -1) >= 1, (
        "builder_diagnostics.skipped_overlay_count must be >= 1"
    )
```

**Step 2: Run test to verify it fails**

```
pytest tests/test_overlay_candidate_filter.py -v
```

Expected: FAIL — overlay candidate appears in output (no filter exists yet) and `skipped_overlay_count` key is absent.

**Step 3: Commit the failing test**

```bash
git add tests/test_overlay_candidate_filter.py
git commit -m "test: failing tests for overlay candidate filter"
```

---

## Task 2: Overlay Candidate Filter — Implementation

**Files:**
- Modify: `project/pipelines/research/build_strategy_candidates.py:1063-1090` (inner loop), `:1145-1153` (diagnostics dict), `:1180-1192` (finalize_manifest stats)

**Step 1: Add `skipped_overlay_count` counter near the other skip counters**

Find the initialization of `skipped_strict_gate_count` (search for `skipped_strict_gate_count = 0`) and add the new counter on the next line:

```python
skipped_overlay_count = 0
```

**Step 2: Add overlay guard in the inner candidate loop**

Locate the block starting at line ~1077 (after `if not detail: ... continue`) and add the overlay check immediately after loading `detail` but before the gate checks:

```python
                    candidate_type = str(row.get("candidate_type", "edge")).strip().lower()
                    if candidate_type == "overlay":
                        skipped_overlay_count += 1
                        continue
```

Full context after the edit (lines ~1066–1091):

```python
                for _, row in selected.iterrows():
                    source_path = Path(str(row.get("source_path", "")))
                    detail = _load_candidate_detail(source_path=source_path, candidate_id=str(row.get("candidate_id", "")))
                    if not detail:
                        missing_detail_records.append(...)
                        continue
                    candidate_type = str(row.get("candidate_type", "edge")).strip().lower()
                    if candidate_type == "overlay":
                        skipped_overlay_count += 1
                        continue
                    gate_oos_consistency_strict = _as_bool(...)
                    if not gate_oos_consistency_strict:
                        skipped_strict_gate_count += 1
                        continue
                    gate_bridge_tradable = _as_bool(...)
                    if not gate_bridge_tradable:
                        skipped_strict_gate_count += 1
                        continue
                    strategy_rows.append(_build_edge_strategy_candidate(...))
```

**Step 3: Add `skipped_overlay_count` to `builder_diagnostics` dict**

In the `deployment_manifest` dict (around line 1145), add the new key:

```python
            "builder_diagnostics": {
                "missing_candidate_detail_count": skipped_missing_detail_count,
                "skipped_missing_candidate_detail_count": skipped_missing_detail_count if int(args.allow_missing_candidate_detail) else 0,
                "skipped_overlay_count": int(skipped_overlay_count),    # ← add this
                "source_counts_seen": source_counts_seen,
                "source_counts_selected": source_counts_selected,
                "skipped_strict_gate_count": int(skipped_strict_gate_count),
            },
```

**Step 4: Add `skipped_overlay_count` to `finalize_manifest` stats**

In the `finalize_manifest(...)` call's `stats=` dict, add:

```python
                "skipped_overlay_count": int(skipped_overlay_count),
```

**Step 5: Run tests to verify they pass**

```
pytest tests/test_overlay_candidate_filter.py -v
```

Expected: PASS for both tests.

**Step 6: Run full fast suite to check for regressions**

```
pytest -q -m "not slow"
```

Expected: no new failures.

**Step 7: Commit**

```bash
git add project/pipelines/research/build_strategy_candidates.py
git commit -m "fix: filter overlay candidates from standalone strategy builder"
```

---

## Task 3: Non-Production Override Recording — Failing Tests

**Files:**
- Create: `tests/test_non_prod_override_recording.py`

**Step 1: Write the failing tests**

```python
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))


def _run_all_with_flags(monkeypatch, tmp_path, extra_argv):
    """Run run_all.main() with all stages mocked out, returning the written manifest."""
    from pipelines import run_all

    monkeypatch.setattr(run_all, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(run_all, "_run_stage", lambda stage, script_path, base_args, run_id: True)

    base_argv = [
        "run_all.py",
        "--run_id", "override_test",
        "--symbols", "BTCUSDT",
        "--start", "2024-01-01",
        "--end", "2024-01-02",
    ]
    monkeypatch.setattr(sys, "argv", base_argv + extra_argv)

    rc = run_all.main()
    assert rc == 0
    manifest_path = tmp_path / "runs" / "override_test" / "run_manifest.json"
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def test_no_overrides_when_all_defaults(monkeypatch, tmp_path):
    """With default flags, non_production_overrides must be empty."""
    payload = _run_all_with_flags(monkeypatch, tmp_path, [])
    assert payload["non_production_overrides"] == []


def test_allow_fallback_blueprints_recorded(monkeypatch, tmp_path):
    """--strategy_blueprint_allow_fallback 1 must appear in non_production_overrides."""
    payload = _run_all_with_flags(
        monkeypatch, tmp_path,
        ["--strategy_blueprint_allow_fallback", "1"],
    )
    overrides = payload["non_production_overrides"]
    assert any("allow_fallback_blueprints" in o for o in overrides), (
        f"Expected allow_fallback_blueprints in overrides: {overrides}"
    )


def test_allow_unexpected_strategy_files_recorded(monkeypatch, tmp_path):
    """--walkforward_allow_unexpected_strategy_files 1 must appear in non_production_overrides."""
    payload = _run_all_with_flags(
        monkeypatch, tmp_path,
        ["--walkforward_allow_unexpected_strategy_files", "1"],
    )
    overrides = payload["non_production_overrides"]
    assert any("allow_unexpected_strategy_files" in o for o in overrides), (
        f"Expected allow_unexpected_strategy_files in overrides: {overrides}"
    )


def test_promotion_allow_fallback_evidence_recorded(monkeypatch, tmp_path):
    """--promotion_allow_fallback_evidence 1 must appear in non_production_overrides."""
    payload = _run_all_with_flags(
        monkeypatch, tmp_path,
        ["--promotion_allow_fallback_evidence", "1"],
    )
    overrides = payload["non_production_overrides"]
    assert any("allow_fallback_evidence" in o for o in overrides), (
        f"Expected allow_fallback_evidence in overrides: {overrides}"
    )


def test_allow_naive_entry_fail_recorded(monkeypatch, tmp_path):
    """--strategy_blueprint_allow_naive_entry_fail 1 must appear in non_production_overrides."""
    payload = _run_all_with_flags(
        monkeypatch, tmp_path,
        ["--strategy_blueprint_allow_naive_entry_fail", "1"],
    )
    overrides = payload["non_production_overrides"]
    assert any("allow_naive_entry_fail" in o for o in overrides), (
        f"Expected allow_naive_entry_fail in overrides: {overrides}"
    )
```

**Step 2: Run tests to verify they fail**

```
pytest tests/test_non_prod_override_recording.py -v
```

Expected: `test_no_overrides_when_all_defaults` passes (vacuously), the others FAIL because the flags are never recorded.

**Step 3: Commit the failing tests**

```bash
git add tests/test_non_prod_override_recording.py
git commit -m "test: failing tests for non-prod override recording in run manifest"
```

---

## Task 4: Non-Production Override Recording — Implementation

**Files:**
- Modify: `project/pipelines/run_all.py:1075` (after `non_production_overrides: List[str] = []`)

**Step 1: Add startup-time flag scan after `non_production_overrides` is initialized**

Locate line 1075 (`non_production_overrides: List[str] = []`) and insert the following block immediately after it:

```python
    # Record non-production bypass flags that were passed explicitly at startup.
    # This ensures the manifest audit trail captures every gate override, not just
    # those injected by auto-continue logic.
    _STARTUP_NON_PROD_FLAGS: List[tuple] = [
        # (argparse_attr, stage_name, cli_flag_name)
        ("strategy_blueprint_allow_fallback", "compile_strategy_blueprints", "--allow_fallback_blueprints"),
        ("strategy_blueprint_allow_naive_entry_fail", "compile_strategy_blueprints", "--allow_naive_entry_fail"),
        ("strategy_blueprint_allow_non_executable_conditions", "compile_strategy_blueprints", "--allow_non_executable_conditions"),
        ("strategy_builder_allow_non_promoted", "build_strategy_candidates", "--allow_non_promoted"),
        ("walkforward_allow_unexpected_strategy_files", "run_walkforward", "--allow_unexpected_strategy_files"),
        ("promotion_allow_fallback_evidence", "promote_blueprints", "--allow_fallback_evidence"),
        ("report_allow_backtest_artifact_fallback", "make_report", "--allow_backtest_artifact_fallback"),
    ]
    for attr, stage_name, cli_flag in _STARTUP_NON_PROD_FLAGS:
        if bool(int(getattr(args, attr, 0))):
            non_production_overrides.append(f"{stage_name}:{cli_flag}=1")
```

**Important:** Check that each `attr` matches the actual argparse destination name in `run_all.py`. Search for `add_argument` calls near the flag names (e.g. `--strategy_blueprint_allow_fallback`). The `dest` is the underscored version of the long flag name with leading `--` stripped. If a flag doesn't exist on `args`, `getattr(args, attr, 0)` returns `0` safely.

**Step 2: Run tests**

```
pytest tests/test_non_prod_override_recording.py -v
```

Expected: all 5 tests PASS.

**Step 3: Run full fast suite**

```
pytest -q -m "not slow"
```

Expected: no new failures.

**Step 4: Commit**

```bash
git add project/pipelines/run_all.py
git commit -m "fix: record non-production bypass flags in run_manifest non_production_overrides"
```

---

## Task 5: Add `ended_at` to Run Manifest — Failing Test

**Files:**
- Modify: `tests/test_manifest_provenance.py` (add assertion to existing `test_run_all_writes_run_level_manifest_with_provenance`)

**Step 1: Extend existing test to assert `ended_at` is present and non-null**

In `test_manifest_provenance.py`, find `test_run_all_writes_run_level_manifest_with_provenance` (around line 125). After the existing assertions on `status == "success"`, add:

```python
    assert "ended_at" in payload, "run_manifest must contain ended_at field"
    assert payload["ended_at"] is not None, "ended_at must be populated on success"
    # Sanity-check it's ISO format with timezone
    assert "T" in str(payload["ended_at"]) and "+" in str(payload["ended_at"]) or "Z" in str(payload["ended_at"]), (
        f"ended_at should be ISO 8601 with timezone: {payload['ended_at']}"
    )
```

**Step 2: Run the test to verify it fails**

```
pytest tests/test_manifest_provenance.py::test_run_all_writes_run_level_manifest_with_provenance -v
```

Expected: FAIL — `ended_at` not in payload.

**Step 3: Commit the updated test**

```bash
git add tests/test_manifest_provenance.py
git commit -m "test: assert ended_at field in run_manifest on success"
```

---

## Task 6: Add `ended_at` to Run Manifest — Implementation

**Files:**
- Modify: `project/pipelines/run_all.py:1047-1068` (manifest init), `:1083-1088` (failure path), `:1132-1146` (blocked path), `:1164-1174` (success path)

**Step 1: Add `ended_at: None` to the initial manifest dict**

In the `run_manifest = { ... }` block (starting at line 1047), add after `"finished_at": None,`:

```python
        "ended_at": None,
```

**Step 2: Set `ended_at` on the failure path**

Find the failure block around line 1083 (`run_manifest["finished_at"] = _utc_now_iso()`). Add immediately after:

```python
            run_manifest["ended_at"] = run_manifest["finished_at"]
```

**Step 3: Set `ended_at` on the checklist-blocked path**

Find the blocked path around line 1132 (`run_manifest["finished_at"] = _utc_now_iso()`). Add:

```python
                    run_manifest["ended_at"] = run_manifest["finished_at"]
```

**Step 4: Set `ended_at` on the success path**

Find line 1166 (`run_manifest["finished_at"] = _utc_now_iso()`). Add immediately after:

```python
    run_manifest["ended_at"] = run_manifest["finished_at"]
```

**Step 5: Run the test**

```
pytest tests/test_manifest_provenance.py::test_run_all_writes_run_level_manifest_with_provenance -v
```

Expected: PASS.

**Step 6: Run full fast suite**

```
pytest -q -m "not slow"
```

Expected: no new failures.

**Step 7: Commit**

```bash
git add project/pipelines/run_all.py
git commit -m "fix: add ended_at field to run_manifest for completion tracking"
```

---

## Task 7: Verify All Tests Pass and Update Audit Report

**Step 1: Run complete test suite**

```
pytest -q
```

Expected: all tests pass. If any fail, fix them before proceeding.

**Step 2: Update `docs/AUDIT_REPORT.md` — mark findings resolved**

In `docs/AUDIT_REPORT.md`, update the three findings to reflect their status:
- High: overlay candidates — FIXED in `build_strategy_candidates.py`
- High: non-prod override tracking — FIXED in `run_all.py`
- Medium: `ended_at` missing — FIXED in `run_all.py`

Add a "Resolved" section at the top or inline with each finding.

**Step 3: Commit**

```bash
git add docs/AUDIT_REPORT.md
git commit -m "docs: mark audit findings resolved after code fixes"
```

---

## Task 8: Freeze-Run Re-validation (Manual Step)

After all code fixes are merged to `main`, re-execute the frozen canonical run to confirm no overlays flow through to promotions and the manifest is complete.

**Command:**

```bash
./.venv/bin/python project/pipelines/run_all.py \
  --run_id RUN_2022_2023 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2021-01-01 --end 2022-12-31 \
  --run_phase2_conditional 1 \
  --phase2_event_type all \
  --run_backtest 1 \
  --run_walkforward_eval 1 \
  --run_make_report 1 \
  --force 1 \
  --clean_engine_artifacts 1 \
  --walkforward_allow_unexpected_strategy_files 0
```

**Verify:**

```bash
python -c "
import json
m = json.load(open('data/runs/RUN_2022_2023/run_manifest.json'))
print('status:', m['status'])
print('ended_at:', m['ended_at'])
print('non_production_overrides:', m['non_production_overrides'])
"
```

Expected:
- `status == "success"`
- `ended_at` is non-null ISO string
- `non_production_overrides` is empty list (no bypass flags were used)

```bash
python -c "
import json
c = json.load(open('data/reports/strategy_candidates/RUN_2022_2023/deployment_manifest.json'))
print('skipped_overlay_count:', c['builder_diagnostics'].get('skipped_overlay_count', 'MISSING'))
"
```

Expected: `skipped_overlay_count` is present (value may be 0 if no overlays reached the builder after upstream fixes — that's fine, the field must exist).

**Note:** This step requires `BACKTEST_DATA_ROOT` to be set and the data lake to be populated. It is a long-running operation (~2h). Confirm results manually before closing the audit.
