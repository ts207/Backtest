# Full Project Audit — Deep Report

## Scope

This audit covers repository structure, code quality posture, testing health, runtime ergonomics, documentation organization, and operational risks for the current `main` worktree state.

## Audit methodology

Executed checks:

- `bash scripts/audit_repo.sh`
- `pytest`
- repository inventory commands (`rg --files`, marker scans for `TODO|FIXME|XXX|HACK`)

All tests passed and compile checks completed successfully in this environment.

## Current repository health snapshot

- **Test suite status:** 89/89 passing.
- **Compilation status:** Python modules compile successfully.
- **Codebase shape:**
  - 95 Python files across `project/` + `tests/`
  - 29 test files
  - 9 docs files in `docs/`
  - 3 edge specification JSON files
- **Debt markers (`TODO/FIXME/XXX/HACK`):** none detected.

## Architecture audit

### Strengths

1. **Clear staged pipeline design**
   - The pipeline separation (`ingest` → `clean` → `features` → `backtest` → `report` → `research`) is explicit and supports incremental execution.
2. **Run-scoped data model**
   - Run isolation in `data/lake/runs/<run_id>/...` improves reproducibility and post-mortem analysis.
3. **Research governance model present**
   - Phase-gated research flow and promotion checks reduce ad-hoc optimization risk.
4. **Reasonable test breadth**
   - Tests cover engine behavior, analyzers, overlays, reporting, and orchestrator behavior.

### Friction points

1. **Operational entry points spread across locations**
   - Both root and `project/` Makefiles exist; this can drift over time if targets diverge.
2. **Long-lived narrative reports mixed with specifications**
   - While now indexed, docs still combine evergreen contracts and time-bound analyses in the same directory.
3. **Implicit workflow knowledge**
   - Many advanced research scripts are discoverable only by browsing `project/pipelines/research` rather than task-oriented command groups.

## Code quality and reliability audit

### Strengths

- Validation contracts are explicit for key data assumptions (timestamps, required columns, partition completeness).
- Funding ingestion now defensively handles empty-source concatenation edge cases.
- Context normalization in the engine runner is resilient to mixed/object-typed values.

### Residual risks

1. **Schema evolution pressure**
   - Feature/context schemas are consumed across multiple stages; schema drift still requires careful coordination.
2. **Config sprawl risk**
   - Governance constraints and strategy parameters spread across YAML, JSON edges, and script arguments can become inconsistent if not centrally validated.
3. **Data-size scaling risk**
   - Large symbol/date expansions may expose performance bottlenecks in pandas-heavy paths without chunking/partition planning.

## Test and QA audit

### What is good

- Fast local feedback loop (`~12s` for full suite in this environment).
- Good coverage of deterministic behavior and contract tests.

### Gaps to prioritize

1. **No explicit performance regression checks**
   - Add lightweight timing/memory guardrails for critical pipeline stages.
2. **No smoke command matrix in CI-style script**
   - A minimal command matrix (ingest/clean/features/backtest/report with tiny fixture date ranges) would catch integration breakages early.
3. **No strict warning budget**
   - Consider promoting selected warnings to errors in tests for forward-compatibility guarantees.

## Documentation and workflow audit

### Improvements already in place

- Root `README.md` has been simplified and made task-first.
- `docs/README.md` now provides a documentation index.
- `scripts/audit_repo.sh` standardizes quality checks.

### Additional recommendations

1. **Adopt doc taxonomy in filenames**
   - Prefix long-form analyses with `report_` and contracts with `spec_` for consistent sorting.
2. **Add `docs/CHANGELOG.md` for research contract updates**
   - Track interpretation-lock and edge-spec changes with rationale.
3. **Introduce command cookbook**
   - A `docs/operations.md` with copy-paste recipes for common tasks (re-run failed run, audit promoted edge, compare two run_ids).

## Priority action plan

### P0 (immediate)

- Keep using `scripts/audit_repo.sh` as pre-PR gate.
- Prevent Makefile drift by aligning root and `project/` Make targets.

### P1 (near-term)

- Add a fixture-based end-to-end smoke script for tiny date ranges.
- Add schema consistency tests for cross-stage artifacts (context/features/reports).

### P2 (medium-term)

- Add stage-level timing benchmarks and alert thresholds.
- Add centralized config validator for YAML + edge JSON + CLI argument coherence.

## Conclusion

The project is in a **healthy and maintainable** state with strong modularity, reliable tests, and improved developer ergonomics. The highest-value next step is to formalize integration smoke checks and performance guardrails so growth in symbols/features does not degrade reliability or iteration speed.
