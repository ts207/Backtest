# Repository Analysis Audit (2026-02-07)

## Scope
This audit reviewed repository layout, test health, and maintainability signals using a full unit-test run and static inspection of top-level project structure.

## Project snapshot
- Primary code under `project/` with clear subdomains:
  - `pipelines/` for orchestration and ETL/research jobs
  - `engine/` for simulation and PnL logic
  - `strategies/` for strategy and overlay contracts
  - `features/` and `analyzers/` for feature engineering and research analysis
- Strong test coverage footprint under `tests/` including:
  - engine execution and diagnostics
  - ingestion and persistence behavior
  - research/analyzer mechanics and contracts

## Validation result
- Test command: `python3 -m pytest -q`
- Outcome: **71 passed, 0 failed**
- Interpretation: Core behavior is currently stable across included regression tests.

## Warning audit
The test run surfaced deprecation/future warnings that should be prioritized before dependency upgrades:

1. `project/pipelines/_lib/validation.py`
   - Uses `pd.api.types.is_datetime64tz_dtype`, which is deprecated.
   - Risk: Future pandas versions may break timezone validation paths.

2. `project/engine/runner.py`
   - Multiple `fillna(...).astype(...)` paths emit downcasting future warnings.
   - Risk: Silent dtype coercion behavior changes can alter signal columns or mask subtle bugs.

3. `project/pipelines/ingest/ingest_binance_um_funding.py`
   - Concat with empty/all-NA entries emits future warning.
   - Risk: dtype inference changes may affect persisted funding schema consistency.

## Audit assessment
- **Reliability:** Good (green tests).
- **Architecture clarity:** Good (modular folders and explicit pipeline stages).
- **Forward compatibility:** Moderate risk due to pandas deprecations.
- **Immediate production risk:** Low.

## Recommended next actions
1. Replace deprecated datetime dtype checks with pandas-supported dtype classes.
2. Make dtype intent explicit in `runner.py` before/after `fillna` to avoid silent behavior changes.
3. Normalize/guard empty DataFrames before funding concat operations.
4. Add a CI mode that fails on deprecation warnings for changed files (or at least logs trend counts).

## Suggested acceptance criteria for follow-up cleanup PR
- All current tests still pass.
- Warning count reduced from 45 to near-zero (excluding third-party noise).
- No schema drift in generated manifests/CSV/Parquet outputs.
