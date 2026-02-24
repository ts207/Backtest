# AGENTS.md

## Purpose
This repository is a spec-first quantitative research and backtesting platform for crypto markets. Agents should optimize for reproducibility, PIT correctness, and auditability over quick hacks.

## Environment
- Use repo-local Python: `.venv/bin/python`.
- Set `BACKTEST_DATA_ROOT` for all pipeline runs:
  - `export BACKTEST_DATA_ROOT=$(pwd)/data`
- Install dependencies with:
  - `.venv/bin/pip install -r requirements.txt -r requirements-dev.txt`

## First Read
- `README.md`
- `docs/GETTING_STARTED.md`
- `docs/CONCEPTS.md`
- `docs/ARCHITECTURE.md`
- `docs/SPEC_FIRST.md`

## Core Layout
- `project/pipelines/run_all.py`: master orchestrator.
- `project/pipelines/`: ingest, clean, features, research, backtest, eval, report stages.
- `project/events/registry.py`: dynamic event registry from `spec/events/*.yaml`.
- `project/strategy_dsl/`: blueprint schema + DSL contracts.
- `spec/`: source of truth (`concepts`, `features`, `events`, `states`, `gates`, `multiplicity`).
- `tests/`: unit/integration checks across pipelines, events, eval, specs.

## Non-Negotiable Invariants
- Spec-first: trading/event logic belongs in `spec/` and must stay code-aligned.
- PIT integrity: never introduce look-ahead joins or future leakage.
- Reproducibility: preserve run-manifest integrity (`spec_hashes`, `data_hash`, `git_commit`, config digest).
- Evaluation guard: fallback blueprint paths are blocked from measurement in `run_all.py`; do not bypass.
- Checklist gate is fail-closed for execution when decision is `KEEP_RESEARCH`.
- Keep timeframe assumptions coherent (default data lake is `5m` unless explicitly extended end-to-end).

## Standard Commands
- Fast tests: `make test-fast`
- Full tests: `make test`
- Hygiene checks: `make check-hygiene`
- Compile check: `make compile`
- Discovery run: `make discover-edges`
- Baseline run: `make baseline STRATEGIES=<comma-separated>`

## Validation Before Claiming Completion
Run this minimum set unless task scope clearly excludes it:
1. `make check-hygiene`
2. `make test-fast`
3. Targeted tests for touched modules (examples):
   - `tests/events/test_registry_loader.py`
   - `tests/pipelines/research/test_atlas_plan_expansion.py`
   - `tests/pipelines/report/test_performance_attribution_report.py`

## File-Touch Guidance
- Event type changes:
  - Update `spec/events/<event>.yaml`.
  - Ensure registry compatibility (`event_type`, `reports_dir`, `events_file`, `signal_column`).
  - Verify with `tests/events/test_registry_loader.py`.
- Phase2 / discovery changes:
  - Keep multiplicity and gating semantics aligned with `spec/gates.yaml` and `spec/multiplicity/*`.
  - Preserve artifact contracts under `data/reports/<stage>/<run_id>/...`.
- Orchestrator changes (`run_all.py`):
  - Preserve CLI compatibility and stage ordering contracts.
  - Keep run-manifest fields stable; avoid silent schema drift.
- Data/cleanup changes:
  - Respect `project/scripts/check_repo_hygiene.sh` blocked tracked paths.
  - Do not commit generated runtime artifacts from `data/runs`, `data/reports`, `data/lake/*` (except tracked `.gitkeep`).

## CI Expectations
- GitHub `pytest` workflow uses Python 3.12 and runs:
  1. `bash project/scripts/check_repo_hygiene.sh`
  2. `python -m pytest -q`
- Keep local verification close to CI behavior before opening PRs.

## Practical Notes
- System `python3` may miss parquet engine deps in this environment; use `.venv/bin/python`.
- Repository may be a dirty working tree during tasks; never revert unrelated user changes.
