# Repository Guidelines

## Project Structure & Module Organization
- `project/pipelines/` contains runnable pipeline stages: `ingest/`, `clean/`, `features/`, `research/`, `backtest/`, `report/`, and `alpha_bundle/`.
- `project/features/` contains reusable feature/event logic (for example liquidity vacuum definitions).
- `project/engine/` and `project/strategies/` contain execution/backtest runtime components.
- `tests/` contains pytest coverage for orchestration and pipeline contracts.
- Runtime artifacts are written under `data/` (especially `data/runs/`, `data/reports/`, `data/lake/`, `data/feature_store/`).

## Build, Test, and Development Commands
- Setup:
```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
```
- Run canonical discovery:
```bash
./.venv/bin/python project/pipelines/run_all.py --run_id RUN --symbols BTCUSDT,ETHUSDT --start 2020-01-01 --end 2025-12-31 --run_phase2_conditional 1 --phase2_event_type all
```
- Make targets:
  - `make run` (ingest + clean + features + context)
  - `make discover-edges` (phase1 + phase2 + edge export)
  - `make discover-hybrid` (discover + expectancy checks)
  - `make test` (pytest)

## Coding Style & Naming Conventions
- Python style: PEP 8, 4-space indentation, explicit typing for new/modified public helpers.
- Keep pipeline scripts deterministic and file-path explicit (`Path`, no hidden cwd assumptions).
- Use descriptive stage names and snake_case IDs (`phase2_conditional_hypotheses`, `liquidity_vacuum`).
- Preserve existing data contracts (`data/reports/<stage>/<run_id>/...`) when extending stages.

## Testing Guidelines
- Framework: `pytest` with tests in `tests/test_*.py`.
- Add or update tests for orchestration changes (`run_all.py`), schema/IO contract changes, and new ingest/feature logic.
- Run locally before PR:
```bash
./.venv/bin/pytest -q
```

## Commit & Pull Request Guidelines
- Preferred commit format: `feat: ...`, `fix: ...`, `chore: ...`, `refactor: ...`.
- Keep commits focused (pipeline logic, tests, docs together only when tightly coupled).
- PRs should include:
  - change summary and affected stages,
  - exact run/test commands executed,
  - sample output paths (for example `data/reports/phase2/<run_id>/...`),
  - notes on data assumptions (symbols, date range, market: `perp`/`spot`).

## Security & Configuration Tips
- Set `BACKTEST_DATA_ROOT` to a controlled local path; avoid committing runtime artifacts.
- Treat API/network ingestion as non-deterministic input; persist manifests/logs for reproducibility.

## Plan Milestone Map (Engineering Backlog)
- **Milestone A (R2 + R3):** Implement walk-forward split labels, OOS-only promotion gating, and multiplicity-adjusted significance in phase2 outputs.
- **Milestone B (R4):** Enforce source/schema provenance completeness across clean/features manifests.
- **Milestone C (R6 + R11):** Add funding/borrow PnL decomposition and historical universe snapshots for survivorship-bias-safe backtests.
- **Milestone D (R5 + R8):** Integrate OI/liquidation/revision-lag features and add stability/capacity robustness diagnostics with checklist gates.
- **Milestone E (R10):** Add MEV-aware execution overlay and microstructure strategy template routing/tests.
