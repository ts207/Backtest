# Backtest

## Standardized setup (recommended)
- Python 3.10+
- Create a local virtualenv:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt -r requirements-dev.txt
```

- Validate the environment:

```bash
python -m pytest -q
```

## Dependency compatibility policy
- CI/local validation target is `python3 -m pytest -q` and should remain green before/after dependency upgrades.
- For pandas upgrades, warning-sensitive paths are guarded by `tests/test_warning_hardening.py`.
- If a dependency bump introduces deprecation/future warnings in touched modules, treat that as a blocking issue and harden the path before merging.

## Data root
By default, all data, runs, and reports live under `data/` at the repo root.
To change this location, set `BACKTEST_DATA_ROOT` to an absolute path before running.

```bash
export BACKTEST_DATA_ROOT=/path/to/backtest-data
```

## Data strategy
- `data/` is local-first and intentionally ignored in git.
- Keep code/config/manifests in git; keep large market data outside version control.
- For archival/sharing, publish compressed run snapshots externally (object storage/NAS) and preserve `run_id`.
- See `docs/data_strategy.md` for the full policy.

## End-to-end smoke run
Use this short run to validate ingest -> clean -> features -> backtest -> report:

```bash
python project/pipelines/run_all.py \
  --run_id 20260210_000001 \
  --symbols BTCUSDT \
  --start 2020-06-01 \
  --end 2020-06-07
```

## One-command pipeline run
Run the full pipeline from the repo root:

```bash
python3 project/pipelines/run_all.py --symbols BTCUSDT,ETHUSDT --start 2020-06-01 --end 2025-07-10
```

PowerShell example:

```powershell
python project\pipelines\run_all.py --symbols BTCUSDT,ETHUSDT --start 2020-06-01 --end 2025-07-10
```

The orchestrator auto-generates `run_id` (format `YYYYMMDD_HHMMSS`) unless you provide one via `--run_id`.

## Multi-edge portfolio run (Top 10 universe, recommended constraints)
Run baseline + multi-edge allocator + validation in one command:

```bash
python3 project/pipelines/run_all.py \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10 \
  --run_edge_candidate_universe 1 \
  --run_multi_edge_portfolio 1 \
  --run_multi_edge_validation 1 \
  --multi_edge_symbols TOP10 \
  --multi_edge_modes equal_risk,score_weighted,constrained_optimizer
```

Key multi-edge outputs:
- `data/lake/trades/backtests/multi_edge_portfolio/<run_id>/metrics.json`
- `data/reports/multi_edge_portfolio/<run_id>/summary.md`
- `data/reports/multi_edge_validation/<run_id>/verdict.json`

## Run stages individually
```bash
# 1) Ingest 15m OHLCV
python3 project/pipelines/ingest/ingest_binance_um_ohlcv_15m.py --run_id 20260101_120000 --symbols BTCUSDT,ETHUSDT --start 2020-06-01 --end 2020-06-01

# 2) Ingest funding rates
python3 project/pipelines/ingest/ingest_binance_um_funding.py --run_id 20260101_120000 --symbols BTCUSDT,ETHUSDT --start 2020-06-01 --end 2025-06-01

# 3) Build cleaned canonical 15m bars + aligned funding
python3 project/pipelines/clean/build_cleaned_15m.py --run_id 20260101_120000 --symbols BTCUSDT,ETHUSDT --start 2020-06-01 --end 2025-06-01

# 4) Build features v1
python3 project/pipelines/features/build_features_v1.py --run_id 20260101_120000 --symbols BTCUSDT,ETHUSDT

# 5) Run the backtest
python3 project/pipelines/backtest/backtest_vol_compression_v1.py --run_id 20260101_120000 --symbols BTCUSDT,ETHUSDT

# 6) Generate report
python3 project/pipelines/report/make_report.py --run_id 20260101_120000
```

## Research lifecycle (Phase 1 -> Phase 2)
Phase 1 verifies structural event truth (event-level, de-overlapped). Phase 2 tests conditional edge hypotheses with hard simplicity caps before any strategy optimization.

### Phase 1 (vol shock -> relaxation)
```bash
python3 project/pipelines/research/analyze_vol_shock_relaxation.py \
  --run_id 20260101_120000 \
  --symbols BTCUSDT,ETHUSDT \
  --timeframe 15m
```

### Phase 2 (Conditional Edge Hypothesis)
```bash
python3 project/pipelines/research/phase2_conditional_hypotheses.py \
  --run_id 20260101_120000 \
  --event_type vol_shock_relaxation \
  --symbols BTCUSDT,ETHUSDT \
  --max_conditions 20 \
  --max_actions 9
```

Phase 2 gates:
- Gate A: CI separation on adverse-risk reduction
- Gate B: no sign flip over time (>= 80% yearly sign stability)
- Gate C: sign stability across symbol/vol splits within condition
- Gate D: friction floor plausibility
- Gate E: simplicity cap (condition/action hard caps)

Phase 2 outputs:
- `data/reports/phase2/<run_id>/<event_type>/phase2_candidates.csv`
- `data/reports/phase2/<run_id>/<event_type>/phase2_summary.md`
- `data/reports/phase2/<run_id>/<event_type>/phase2_manifests.json`
- `data/reports/phase2/<run_id>/<event_type>/promoted_candidates.json` (max 1-2 candidates)

### Return-maximization objective and constraints
When the research objective is **maximize return**, the repository uses a constrained objective:

- Primary target metric is pinned per APPROVED overlay spec (`objective.target_metric`) and should normally be `net_total_return` after fees, slippage, funding, and overlay costs.
- Hard constraints are required for APPROVED overlays and must include:
  - `constraints.max_drawdown_pct`
  - `constraints.tail_loss`
  - `constraints.exposure_limits`
  - `constraints.turnover_budget`
- Evidence records for APPROVED overlays must be structured objects per run with:
  - `run_id`, `split`, `date_range`, `universe`, `config_hash`
- Stability requirements are mandatory and falsifiable for APPROVED overlays. Each spec must include:
  - `stability.sign_consistency_min`
  - `stability.effect_ci_excludes_0`
  - `stability.max_regime_flip_count`

This enforces: maximize constrained net return, not unconstrained PnL on noise.

### Phase gating under return objective
- **Phase 1**: structure-only mechanism discovery (no action tuning loops).
- **Phase 2**: first PnL-allowed stage for a small fixed set of falsifiable actions with pre-registered costs and constraints.
- **Phase 3**: limited parameter selection with walk-forward / nested-CV only after Phase 2 pass, then lock.

### Overlay promotion criteria
- `DRAFT` specs are listable but non-applicable (`apply_overlay` blocks non-APPROVED statuses).
- `APPROVED` specs must carry validated cost, evidence run IDs, objective definition, constraint stats, and stability declarations.



### Overlay promotion checker
Run the promotion contract checker to evaluate a pinned overlay spec against referenced run artifacts:

```bash
python3 project/pipelines/research/check_overlay_promotion.py   --overlay vol_shock_relaxation_delay30_v1
```

This emits:
- `data/reports/overlay_promotion/<overlay>/promotion_verdict.json`
- `data/reports/overlay_promotion/<overlay>/promotion_verdict.md`

Verdicts are `PASS_PROMOTION` or `FAIL_PROMOTION` with machine-readable reasons.

### Optional one-command run with Phase 2
```bash
python3 project/pipelines/run_all.py \
  --symbols BTCUSDT,ETHUSDT \
  --start 2020-06-01 \
  --end 2025-07-10 \
  --run_phase2_conditional 1
```

## Output locations
- Raw data: `data/lake/raw/binance/perp/<symbol>/...`
- Cleaned bars (run-scoped): `data/lake/runs/<run_id>/cleaned/perp/<symbol>/bars_15m/...`
- Aligned funding (run-scoped): `data/lake/runs/<run_id>/cleaned/perp/<symbol>/funding_15m/...`
- Features (run-scoped): `data/lake/runs/<run_id>/features/perp/<symbol>/15m/features_v1/...`
- Context features (run-scoped): `data/lake/runs/<run_id>/context/funding_persistence/<symbol>/15m.parquet`
- Backtest outputs: `data/lake/trades/backtests/vol_compression_expansion_v1/<run_id>/...`
- Reports: `data/reports/vol_compression_expansion_v1/<run_id>/summary.md`
- Manifests/logs: `data/runs/<run_id>/<stage>.json` and `.log`

## Optional: run-centric report view
Generate a cleaner report tree grouped by `run_id` under `data/reports/by_run`:

```bash
python3 project/pipelines/report/organize_reports.py --mode symlink
```

Use `--mode copy` if your environment does not support symlinks.

## Sanity gates and funding handling
- Funding is treated as discrete 8h events. Cleaned funding stores `funding_event_ts` and `funding_rate_scaled` aligned to each 15m bar.
- Missing funding fails the clean and features stages unless `--allow_missing_funding=1` is provided.
- Constant funding within a month (std == 0 after scaling) fails the clean stage unless `--allow_constant_funding=1`.
- Funding timestamps must be on-the-hour; `--allow_funding_timestamp_rounding=1` will round to the nearest hour and record counts in manifests.
- Sanity checks enforce UTC, monotonic timestamps, and funding bounds (abs <= 1% per 8h).

## Data availability and gaps
- USD-M futures archives do not exist before late 2019. Requests earlier than that are clamped to the first available date and recorded in manifests as `requested_start` vs `effective_start`.
- Missing archive files are recorded in manifests and do not fail the run.
- Funding gaps are recorded in manifests; missing funding does not get silently filled.
