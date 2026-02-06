# Backtest

## Setup
- Python 3.10+
- Install dependencies: `python3 -m pip install -r requirements.txt`
- Optional: create a virtual environment in `.venv` before installing

## Data root
By default, all data, runs, and reports live under `data/` at the repo root.
To change this location, set `BACKTEST_DATA_ROOT` to an absolute path before running.

```bash
export BACKTEST_DATA_ROOT=/path/to/backtest-data
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
- Cleaned bars: `data/lake/cleaned/perp/<symbol>/bars_15m/...`
- Aligned funding: `data/lake/cleaned/perp/<symbol>/funding_15m/...`
- Features: `data/lake/features/perp/<symbol>/15m/features_v1/...`
- Backtest outputs: `data/lake/trades/backtests/vol_compression_expansion_v1/<run_id>/...`
- Reports: `data/reports/vol_compression_expansion_v1/<run_id>/summary.md`
- Manifests/logs: `data/runs/<run_id>/<stage>.json` and `.log`

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
