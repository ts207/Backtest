# Backtest

## One-command pipeline run
Run the full pipeline from the repo root (Windows PowerShell examples shown):

```powershell
python project\pipelines\run_all.py --symbols BTCUSDT,ETHUSDT --start 2023-06-01 --end 2023-07-10 --allow_funding_timestamp_rounding=1
```

The orchestrator auto-generates `run_id` (format `YYYYMMDD_HHMMSS`) unless you provide one via `--run_id`.

## Run stages individually
```powershell
# 1) Ingest 15m OHLCV
python project\pipelines\ingest\ingest_binance_um_ohlcv_15m.py --run_id 20240101_120000 --symbols BTCUSDT,ETHUSDT --start 2024-06-01 --end 2025-06-01

# 2) Ingest funding rates
python project\pipelines\ingest\ingest_binance_um_funding.py --run_id 20240101_120000 --symbols BTCUSDT,ETHUSDT --start 2024-06-01 --end 2025-06-01

# 3) Build cleaned canonical 15m bars + aligned funding
python project\pipelines\clean\build_cleaned_15m.py --run_id 20240101_120000 --symbols BTCUSDT,ETHUSDT --start 2024-06-01 --end 2025-06-01

# 4) Build features v1
python project\pipelines\features\build_features_v1.py --run_id 20240101_120000 --symbols BTCUSDT,ETHUSDT

# 5) Run the backtest
python project\pipelines\backtest\backtest_vol_compression_v1.py --run_id 20240101_120000 --symbols BTCUSDT,ETHUSDT

# 6) Generate report
python project\pipelines\report\make_report.py --run_id 20240101_120000
```

## Output locations
- Raw data: `project\lake\raw\binance\perp\<symbol>\...`
- Cleaned bars: `project\lake\cleaned\perp\<symbol>\bars_15m\...`
- Aligned funding: `project\lake\cleaned\perp\<symbol>\funding_15m\...`
- Features: `project\lake\features\perp\<symbol>\15m\features_v1\...`
- Backtest outputs: `project\lake\trades\backtests\vol_compression_expansion_v1\<run_id>\...`
- Reports: `project\reports\vol_compression_expansion_v1\<run_id>\summary.md`
- Manifests/logs: `project\runs\<run_id>\<stage>.json` and `.log`

## Sanity gates & funding handling
- Funding is treated as discrete 8h events. Cleaned funding stores `funding_event_ts` and `funding_rate_scaled` aligned to each 15m bar.
- Missing funding fails the clean/features stages unless `--allow_missing_funding=1` is provided.
- Constant funding within a month (std == 0 after scaling) fails the clean stage unless `--allow_constant_funding=1`.
- Funding timestamps must be on-the-hour; `--allow_funding_timestamp_rounding=1` will round to the nearest hour and record counts in manifests.
- Sanity checks enforce UTC, monotonic timestamps and funding bounds (abs <= 1% per 8h).

## Data availability & gaps
- USD-M futures archives do not exist before late 2019. Requests earlier than that are clamped to the first
  available date and recorded in manifests as `requested_start` vs `effective_start`.
- Missing archive files are recorded in manifests and do **not** fail the run.
- Funding gaps are recorded in manifests; missing funding does not get silently filled.
