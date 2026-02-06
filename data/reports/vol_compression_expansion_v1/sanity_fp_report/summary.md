# Volatility Compression → Expansion Report

Run ID: `sanity_fp_report`

## Summary Metrics
- Total trades (combined): 0
- Win rate (combined): 0.00%
- Avg R (combined): 0.00
- Ending equity (combined): 1,000,000.00
- Max drawdown (combined): 0.00%
- Sharpe (annualized): 0.00

## Trades by Symbol

 index  symbol  total_trades  avg_r
     0 BTCUSDT             0    0.0
     1 ETHUSDT             0    0.0

## Fee Sensitivity

 fee_bps_per_side  net_return  avg_r  max_drawdown  trades
              0.0         0.0    0.0           0.0       0
              2.0         0.0    0.0           0.0       0
              5.0         0.0    0.0           0.0       0
              6.0         0.0    0.0           0.0       0
             10.0         0.0    0.0           0.0       0

## Data Quality

### Missing OHLCV (%) by month
- **BTCUSDT**
  - 2021-01: 0.00%
- **ETHUSDT**
  - 2021-01: 0.00%

### Bad bars by month
- **BTCUSDT**
  - 2021-01: 0
- **ETHUSDT**
  - 2021-01: 0

### Funding coverage (%) by month
- **BTCUSDT**
  - 2021-01: 100.00%
- **ETHUSDT**
  - 2021-01: 100.00%

### Funding missing (%) by month
- **BTCUSDT**
  - 2021-01: 0.00%
- **ETHUSDT**
  - 2021-01: 0.00%

### Funding diagnostics
- **BTCUSDT**
  - % bars with funding_event_ts: 100.00%
  - funding_rate_scaled min/max/std: 0.000100 / 0.002365 / 0.000540
- **ETHUSDT**
  - % bars with funding_event_ts: 100.00%
  - funding_rate_scaled min/max/std: 0.000100 / 0.003750 / 0.000850

## Feature Diagnostics

- **BTCUSDT**
  - pct NaN ret_1: 0.07%
  - pct NaN logret_1: 0.07%
  - pct NaN rv_96: 6.67%
  - pct NaN rv_pct_2880: 100.00%
  - pct NaN range_med_480: 39.86%
  - segments count/min/median/max: 1 / 1440 / 1440.0 / 1440
  - pct rows dropped: 100.00%
- **ETHUSDT**
  - pct NaN ret_1: 0.07%
  - pct NaN logret_1: 0.07%
  - pct NaN rv_96: 6.67%
  - pct NaN rv_pct_2880: 100.00%
  - pct NaN range_med_480: 39.86%
  - segments count/min/median/max: 1 / 1440 / 1440.0 / 1440
  - pct rows dropped: 100.00%

## Context Segmentation (Funding Persistence)

### By fp_active
 fp_active  bars  total_pnl  mean_pnl
         0  2035        0.0       0.0
         1   845        0.0       0.0

### By fp_age_bars bucket
fp_age_bucket  bars  total_pnl  mean_pnl
     inactive  2035        0.0       0.0
          0-8   120        0.0       0.0
         9-30   315        0.0       0.0
        31-96   378        0.0       0.0
          >96    32        0.0       0.0

## Engine Diagnostics

- **vol_compression_v1**
  - nan_return_bars: 2 (0.07%)
  - forced_flat_bars: 0 (0.00%)
  - missing_feature_bars: 2880 (100.00%)