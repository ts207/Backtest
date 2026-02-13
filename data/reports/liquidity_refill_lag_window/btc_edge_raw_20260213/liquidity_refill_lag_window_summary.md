# Liquidity Refill / Requote Lag Window (Phase 1)

Run ID: `btc_edge_raw_20260213`
Window: [0, 48]
Impulse quantile: 0.99
Refill horizon: 8
Refill threshold multiplier: 1.25

- Events: 211
- Controls: 211
- Actions generated: 0 (Phase 1 structure only)

## Matched deltas (head)
          event_id  symbol  year  delta_realized_vol_mean  delta_range_expansion  delta_tail_move_probability  delta_tail_move_within
lrl_BTCUSDT_000092 BTCUSDT  2020                 0.001612               0.017340                    -0.040816                       0
lrl_BTCUSDT_000155 BTCUSDT  2020                -0.000253               0.013476                    -0.040816                       0
lrl_BTCUSDT_000938 BTCUSDT  2020                 0.000053               0.018225                     0.040816                       0
lrl_BTCUSDT_001027 BTCUSDT  2020                 0.000755               0.001911                    -0.061224                       0
lrl_BTCUSDT_003036 BTCUSDT  2020                 0.000641               0.012924                     0.020408                       1
lrl_BTCUSDT_005463 BTCUSDT  2020                 0.001481               0.027530                     0.102041                       0
lrl_BTCUSDT_009092 BTCUSDT  2020                 0.001596               0.057440                     0.040816                       0
lrl_BTCUSDT_009178 BTCUSDT  2020                 0.000321               0.031285                     0.122449                       0
lrl_BTCUSDT_011775 BTCUSDT  2020                 0.000575               0.020045                     0.061224                       1
lrl_BTCUSDT_015164 BTCUSDT  2020                 0.001514               0.019727                     0.244898                       1
lrl_BTCUSDT_015503 BTCUSDT  2020                 0.001149              -0.017178                     0.102041                       0
lrl_BTCUSDT_017102 BTCUSDT  2020                 0.001679               0.053008                     0.346939                       0

## Hazards (head)
 age  at_risk  hits  hazard cohort
   1        0     0     NaN events
   2        0     0     NaN events
   3        0     0     NaN events
   4        0     0     NaN events
   5        0     0     NaN events
   6        0     0     NaN events
   7        0     0     NaN events
   8        0     0     NaN events
   9        0     0     NaN events
  10        0     0     NaN events
  11        0     0     NaN events
  12        0     0     NaN events
  13        0     0     NaN events
  14        0     0     NaN events
  15        0     0     NaN events
  16        0     0     NaN events
  17        0     0     NaN events
  18        0     0     NaN events
  19        0     0     NaN events
  20        0     0     NaN events

## Phase stability
 symbol   n  delta_realized_vol_mean  delta_range_expansion  delta_tail_move_probability
BTCUSDT 211                 0.001011               0.033905                     0.107651

## Sign stability
 symbol  year  n  sign_tail_prob  sign_vol
BTCUSDT  2020 20             1.0       1.0
BTCUSDT  2021 44             1.0       1.0
BTCUSDT  2022 51             1.0       1.0
BTCUSDT  2023 39             1.0       1.0
BTCUSDT  2024 40             1.0       1.0
BTCUSDT  2025 17             1.0       1.0