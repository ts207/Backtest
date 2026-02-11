# Liquidity Refill / Requote Lag Window (Phase 1)

Run ID: `20260211_031950`
Window: [0, 48]
Impulse quantile: 0.99
Refill horizon: 8
Refill threshold multiplier: 1.25

- Events: 387
- Controls: 387
- Actions generated: 0 (Phase 1 structure only)

## Matched deltas (head)
          event_id  symbol  year  delta_realized_vol_mean  delta_range_expansion  delta_tail_move_probability  delta_tail_move_within
lrl_BTCUSDT_000092 BTCUSDT  2020                 0.002760               0.051377                     0.020408                       0
lrl_BTCUSDT_000155 BTCUSDT  2020                 0.000475               0.039208                    -0.163265                       0
lrl_BTCUSDT_000938 BTCUSDT  2020                 0.000138               0.005858                     0.040816                       0
lrl_BTCUSDT_001027 BTCUSDT  2020                 0.000575               0.000071                     0.020408                       0
lrl_BTCUSDT_003036 BTCUSDT  2020                 0.000280               0.011831                     0.020408                       1
lrl_BTCUSDT_005463 BTCUSDT  2020                 0.001772               0.052631                     0.163265                       0
lrl_BTCUSDT_009092 BTCUSDT  2020                 0.001711               0.055626                     0.061224                       1
lrl_BTCUSDT_009178 BTCUSDT  2020                 0.001345               0.043373                     0.040816                       0
lrl_BTCUSDT_011775 BTCUSDT  2020                 0.000418               0.007396                     0.020408                       0
lrl_BTCUSDT_015164 BTCUSDT  2020                 0.001457               0.019756                     0.244898                       1
lrl_BTCUSDT_015503 BTCUSDT  2020                 0.001646               0.035219                     0.142857                       0
lrl_BTCUSDT_017102 BTCUSDT  2020                 0.001545               0.073750                     0.408163                       0

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
BTCUSDT 210                 0.001021               0.032013                     0.105345
ETHUSDT 177                 0.001375               0.037593                     0.103990

## Sign stability
 symbol  year  n  sign_tail_prob  sign_vol
BTCUSDT  2020 20             1.0       1.0
BTCUSDT  2021 44             1.0       1.0
BTCUSDT  2022 51             1.0       1.0
BTCUSDT  2023 39             1.0       1.0
BTCUSDT  2024 39             1.0       1.0
BTCUSDT  2025 17             1.0       1.0
ETHUSDT  2020 19             1.0       1.0
ETHUSDT  2021 38             1.0       1.0
ETHUSDT  2022 41             1.0       1.0
ETHUSDT  2023 19            -1.0       1.0
ETHUSDT  2024 30             1.0       1.0
ETHUSDT  2025 30             1.0       1.0