# Directional Exhaustion After Forced Flow (Phase 1)

Run ID: `btc_edge_raw_20260213`
Window: [0, 32]
Anchor mode: taker_imbalance
Anchor quantile: 0.99

- Events: 105
- Controls: 105
- Actions generated: 0 (Phase 1 structure only)

## Matched deltas (head)
           event_id  symbol  year  delta_realized_vol_mean  delta_range_expansion  delta_tail_move_probability  delta_tail_move_within
deff_BTCUSDT_000769 BTCUSDT  2020                -0.000009               0.010268                    -0.030303                       0
deff_BTCUSDT_000938 BTCUSDT  2020                 0.000299               0.024406                     0.060606                       1
deff_BTCUSDT_005320 BTCUSDT  2020                 0.000156               0.019489                     0.000000                       0
deff_BTCUSDT_005605 BTCUSDT  2020                -0.000050               0.008716                     0.000000                       0
deff_BTCUSDT_013724 BTCUSDT  2020                -0.000229              -0.010678                    -0.242424                       0
deff_BTCUSDT_014039 BTCUSDT  2020                 0.000055               0.023491                     0.030303                       1
deff_BTCUSDT_015503 BTCUSDT  2020                 0.000377              -0.006651                     0.030303                       0
deff_BTCUSDT_017532 BTCUSDT  2020                 0.000757               0.024622                     0.242424                       0
deff_BTCUSDT_017697 BTCUSDT  2020                -0.001361              -0.070613                    -0.030303                       0
deff_BTCUSDT_020500 BTCUSDT  2020                -0.000015               0.001122                     0.090909                       0
deff_BTCUSDT_020930 BTCUSDT  2021                 0.001083              -0.026961                     0.030303                       0
deff_BTCUSDT_021080 BTCUSDT  2021                 0.000132               0.000989                     0.000000                       0

## Hazards (head)
 age  at_risk  hits  hazard cohort
   1      105   105     1.0 events
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
BTCUSDT 105                 0.000177               0.011869                     0.047619

## Sign stability
 symbol  year  n  sign_tail_prob  sign_vol
BTCUSDT  2020 10             1.0      -1.0
BTCUSDT  2021 39             1.0       1.0
BTCUSDT  2022 22             1.0       1.0
BTCUSDT  2023 13             1.0       1.0
BTCUSDT  2024 15             1.0       1.0
BTCUSDT  2025  6             1.0       1.0