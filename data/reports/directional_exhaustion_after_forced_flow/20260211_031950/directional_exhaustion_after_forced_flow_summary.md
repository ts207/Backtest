# Directional Exhaustion After Forced Flow (Phase 1)

Run ID: `20260211_031950`
Window: [0, 32]
Anchor mode: taker_imbalance
Anchor quantile: 0.99

- Events: 211
- Controls: 211
- Actions generated: 0 (Phase 1 structure only)

## Matched deltas (head)
           event_id  symbol  year  delta_realized_vol_mean  delta_range_expansion  delta_tail_move_probability  delta_tail_move_within
deff_BTCUSDT_000769 BTCUSDT  2020                 0.000308               0.014506                     0.030303                       1
deff_BTCUSDT_000938 BTCUSDT  2020                 0.000151              -0.017131                     0.000000                       0
deff_BTCUSDT_005320 BTCUSDT  2020                 0.000552               0.038521                     0.030303                       1
deff_BTCUSDT_013724 BTCUSDT  2020                -0.000229              -0.010678                    -0.242424                       0
deff_BTCUSDT_014039 BTCUSDT  2020                 0.000055               0.023491                     0.030303                       1
deff_BTCUSDT_015503 BTCUSDT  2020                 0.000377              -0.006651                     0.030303                       0
deff_BTCUSDT_017532 BTCUSDT  2020                -0.004342              -0.182992                    -0.151515                       0
deff_BTCUSDT_017697 BTCUSDT  2020                -0.000319               0.005912                     0.030303                       0
deff_BTCUSDT_020500 BTCUSDT  2020                 0.000434              -0.006513                     0.000000                       0
deff_BTCUSDT_020930 BTCUSDT  2021                 0.000229               0.026192                     0.181818                       0
deff_BTCUSDT_021080 BTCUSDT  2021                 0.000510               0.028578                     0.090909                       0
deff_BTCUSDT_021630 BTCUSDT  2021                -0.005078              -0.025387                    -0.090909                       0

## Hazards (head)
 age  at_risk  hits  hazard cohort
   1      211   211     1.0 events
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
BTCUSDT 103                 0.000105               0.010355                     0.036776
ETHUSDT 108                 0.000246               0.014121                     0.030864

## Sign stability
 symbol  year  n  sign_tail_prob  sign_vol
BTCUSDT  2020  9            -1.0      -1.0
BTCUSDT  2021 39             1.0       1.0
BTCUSDT  2022 22             1.0       1.0
BTCUSDT  2023 12             1.0       1.0
BTCUSDT  2024 15            -1.0      -1.0
BTCUSDT  2025  6             1.0       1.0
ETHUSDT  2020 12             1.0       1.0
ETHUSDT  2021 22             1.0       1.0
ETHUSDT  2022 29             1.0       1.0
ETHUSDT  2023 19             1.0       1.0
ETHUSDT  2024 18             1.0       1.0
ETHUSDT  2025  8             1.0       1.0