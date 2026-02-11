# Liquidity Absence Window (Phase 1)

Run ID: `20260211_031950`
Window: [0, 96]
Absence quantile: 0.2

- Events: 739
- Controls: 739
- Actions generated: 0 (Phase 1 structure only)

## Matched deltas (head)
          event_id  symbol  year  anchor_hour  delta_realized_vol_mean  delta_range_expansion  delta_tail_move_probability  delta_tail_move_within
law_BTCUSDT_000576 BTCUSDT  2020            0                -0.000520               0.014949                     0.010309                       0
law_BTCUSDT_000896 BTCUSDT  2020            8                 0.000454               0.015750                     0.020619                       0
law_BTCUSDT_001216 BTCUSDT  2020           16                -0.000027              -0.010749                     0.000000                       0
law_BTCUSDT_001344 BTCUSDT  2020            0                 0.000407               0.032893                     0.041237                       0
law_BTCUSDT_001600 BTCUSDT  2020           16                 0.000206              -0.004312                     0.000000                       0
law_BTCUSDT_001728 BTCUSDT  2020            0                -0.000542              -0.008870                    -0.020619                      -1
law_BTCUSDT_001856 BTCUSDT  2020            8                -0.000084               0.008860                    -0.010309                      -1
law_BTCUSDT_001984 BTCUSDT  2020           16                -0.000402              -0.003502                    -0.010309                      -1
law_BTCUSDT_002112 BTCUSDT  2020            0                -0.000731              -0.017477                    -0.020619                      -1
law_BTCUSDT_002240 BTCUSDT  2020            8                 0.001012               0.039050                     0.030928                       0
law_BTCUSDT_002560 BTCUSDT  2020           16                 0.000818               0.009668                     0.041237                       0
law_BTCUSDT_002784 BTCUSDT  2020            0                -0.000694              -0.016363                    -0.020619                      -1

## Hazards (head)
 age  at_risk  hits   hazard cohort
   1      739     1 0.001353 events
   2      738    11 0.014905 events
   3      727     3 0.004127 events
   4      724     3 0.004144 events
   5      721     6 0.008322 events
   6      715     9 0.012587 events
   7      706     7 0.009915 events
   8      699     6 0.008584 events
   9      693     8 0.011544 events
  10      685     4 0.005839 events
  11      681     5 0.007342 events
  12      676     6 0.008876 events
  13      670     3 0.004478 events
  14      667     5 0.007496 events
  15      662     3 0.004532 events
  16      659     7 0.010622 events
  17      652     2 0.003067 events
  18      650     2 0.003077 events
  19      648     5 0.007716 events
  20      643     3 0.004666 events

## Phase stability
 anchor_hour   n  delta_realized_vol_mean  delta_range_expansion  delta_tail_move_probability  delta_tail_move_within
           0 272                -0.000760              -0.011181                    -0.022438               -0.466912
           8 213                 0.000174               0.011413                     0.005082               -0.028169
          16 254                -0.000569              -0.008327                    -0.010837               -0.551181

## Sign stability
 year  anchor_hour   n  sign_tail_prob  sign_vol
 2020            0  26            -1.0      -1.0
 2020            8  31             1.0       1.0
 2020           16  29            -1.0      -1.0
 2021            0   3             1.0       1.0
 2022            0  38            -1.0      -1.0
 2022            8  29             1.0       1.0
 2022           16  33            -1.0      -1.0
 2023            0 113            -1.0      -1.0
 2023            8  99             1.0       1.0
 2023           16 105            -1.0      -1.0
 2024            0  58            -1.0      -1.0
 2024            8  31             1.0       1.0
 2024           16  60            -1.0      -1.0
 2025            0  34            -1.0      -1.0
 2025            8  23             1.0       1.0
 2025           16  27            -1.0      -1.0

## Interpretation
Preliminary Phase-1 read: liquidity-absence windows show lower tail-move incidence than matched controls.