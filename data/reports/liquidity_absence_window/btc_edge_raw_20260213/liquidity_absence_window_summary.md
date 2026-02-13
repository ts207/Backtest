# Liquidity Absence Window (Phase 1)

Run ID: `btc_edge_raw_20260213`
Window: [0, 96]
Absence quantile: 0.2

- Events: 378
- Controls: 378
- Actions generated: 0 (Phase 1 structure only)

## Matched deltas (head)
          event_id  symbol  year  anchor_hour  delta_realized_vol_mean  delta_range_expansion  delta_tail_move_probability  delta_tail_move_within
law_BTCUSDT_000576 BTCUSDT  2020            0                -0.000050               0.011027                     0.020619                       0
law_BTCUSDT_000896 BTCUSDT  2020            8                 0.000302               0.013792                     0.030928                       1
law_BTCUSDT_001216 BTCUSDT  2020           16                -0.001509              -0.083209                    -0.092784                       0
law_BTCUSDT_001344 BTCUSDT  2020            0                 0.000877               0.028970                     0.051546                       0
law_BTCUSDT_001600 BTCUSDT  2020           16                -0.001276              -0.076772                    -0.092784                       0
law_BTCUSDT_001728 BTCUSDT  2020            0                -0.000072              -0.012792                    -0.010309                      -1
law_BTCUSDT_001856 BTCUSDT  2020            8                -0.000235               0.006902                     0.000000                       0
law_BTCUSDT_001984 BTCUSDT  2020           16                -0.001883              -0.075962                    -0.103093                      -1
law_BTCUSDT_002112 BTCUSDT  2020            0                -0.000261              -0.021400                    -0.010309                      -1
law_BTCUSDT_002240 BTCUSDT  2020            8                 0.000861               0.037092                     0.041237                       1
law_BTCUSDT_002560 BTCUSDT  2020           16                -0.000663              -0.062792                    -0.051546                       0
law_BTCUSDT_002784 BTCUSDT  2020            0                -0.000224              -0.020286                    -0.010309                      -1

## Hazards (head)
 age  at_risk  hits   hazard cohort
   1      378     1 0.002646 events
   2      377     6 0.015915 events
   3      371     1 0.002695 events
   4      370     1 0.002703 events
   5      369     4 0.010840 events
   6      365     1 0.002740 events
   7      364     3 0.008242 events
   8      361     3 0.008310 events
   9      358     3 0.008380 events
  10      355     2 0.005634 events
  11      353     3 0.008499 events
  12      350     2 0.005714 events
  13      348     1 0.002874 events
  14      347     4 0.011527 events
  15      343     3 0.008746 events
  16      340     4 0.011765 events
  17      336     1 0.002976 events
  18      335     0 0.000000 events
  19      335     2 0.005970 events
  20      333     2 0.006006 events

## Phase stability
 anchor_hour   n  delta_realized_vol_mean  delta_range_expansion  delta_tail_move_probability  delta_tail_move_within
           0 140                -0.000245              -0.004468                     0.003314               -0.464286
           8 102                -0.000070               0.007346                     0.010309                0.431373
          16 136                -0.001510              -0.076310                    -0.092784               -0.522059

## Sign stability
 year  anchor_hour  n  sign_tail_prob  sign_vol
 2020            0 16             1.0      -1.0
 2020            8 18             1.0      -1.0
 2020           16 20            -1.0      -1.0
 2022            0 21             1.0      -1.0
 2022            8 12             1.0      -1.0
 2022           16 19            -1.0      -1.0
 2023            0 50             1.0      -1.0
 2023            8 43             1.0      -1.0
 2023           16 48            -1.0      -1.0
 2024            0 29             1.0      -1.0
 2024            8 10             1.0       1.0
 2024           16 25            -1.0      -1.0
 2025            0 24            -1.0      -1.0
 2025            8 19             1.0      -1.0
 2025           16 24            -1.0      -1.0

## Interpretation
Preliminary Phase-1 read: liquidity-absence windows show lower tail-move incidence than matched controls.