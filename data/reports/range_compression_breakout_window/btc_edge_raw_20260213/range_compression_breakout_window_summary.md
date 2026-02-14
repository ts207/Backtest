# Range Compression Breakout Window (Phase 1)

Run ID: `btc_edge_raw_20260213`
Window: [0, 96]
Compression quantile: 0.2

- Events: 682
- Controls: 682
- Actions generated: 0 (Phase 1 structure only)

## Matched deltas (head)
           event_id  symbol  year  anchor_hour  delta_realized_vol_mean  delta_range_expansion  delta_tail_move_probability  delta_tail_move_within
rcbw_BTCUSDT_002990 BTCUSDT  2020            3                 0.000360              -0.015246                    -0.010309                       0
rcbw_BTCUSDT_003139 BTCUSDT  2020           16                -0.000120              -0.030404                    -0.010309                      -1
rcbw_BTCUSDT_003236 BTCUSDT  2020           17                 0.000366               0.008046                     0.010309                       1
rcbw_BTCUSDT_003506 BTCUSDT  2020           12                -0.001707              -0.051721                    -0.051546                      -1
rcbw_BTCUSDT_003603 BTCUSDT  2020           12                 0.000820               0.009133                     0.010309                       1
rcbw_BTCUSDT_003702 BTCUSDT  2020           13                 0.000079               0.017870                     0.000000                       0
rcbw_BTCUSDT_003804 BTCUSDT  2020           15                 0.000418               0.008170                     0.000000                       0
rcbw_BTCUSDT_003901 BTCUSDT  2020           15                 0.000180               0.008354                     0.000000                       0
rcbw_BTCUSDT_004269 BTCUSDT  2020           11                -0.000591              -0.003808                     0.000000                       0
rcbw_BTCUSDT_004443 BTCUSDT  2020            6                -0.000789              -0.026575                    -0.030928                      -1
rcbw_BTCUSDT_004540 BTCUSDT  2020            7                -0.001326              -0.045554                    -0.020619                      -1
rcbw_BTCUSDT_004637 BTCUSDT  2020            7                -0.000968              -0.015127                    -0.010309                      -1

## Hazards (head)
 age  at_risk  hits   hazard cohort
   1      667    13 0.019490 events
   2      654    14 0.021407 events
   3      640    10 0.015625 events
   4      630    18 0.028571 events
   5      612    12 0.019608 events
   6      600     8 0.013333 events
   7      592     7 0.011824 events
   8      585     9 0.015385 events
   9      576    10 0.017361 events
  10      566     7 0.012367 events
  11      559    11 0.019678 events
  12      548    11 0.020073 events
  13      537     9 0.016760 events
  14      528     5 0.009470 events
  15      523     9 0.017208 events
  16      514    10 0.019455 events
  17      504     6 0.011905 events
  18      498     7 0.014056 events
  19      491     9 0.018330 events
  20      482     5 0.010373 events
  21      477     3 0.006289 events
  22      474     4 0.008439 events
  23      470     5 0.010638 events
  24      465     2 0.004301 events