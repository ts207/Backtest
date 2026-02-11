# Range Compression Breakout Window (Phase 1)

Run ID: `20260211_031950`
Window: [0, 96]
Compression quantile: 0.2

- Events: 1451
- Controls: 1451
- Actions generated: 0 (Phase 1 structure only)

## Matched deltas (head)
           event_id  symbol  year  anchor_hour  delta_realized_vol_mean  delta_range_expansion  delta_tail_move_probability  delta_tail_move_within
rcbw_BTCUSDT_000574 BTCUSDT  2020           23                -0.000187               0.019627                     0.030928                       1
rcbw_BTCUSDT_000750 BTCUSDT  2020           19                 0.001072               0.024516                     0.020619                       1
rcbw_BTCUSDT_000865 BTCUSDT  2020            0                -0.001362              -0.016091                    -0.020619                       0
rcbw_BTCUSDT_001209 BTCUSDT  2020           14                -0.000382              -0.024258                     0.010309                       1
rcbw_BTCUSDT_001306 BTCUSDT  2020           14                 0.000661               0.043605                     0.051546                       0
rcbw_BTCUSDT_001588 BTCUSDT  2020           13                -0.000322               0.004102                     0.000000                       0
rcbw_BTCUSDT_001713 BTCUSDT  2020           20                 0.000621               0.001744                     0.000000                       0
rcbw_BTCUSDT_001861 BTCUSDT  2020            9                 0.000492               0.023121                     0.000000                       0
rcbw_BTCUSDT_001995 BTCUSDT  2020           18                -0.000784              -0.018647                    -0.020619                      -1
rcbw_BTCUSDT_002196 BTCUSDT  2020           21                 0.000530               0.036329                     0.020619                       1
rcbw_BTCUSDT_002420 BTCUSDT  2020            5                 0.000175               0.000583                     0.020619                       1
rcbw_BTCUSDT_002517 BTCUSDT  2020            5                -0.000540              -0.020830                    -0.030928                       0

## Hazards (head)
 age  at_risk  hits   hazard cohort
   1     1410    35 0.024823 events
   2     1375    37 0.026909 events
   3     1338    31 0.023169 events
   4     1307    35 0.026779 events
   5     1272    33 0.025943 events
   6     1239    25 0.020178 events
   7     1214    21 0.017298 events
   8     1193    17 0.014250 events
   9     1176    27 0.022959 events
  10     1149    21 0.018277 events
  11     1128    22 0.019504 events
  12     1106    12 0.010850 events
  13     1094    21 0.019196 events
  14     1073    15 0.013979 events
  15     1058    17 0.016068 events
  16     1041    19 0.018252 events
  17     1022    16 0.015656 events
  18     1006    17 0.016899 events
  19      989     7 0.007078 events
  20      982    16 0.016293 events
  21      966     7 0.007246 events
  22      959    11 0.011470 events
  23      948    12 0.012658 events
  24      936     6 0.006410 events