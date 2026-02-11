# Phase-1 Interpretation Lock: liquidity_absence_window

## Gate Question
> Is there any window where absence shows consistent suppression vs matched baseline (and placebo, if used), with no year/session sign flips and non-increasing early hazard?

## Evaluation (baseline_2021_2022, window [0,96])
- Matched-baseline suppression is **not consistent** across anchor sessions (hour buckets):
  - anchor 0: `delta_tail_move_probability = +0.012230` (worse vs controls)
  - anchor 8: `delta_tail_move_probability = -0.021364` (better vs controls)
  - anchor 16: `delta_tail_move_probability = +0.007947` (worse vs controls)
- Year/session sign stability fails:
  - `sign_vol` flips between 2021 and 2022 for anchor 0 and anchor 16.
- Early hazard shape fails monotone-non-increasing condition:
  - event hazards rise early (e.g., age 1→3 and 8→9 and 11→12).
  - matched-control hazards also show early jumps.
- Placebo was not used in this first liquidity-absence Phase-1 run, so the lock decision is based on matched baseline + stability + hazard shape criteria.

## Binary Outcome Decision
**NO** — freeze `liquidity_absence_window` permanently at Phase-1 interpretation lock (current specification), and do not promote to Phase-2 hypothesis design from this run.
