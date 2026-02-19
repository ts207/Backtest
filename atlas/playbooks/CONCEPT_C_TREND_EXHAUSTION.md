# Concept Playbook: C_TREND_EXHAUSTION

## Definition
The statistical identification of "mature" trends that are likely to reverse or consolidate. This concept rejects a binary trend/no-trend view in favor of measuring trend age, slope decay, and deleveraging exhaustion (OI drop during price moves).

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `perp_ohlcv_15m` | `close`, `volume` | 15m | 120 days |
| `um_open_interest_hist` | `sum_open_interest` | 5m | 60 days |

## Metrics
*   **`trend_age_bars`**: The number of consecutive bars where the price remains on one side of a 20-period EMA.
*   **`directional_streak`**: Number of consecutive bars with the same return sign.
*   **`oi_standardized_change`**: Z-score of Open Interest change over a rolling window.
*   **`slope_decay_ratio`**: Ratio of the current 10-bar regression slope to the 50-bar slope.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_TREND_01` | Streak Reversal | `directional_streak` >= 5 must show a return to mean (reversal) probability > 0.6 in compression regimes. |
| `T_TREND_02` | Deleveraging Signal | OI drops of > 1.5 sigma during price moves must precede a reduction in trend slope. |
| `T_TREND_03` | Age Sensitivity | Continuation probability must be a monotonically decreasing function of `trend_age_bars` for ages > 20. |

## Artifacts
*   **`spec/features/trend_exhaustion.yaml`**: Features for trend age, streaks, and OI deltas.
*   **`project/features/funding_persistence.py`**: Logic for identifying trend persistence vs exhaustion.
*   **`data/lake/reports/trend_maturity_stats.csv`**: Correlation between trend age and future returns.
