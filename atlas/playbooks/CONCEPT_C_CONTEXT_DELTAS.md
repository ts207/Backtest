# Concept Playbook: C_CONTEXT_DELTAS

## Definition
The measurement of state changes (deltas) in the market environment leading up to an event. Instead of a static "snapshot," context deltas capture the trajectory of volatility, liquidity, and leverage, providing a vector of the market's recent evolution.

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `perp_ohlcv_1m` | All core | 1m | 30 days |
| `um_funding_rates`| `funding_rate` | 8h | 30 days |

## Metrics
*   **`basis_delta_1h`**: Change in the perp-spot basis over the last hour.
*   **`vol_acceleration_ratio`**: Ratio of 1-hour realized volatility to 24-hour realized volatility.
*   **`oi_velocity_zscore`**: Standardized rate of change in Open Interest over the last 4 hours.
*   **`spread_widening_velocity`**: Time-derivative of the bid-ask spread.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_DELTA_01` | Feature Causality | All deltas must be computed using windows ending at or before t0. |
| `T_DELTA_02` | Informational Value | `vol_acceleration_ratio` must show a Spearman correlation > 0.2 with 1-hour forward range. |
| `T_DELTA_03` | Drift Detection | Standardized basis deltas > 2.0 must precede price moves in the direction of parity in 65%+ of cases. |

## Artifacts
*   **`spec/features/context_deltas.yaml`**: Multi-window delta feature specifications.
*   **`project/features/context.py`**: Implementation of trajectory and velocity metrics.
*   **`data/lake/features/deltas/`**: Output tables for all standardized market trajectory vectors.
