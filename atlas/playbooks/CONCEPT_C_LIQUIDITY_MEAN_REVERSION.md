# Concept Playbook: C_LIQUIDITY_MEAN_REVERSION

## Definition
The tendency of market prices to revert to a mean or "fair value" anchor after a temporary dislocation caused by liquidity shocks, sweeps, or order flow imbalances. This concept leverages the transient nature of liquidity-driven price moves (as opposed to information-driven ones).

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `perp_ohlcv_1m` | `close`, `volume` | 1m | 14 days |
| `tob_1s` | `bid_price`, `ask_price` | 1s | 7 days |
| `basis_1m` | `basis_bps` | 1m | 7 days |

## Metrics
*   **`basis_zscore`**: The current basis (perp-spot) deviation from its 24-hour rolling mean, normalized by standard deviation.
*   **`vwap_deviation_bps`**: Distance between current price and the 1-hour Volume Weighted Average Price (VWAP).
*   **`spread_mean_reversion_half_life`**: Estimated time for an expanded spread to return to its median level.
*   **`depth_imbalance`**: Ratio of bid depth to total depth at the top of the book.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_REVERT_01` | Basis Reversion | 70%+ of basis spikes (> 3 sigma) must revert to < 1 sigma within 30 minutes. |
| `T_REVERT_02` | Fade Expectancy | Cumulative return of fading 2-sigma price deviations from VWAP must be positive over 100+ samples. |
| `T_REVERT_03` | Spread Normalization | Median time to spread normalization after a 2x expansion must be < 5 minutes. |

## Artifacts
*   **`spec/features/liquidity_reversion.yaml`**: Feature specs for basis Z-score and VWAP deviation.
*   **`project/features/liquidity_vacuum.py`**: Detection logic for liquidity-driven dislocations.
*   **`data/lake/events/reversion_triggers.parquet`**: Detected mean-reversion opportunity events.
