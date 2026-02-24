# Concept Playbook: C_INVARIANTS

## Definition
Structural constraints and relationships that should approximately hold in a functioning market, used as sanity checks and mispricing anchors. Edges are often discovered when these invariants are violated beyond the bounds of transaction and financing frictions.

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `perp_ohlcv_5m` | `close` | 5m | 30 days |
| `spot_ohlcv_5m` | `close` | 5m | 30 days |
| `um_funding_rates`| `funding_rate` | 8h | 30 days |

## Metrics
*   **`basis_parity_deviation`**: Absolute difference between perp price and the theoretical no-arbitrage price (spot + carry).
*   **`triangular_arbitrage_index`**: Product of cross-rates (e.g., BTC/USDT, ETH/BTC, ETH/USDT) relative to 1.0.
*   **`funding_anchor_spread`**: Standardized distance between perp mark and spot index normalized by funding rate.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_INV_01` | Parity Bounds | `basis_bps` must stay within +/- 50bps of the carry anchor during 90%+ of non-stress periods. |
| `T_INV_02` | Friction Verification | Invariant violations must only be tradable if the deviation > estimated round-trip transaction costs. |
| `T_INV_03` | Cross-Venue Drift | Price differences for the same symbol across primary venues must mean-revert within 10 minutes. |

## Artifacts
*   **`project/engine/execution_model.py`**: Implementation of arbitrage and parity bounds.
*   **`data/reports/invariants/parity_violations.parquet`**: Timeseries of identified invariant breaks.
