# Concept Playbook: C_CAPACITY_CONSTRAINTS

## Definition
The physical limits on how much capital can be deployed into an edge before transaction costs (impact) erase the expectancy. Capacity is treated as an inherent property of the edge, driven by the frequency of events, the available liquidity at those times, and the participation rate limits.

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `perp_ohlcv_5m` | `volume`, `quote_volume` | 5m | 90 days |
| `tob_5m_agg` | `bid_depth_usd_mean`, `ask_depth_usd_mean` | 5m | 30 days |

## Metrics
*   **`max_notional_per_trade`**: Maximum trade size allowed given a 5% participation rate cap of recent ADV.
*   **`expectancy_decay_curve`**: Function mapping deployed capital to remaining after-cost return.
*   **`capacity_bps_limit`**: The point at which slippage + impact costs exceed the gross event study return.
*   **`turnover_capacity`**: Limit on total daily notional based on portfolio-level liquidity gates.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_CAP_01` | Participation Cap | No simulated trade may exceed 10% of the median volume in the 1-minute bar of entry. |
| `T_CAP_02` | Expectancy Erosion | Edge must retain > 50% of its expectancy when scaled to the minimum target fund size. |
| `T_CAP_03` | Depth Constraint | Total position size must be < 5x the average top-of-book depth USD. |

## Artifacts
*   **`project/eval/capacity.py`**: Sensitivity analysis for capital scaling vs returns.
*   **`data/reports/capacity/scaling_limits.json`**: Maximum recommended capital per edge family.
